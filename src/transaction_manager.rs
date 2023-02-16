use diesel::result::Error;
use diesel::QueryResult;
use scoped_futures::ScopedBoxFuture;
use std::borrow::Cow;
use std::num::NonZeroU32;

use crate::AsyncConnection;
// TODO: refactor this to share more code with diesel

/// Manages the internal transaction state for a connection.
///
/// You will not need to interact with this trait, unless you are writing an
/// implementation of [`AsyncConnection`].
#[async_trait::async_trait]
pub trait TransactionManager<Conn: AsyncConnection>: Send {
    /// Data stored as part of the connection implementation
    /// to track the current transaction state of a connection
    type TransactionStateData;

    /// Begin a new transaction or savepoint
    ///
    /// If the transaction depth is greater than 0,
    /// this should create a savepoint instead.
    /// This function is expected to increment the transaction depth by 1.
    async fn begin_transaction(conn: &mut Conn) -> QueryResult<()>;

    /// Rollback the inner-most transaction or savepoint
    ///
    /// If the transaction depth is greater than 1,
    /// this should rollback to the most recent savepoint.
    /// This function is expected to decrement the transaction depth by 1.
    async fn rollback_transaction(conn: &mut Conn) -> QueryResult<()>;

    /// Commit the inner-most transaction or savepoint
    ///
    /// If the transaction depth is greater than 1,
    /// this should release the most recent savepoint.
    /// This function is expected to decrement the transaction depth by 1.
    async fn commit_transaction(conn: &mut Conn) -> QueryResult<()>;

    /// Fetch the current transaction status as mutable
    ///
    /// Used to ensure that `begin_test_transaction` is not called when already
    /// inside of a transaction, and that operations are not run in a `InError`
    /// transaction manager.
    #[doc(hidden)]
    fn transaction_manager_status_mut(conn: &mut Conn) -> &mut TransactionManagerStatus;

    /// Executes the given function inside of a database transaction
    ///
    /// Each implementation of this function needs to fulfill the documented
    /// behaviour of [`AsyncConnection::transaction`]
    async fn transaction<'a, F, R, E>(conn: &mut Conn, callback: F) -> Result<R, E>
    where
        F: for<'r> FnOnce(&'r mut Conn) -> ScopedBoxFuture<'a, 'r, Result<R, E>> + Send + 'a,
        E: From<Error> + Send,
        R: Send,
    {
        Self::begin_transaction(conn).await?;
        match callback(&mut *conn).await {
            Ok(value) => {
                Self::commit_transaction(conn).await?;
                Ok(value)
            }
            Err(user_error) => match Self::rollback_transaction(conn).await {
                Ok(()) => Err(user_error),
                Err(Error::BrokenTransactionManager) => {
                    // In this case we are probably more interested by the
                    // original error, which likely caused this
                    Err(user_error)
                }
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    /// This methods checks if the connection manager is considered to be broken
    /// by connection pool implementations
    ///
    /// A connection manager is considered to be broken by default if it either
    /// contains an open transaction (because you don't want to have connections
    /// with open transactions in your pool) or when the transaction manager is
    /// in an error state.
    #[doc(hidden)]
    fn is_broken_transaction_manager(conn: &mut Conn) -> bool {
        match Self::transaction_manager_status_mut(conn).transaction_state() {
            // all transactions are closed
            // so we don't consider this connection broken
            Ok(ValidTransactionManagerStatus {
                in_transaction: None,
            }) => false,
            // The transaction manager is in an error state
            // Therefore we consider this connection broken
            Err(_) => true,
            // The transaction manager contains a open transaction
            // we do consider this connection broken
            // if that transaction was not opened by `begin_test_transaction`
            Ok(ValidTransactionManagerStatus {
                in_transaction: Some(s),
            }) => !s.test_transaction,
        }
    }
}

/// An implementation of `TransactionManager` which can be used for backends
/// which use ANSI standard syntax for savepoints such as SQLite and PostgreSQL.
#[derive(Default, Debug)]
pub struct AnsiTransactionManager {
    pub(crate) status: TransactionManagerStatus,
}

/// Status of the transaction manager
#[derive(Debug)]
pub enum TransactionManagerStatus {
    /// Valid status, the manager can run operations
    Valid(ValidTransactionManagerStatus),
    /// Error status, probably following a broken connection. The manager will no longer run operations
    InError,
}

impl Default for TransactionManagerStatus {
    fn default() -> Self {
        TransactionManagerStatus::Valid(ValidTransactionManagerStatus::default())
    }
}

impl TransactionManagerStatus {
    /// Returns the transaction depth if the transaction manager's status is valid, or returns
    /// [`Error::BrokenTransactionManager`] if the transaction manager is in error.
    pub fn transaction_depth(&self) -> QueryResult<Option<NonZeroU32>> {
        match self {
            TransactionManagerStatus::Valid(valid_status) => Ok(valid_status.transaction_depth()),
            TransactionManagerStatus::InError => Err(Error::BrokenTransactionManager),
        }
    }

    /// If in transaction and transaction manager is not broken, registers that the
    /// connection can not be used anymore until top-level transaction is rolled back
    pub(crate) fn set_top_level_transaction_requires_rollback(&mut self) {
        if let TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
            in_transaction:
                Some(InTransactionStatus {
                    top_level_transaction_requires_rollback,
                    ..
                }),
        }) = self
        {
            *top_level_transaction_requires_rollback = true;
        }
    }

    /// Sets the transaction manager status to InError
    ///
    /// Subsequent attempts to use transaction-related features will result in a
    /// [`Error::BrokenTransactionManager`] error
    pub fn set_in_error(&mut self) {
        *self = TransactionManagerStatus::InError
    }

    fn transaction_state(&mut self) -> QueryResult<&mut ValidTransactionManagerStatus> {
        match self {
            TransactionManagerStatus::Valid(valid_status) => Ok(valid_status),
            TransactionManagerStatus::InError => Err(Error::BrokenTransactionManager),
        }
    }

    pub(crate) fn set_test_transaction_flag(&mut self) {
        if let TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
            in_transaction: Some(s),
        }) = self
        {
            s.test_transaction = true;
        }
    }
}

/// Valid transaction status for the manager. Can return the current transaction depth
#[allow(missing_copy_implementations)]
#[derive(Debug, Default)]
pub struct ValidTransactionManagerStatus {
    in_transaction: Option<InTransactionStatus>,
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
struct InTransactionStatus {
    transaction_depth: NonZeroU32,
    top_level_transaction_requires_rollback: bool,
    test_transaction: bool,
}

impl ValidTransactionManagerStatus {
    /// Return the current transaction depth
    ///
    /// This value is `None` if no current transaction is running
    /// otherwise the number of nested transactions is returned.
    pub fn transaction_depth(&self) -> Option<NonZeroU32> {
        self.in_transaction.as_ref().map(|it| it.transaction_depth)
    }

    /// Update the transaction depth by adding the value of the `transaction_depth_change` parameter if the `query` is
    /// `Ok(())`
    pub fn change_transaction_depth(
        &mut self,
        transaction_depth_change: TransactionDepthChange,
    ) -> QueryResult<()> {
        match (&mut self.in_transaction, transaction_depth_change) {
            (Some(in_transaction), TransactionDepthChange::IncreaseDepth) => {
                // Can be replaced with saturating_add directly on NonZeroU32 once
                // <https://github.com/rust-lang/rust/issues/84186> is stable
                in_transaction.transaction_depth =
                    NonZeroU32::new(in_transaction.transaction_depth.get().saturating_add(1))
                        .expect("nz + nz is always non-zero");
                Ok(())
            }
            (Some(in_transaction), TransactionDepthChange::DecreaseDepth) => {
                // This sets `transaction_depth` to `None` as soon as we reach zero
                match NonZeroU32::new(in_transaction.transaction_depth.get() - 1) {
                    Some(depth) => in_transaction.transaction_depth = depth,
                    None => self.in_transaction = None,
                }
                Ok(())
            }
            (None, TransactionDepthChange::IncreaseDepth) => {
                self.in_transaction = Some(InTransactionStatus {
                    transaction_depth: NonZeroU32::new(1).expect("1 is non-zero"),
                    top_level_transaction_requires_rollback: false,
                    test_transaction: false,
                });
                Ok(())
            }
            (None, TransactionDepthChange::DecreaseDepth) => {
                // We screwed up something somewhere
                // we cannot decrease the transaction count if
                // we are not inside a transaction
                Err(Error::NotInTransaction)
            }
        }
    }
}

/// Represents a change to apply to the depth of a transaction
#[derive(Debug, Clone, Copy)]
pub enum TransactionDepthChange {
    /// Increase the depth of the transaction (corresponds to `BEGIN` or `SAVEPOINT`)
    IncreaseDepth,
    /// Decreases the depth of the transaction (corresponds to `COMMIT`/`RELEASE SAVEPOINT` or `ROLLBACK`)
    DecreaseDepth,
}

impl AnsiTransactionManager {
    fn get_transaction_state<Conn>(
        conn: &mut Conn,
    ) -> QueryResult<&mut ValidTransactionManagerStatus>
    where
        Conn: AsyncConnection<TransactionManager = Self>,
    {
        conn.transaction_state().status.transaction_state()
    }

    /// Begin a transaction with custom SQL
    ///
    /// This is used by connections to implement more complex transaction APIs
    /// to set things such as isolation levels.
    /// Returns an error if already inside of a transaction.
    pub async fn begin_transaction_sql<Conn>(conn: &mut Conn, sql: &str) -> QueryResult<()>
    where
        Conn: AsyncConnection<TransactionManager = Self>,
    {
        let state = Self::get_transaction_state(conn)?;
        match state.transaction_depth() {
            None => {
                conn.batch_execute(sql).await?;
                Self::get_transaction_state(conn)?
                    .change_transaction_depth(TransactionDepthChange::IncreaseDepth)?;
                Ok(())
            }
            Some(_depth) => Err(Error::AlreadyInTransaction),
        }
    }
}

#[async_trait::async_trait]
impl<Conn> TransactionManager<Conn> for AnsiTransactionManager
where
    Conn: AsyncConnection<TransactionManager = Self>,
{
    type TransactionStateData = Self;

    async fn begin_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;
        let start_transaction_sql = match transaction_state.transaction_depth() {
            None => Cow::from("BEGIN"),
            Some(transaction_depth) => {
                Cow::from(format!("SAVEPOINT diesel_savepoint_{transaction_depth}"))
            }
        };
        conn.batch_execute(&start_transaction_sql).await?;
        Self::get_transaction_state(conn)?
            .change_transaction_depth(TransactionDepthChange::IncreaseDepth)?;

        Ok(())
    }

    async fn rollback_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;

        let rollback_sql = match transaction_state.in_transaction {
            Some(ref mut in_transaction) => {
                match in_transaction.transaction_depth.get() {
                    1 => Cow::Borrowed("ROLLBACK"),
                    depth_gt1 => {
                        if in_transaction.top_level_transaction_requires_rollback {
                            // There's no point in *actually* rolling back this one
                            // because we won't be able to do anything until top-level
                            // is rolled back.

                            // To make it easier on the user (that they don't have to really look
                            // at actual transaction depth and can just rely on the number of
                            // times they have called begin/commit/rollback) we don't mark the
                            // transaction manager as out of the savepoints as soon as we
                            // realize there is that issue, but instead we still decrement here:
                            in_transaction.transaction_depth = NonZeroU32::new(depth_gt1 - 1)
                                .expect("Depth was checked to be > 1");
                            return Ok(());
                        } else {
                            Cow::Owned(format!(
                                "ROLLBACK TO SAVEPOINT diesel_savepoint_{}",
                                depth_gt1 - 1
                            ))
                        }
                    }
                }
            }
            None => return Err(Error::NotInTransaction),
        };

        match conn.batch_execute(&rollback_sql).await {
            Ok(()) => {
                Self::get_transaction_state(conn)?
                    .change_transaction_depth(TransactionDepthChange::DecreaseDepth)?;
                Ok(())
            }
            Err(rollback_error) => {
                let tm_status = Self::transaction_manager_status_mut(conn);
                match tm_status {
                    TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
                        in_transaction:
                            Some(InTransactionStatus {
                                transaction_depth,
                                top_level_transaction_requires_rollback,
                                ..
                            }),
                    }) if transaction_depth.get() > 1
                        && !*top_level_transaction_requires_rollback =>
                    {
                        // A savepoint failed to rollback - we may still attempt to repair
                        // the connection by rolling back top-level transaction.
                        *transaction_depth = NonZeroU32::new(transaction_depth.get() - 1)
                            .expect("Depth was checked to be > 1");
                        *top_level_transaction_requires_rollback = true;
                    }
                    _ => tm_status.set_in_error(),
                }
                Err(rollback_error)
            }
        }
    }

    /// If the transaction fails to commit due to a `SerializationFailure` or a
    /// `ReadOnlyTransaction` a rollback will be attempted. If the rollback succeeds,
    /// the original error will be returned, otherwise the error generated by the rollback
    /// will be returned. In the second case the connection will be considered broken
    /// as it contains a uncommitted unabortable open transaction.
    async fn commit_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;
        let transaction_depth = transaction_state.transaction_depth();
        let commit_sql = match transaction_depth {
            None => return Err(Error::NotInTransaction),
            Some(transaction_depth) if transaction_depth.get() == 1 => Cow::Borrowed("COMMIT"),
            Some(transaction_depth) => Cow::Owned(format!(
                "RELEASE SAVEPOINT diesel_savepoint_{}",
                transaction_depth.get() - 1
            )),
        };
        match conn.batch_execute(&commit_sql).await {
            Ok(()) => {
                Self::get_transaction_state(conn)?
                    .change_transaction_depth(TransactionDepthChange::DecreaseDepth)?;
                Ok(())
            }
            Err(commit_error) => {
                if let TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
                    in_transaction:
                        Some(InTransactionStatus {
                            ref mut transaction_depth,
                            top_level_transaction_requires_rollback: true,
                            ..
                        }),
                }) = conn.transaction_state().status
                {
                    match transaction_depth.get() {
                        1 => match Self::rollback_transaction(conn).await {
                            Ok(()) => {}
                            Err(rollback_error) => {
                                conn.transaction_state().status.set_in_error();
                                return Err(Error::RollbackErrorOnCommit {
                                    rollback_error: Box::new(rollback_error),
                                    commit_error: Box::new(commit_error),
                                });
                            }
                        },
                        depth_gt1 => {
                            // There's no point in *actually* rolling back this one
                            // because we won't be able to do anything until top-level
                            // is rolled back.

                            // To make it easier on the user (that they don't have to really look
                            // at actual transaction depth and can just rely on the number of
                            // times they have called begin/commit/rollback) we don't mark the
                            // transaction manager as out of the savepoints as soon as we
                            // realize there is that issue, but instead we still decrement here:
                            *transaction_depth = NonZeroU32::new(depth_gt1 - 1)
                                .expect("Depth was checked to be > 1");
                        }
                    }
                }
                Err(commit_error)
            }
        }
    }

    fn transaction_manager_status_mut(conn: &mut Conn) -> &mut TransactionManagerStatus {
        &mut conn.transaction_state().status
    }
}
