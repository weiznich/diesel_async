use diesel::ConnectionError;

pub(super) struct ErrorHelper(pub(super) tokio_postgres::Error);

impl From<ErrorHelper> for ConnectionError {
    fn from(postgres_error: ErrorHelper) -> Self {
        ConnectionError::CouldntSetupConfiguration(postgres_error.into())
    }
}

impl From<ErrorHelper> for diesel::result::Error {
    fn from(ErrorHelper(postgres_error): ErrorHelper) -> Self {
        use diesel::result::DatabaseErrorKind::*;
        use tokio_postgres::error::SqlState;

        match postgres_error.code() {
            Some(code) => {
                let kind = match *code {
                    SqlState::UNIQUE_VIOLATION => UniqueViolation,
                    SqlState::FOREIGN_KEY_VIOLATION => ForeignKeyViolation,
                    SqlState::T_R_SERIALIZATION_FAILURE => SerializationFailure,
                    SqlState::READ_ONLY_SQL_TRANSACTION => ReadOnlyTransaction,
                    SqlState::NOT_NULL_VIOLATION => NotNullViolation,
                    SqlState::CHECK_VIOLATION => CheckViolation,
                    _ => Unknown,
                };

                diesel::result::Error::DatabaseError(
                    kind,
                    Box::new(PostgresDbErrorWrapper(
                        postgres_error
                            .into_source()
                            .and_then(|e| e.downcast::<tokio_postgres::error::DbError>().ok())
                            .expect("It's a db error, because we've got a SQLState code above"),
                    )) as _,
                )
            }
            None => diesel::result::Error::DatabaseError(
                UnableToSendCommand,
                Box::new(postgres_error.to_string()),
            ),
        }
    }
}

struct PostgresDbErrorWrapper(Box<tokio_postgres::error::DbError>);

impl diesel::result::DatabaseErrorInformation for PostgresDbErrorWrapper {
    fn message(&self) -> &str {
        self.0.message()
    }

    fn details(&self) -> Option<&str> {
        self.0.detail()
    }

    fn hint(&self) -> Option<&str> {
        self.0.hint()
    }

    fn table_name(&self) -> Option<&str> {
        self.0.table()
    }

    fn column_name(&self) -> Option<&str> {
        self.0.column()
    }

    fn constraint_name(&self) -> Option<&str> {
        self.0.constraint()
    }

    fn statement_position(&self) -> Option<i32> {
        use tokio_postgres::error::ErrorPosition;
        self.0.position().map(|e| match e {
            ErrorPosition::Original(position) | ErrorPosition::Internal { position, .. } => {
                *position as i32
            }
        })
    }
}
