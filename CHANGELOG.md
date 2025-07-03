# Change Log

All user visible changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/), as described
for Rust libraries in [RFC #1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md)

## [Unreleased]

## [0.6.1] - 2025-07-03

* Fix features for some dependencies

## [0.6.0] - 2025-07-02

* Allow to control the statement cache size
* Minimize dependencies features 
* Bump minimal supported mysql_async version to 0.36.0
* Fixing a bug in how we tracked open transaction that could lead to dangling transactions is specific cases

## [0.5.2] - 2024-11-26

* Fixed an issue around transaction cancellation that could lead to connection pools containing connections with dangling transactions

## [0.5.1] - 2024-11-01

* Add crate feature `pool` for extending connection pool implements through external crate
* Implement `Deref` and `DerefMut` for `AsyncConnectionWrapper` to allow using it in an async context as well

## [0.5.0] - 2024-07-19

* Added type `diesel_async::pooled_connection::mobc::PooledConnection`
* MySQL/MariaDB now use `CLIENT_FOUND_ROWS` capability to allow consistent behaviour with PostgreSQL regarding return value of UPDATe commands.
* The minimal supported rust version is now 1.78.0
* Add a `SyncConnectionWrapper` type that turns a sync connection into an async one. This enables SQLite support for diesel-async
* Add support for `diesel::connection::Instrumentation` to support logging and other instrumentation for any of the provided connection impls.
* Bump minimal supported mysql_async version to 0.34

## [0.4.1] - 2023-09-01

* Fixed feature flags for docs.rs

## [0.4.0] - 2023-09-01

* Add a `AsyncConnectionWrapper` type to turn a `diesel_async::AsyncConnection` into a `diesel::Connection`. This might be used to execute migrations via `diesel_migrations`. 
* Add some connection pool configurations to specify how connections
in the pool should be checked if they are still valid

## [0.3.2] - 2023-07-24

* Fix `TinyInt` serialization
* Check for open transactions before returning the connection to the pool

## [0.3.1] - 2023-06-07

* Minor readme fixes
* Add a missing `UpdateAndFetchResults` impl

## [0.3.0] - 2023-05-26

* Compatibility with diesel 2.1

## [0.2.2] - 2023-04-14

* Dependency updates for `mysql-async` to allow newer versions

## [0.2.1] - 2023-03-08

* Dependency updates for `mobc` and `mysql-async` to allow newer versions as well 
* Extend the README
* Improve the version constraint for diesel so that we do not end up using a newer
 diesel version that's incompatible

## [0.2.0] - 2022-12-16

* [#38](https://github.com/weiznich/diesel_async/pull/38) Relax the requirements for borrowed captures in the transaction closure
* [#41](https://github.com/weiznich/diesel_async/pull/41) Remove GAT workarounds from various traits (Raises the MSRV to 1.65)
* [#42](https://github.com/weiznich/diesel_async/pull/42) Add an additional `AsyncDieselConnectionManager` constructor that allows to specify a custom connection setup method to allow setting up postgres TLS connections
* Relicense the crate under the MIT or Apache 2.0 License

## [0.1.1] - 2022-10-19

### Fixes

* Fix prepared statement leak for the mysql backend implementation

## 0.1.0 - 2022-09-27

* Initial release

[0.1.1]: https://github.com/weiznich/diesel_async/compare/v0.1.0...v0.1.1
[0.2.0]: https://github.com/weiznich/diesel_async/compare/v0.1.1...v0.2.0
[0.2.1]: https://github.com/weiznich/diesel_async/compare/v0.2.0...v0.2.1
[0.2.2]: https://github.com/weiznich/diesel_async/compare/v0.2.1...v0.2.2
[0.3.0]: https://github.com/weiznich/diesel_async/compare/v0.2.0...v0.3.0
[0.3.1]: https://github.com/weiznich/diesel_async/compare/v0.3.0...v0.3.1
[0.3.2]: https://github.com/weiznich/diesel_async/compare/v0.3.1...v0.3.2
[0.4.0]: https://github.com/weiznich/diesel_async/compare/v0.3.2...v0.4.0
[0.4.1]: https://github.com/weiznich/diesel_async/compare/v0.4.0...v0.4.1
[0.5.0]: https://github.com/weiznich/diesel_async/compare/v0.4.0...v0.5.0
[0.5.1]: https://github.com/weiznich/diesel_async/compare/v0.5.0...v0.5.1
[0.5.2]: https://github.com/weiznich/diesel_async/compare/v0.5.1...v0.5.2
[0.6.0]: https://github.com/weiznich/diesel_async/compare/v0.5.2...v0.6.0
[0.6.1]: https://github.com/weiznich/diesel_async/compare/v0.6.0...v0.6.1
[Unreleased]: https://github.com/weiznich/diesel_async/compare/v0.6.1...main
