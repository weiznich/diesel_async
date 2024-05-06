# Change Log

All user visible changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/), as described
for Rust libraries in [RFC #1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md)

## [Unreleased]

* Added type `diesel_async::pooled_connection::mobc::PooledConnection`
* MySQL/MariaDB now use `CLIENT_FOUND_ROWS` capability to allow consistent behavior with PostgreSQL regarding return value of UPDATe commands.
* The minimal supported rust version is now 1.78.0

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
[Unreleased]: https://github.com/weiznich/diesel_async/compare/v0.4.1...main
