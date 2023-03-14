# Change Log

All user visible changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/), as described
for Rust libraries in [RFC #1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md)

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
