# A async interface for diesel

Diesel gets rid of the boilerplate for database interaction and eliminates
runtime errors without sacrificing performance. It takes full advantage of
Rust's type system to create a low overhead query builder that "feels like
Rust."

Diesel-async provides an async implementation of diesels connection implementation
and any method that may issue an query. It is designed as pure async drop in replacement
for the corresponding diesel methods.

Supported databases:
1. PostgreSQL
2. MySQL

## Code of conduct

Anyone who interacts with Diesel in any space, including but not limited to
this GitHub repository, must follow our [code of conduct](https://github.com/diesel-rs/diesel/blob/master/code_of_conduct.md).

## License

Licensed under either of these:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

### Contributing
Unless you explicitly state otherwise, any contribution you intentionally submit
for inclusion in the work, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.

