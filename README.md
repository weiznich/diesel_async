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

## License

Licensed under [AGPL v3 or later](https://www.gnu.org/licenses/agpl-3.0.html) 
Feel free to reach out if you require a more permissive licence.



