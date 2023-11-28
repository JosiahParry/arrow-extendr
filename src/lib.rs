//! Pass arrow-rs objects to and from an R session
//!
//! ## Motivating Example
//!
//! Say we have the following `DBI` connection which we will send requests to using arrow.
//! The result of `dbGetQueryArrow()` is a `nanoarrow_array_stream`. We want to
//! count the number of rows in each batch of the steam using Rust.
//!
//! ```r
//! # adapted from https://github.com/r-dbi/DBI/blob/main/vignettes/DBI-arrow.Rmd
//!
//! library(DBI)
//! con <- dbConnect(RSQLite::SQLite())
//! data <- data.frame(
//!   a = runif(10000, 0, 10),
//!   b = rnorm(10000, 4.5),
//!   c = sample(letters, 10000, TRUE)
//! )
//!
//! dbWriteTable(con, "tbl", data)
//! ```
//!
//! We can write an extendr function which creates an `ArrowArrayStreamReader`
//! from an `&Robj`. In the function we instantiate a counter to keep track
//! of the number of rows per chunk. For each chunk we print the number of rows.
//!
//! ```ignore
//! #[extendr]
//! /// @export
//! fn process_stream(stream: Robj) -> i32 {
//!     let rb = ArrowArrayStreamReader::from_arrow_robj(&stream)
//!         .unwrap();
//!
//!     let mut n = 0;
//!
//!     rprintln!("Processing `ArrowArrayStreamReader`...");
//!     for chunk in rb {
//!         let chunk_rows = chunk.unwrap().num_rows();
//!         rprintln!("Found {chunk_rows} rows");
//!         n += chunk_rows as i32;
//!     }
//!
//!     n
//! }
//! ```
//!
//! With this function we can use it on the output of `dbGetQueryArrow()` or other Arrow
//! related DBI functions.
//!
//! ```r
//! query <- dbGetQueryArrow(con, "SELECT * FROM tbl WHERE a < 3")
//! process_stream(query)
//! #> Processing `ArrowArrayStreamReader`...
//! #> Found 256 rows
//! #> Found 256 rows
//! #> Found 256 rows
//! #> ... truncated ...
//! #> Found 256 rows
//! #> Found 256 rows
//! #> Found 143 rows
//! #> [1] 2959
//! ```
pub mod from;
pub mod to;
