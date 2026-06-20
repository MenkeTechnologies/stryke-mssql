//! Integration-test placeholder.
//!
//! `stryke-mssql` is a `cdylib`-only crate (no `rlib`), so an integration test
//! cannot link its `extern "C"` exports. The real coverage is:
//!
//!   * `src/lib.rs` `#[cfg(test)] mod tests` — unit tests for the pure logic
//!     (ADO connection-string parse/redact, default ports, param extraction),
//!     which run on `cargo test`.
//!   * `t/test_stryke_mssql_surface.stk` — pins every `Mssql::*` wrapper and the
//!     URL helpers, with no database.
//!   * `t/test_mssql.stk` — query/execute/batch against a live SQL Server
//!     (`$MSSQL_HOST`), short-circuited when none is set.

#[test]
fn cdylib_crate_compiles() {
    // Reaching here means every `extern "C"` `mssql__*` export type-checked and
    // linked into the test harness — the minimum contract for a cdylib crate.
}
