```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ m s s q l ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-mssql/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-mssql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[MICROSOFT SQL SERVER CLIENT FOR STRYKE // QUERY + EXECUTE + PARAMS + TRANSACTIONS + INTROSPECTION]`

> *"T-SQL, one stryke pipe at a time."*

Microsoft SQL Server / Azure SQL client for stryke. Parametrized query and
execute, transaction batches, scalar/exists helpers, and schema introspection
against any SQL Server 2012+ or Azure SQL — over `tiberius` (the pure-Rust TDS
driver). Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-mysql`](https://github.com/MenkeTechnologies/stryke-mysql)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] Connecting](#0x02-connecting)
- [\[0x03\] Architecture](#0x03-architecture)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Build & test](#0x05-build--test)
- [\[0x06\] License](#0x06-license)

---

## \[0x00\] Install

```sh
s add github.com/MenkeTechnologies/stryke-mssql
```

On first `use Mssql`, stryke dlopens the cdylib in-process and registers every
`mssql__*` export.

---

## \[0x01\] Quick start

```perl
use Mssql

var %conn = (
    host       => "localhost",
    database   => "app",
    username   => "sa",
    password   => $ENV{MSSQL_PASS},
    trust_cert => 1,           # for a dev cert
)

# parametrized query — @P1, @P2, ...
val @users = Mssql::query("SELECT id, name FROM users WHERE active = @P1", params => [1], %conn)
p scalar(@users)

# scalar + exists
p Mssql::scalar("SELECT COUNT(*) FROM users", %conn)
p Mssql::exists("SELECT 1 FROM users WHERE id = @P1", params => [42], %conn)

# write
Mssql::execute("UPDATE users SET name = @P1 WHERE id = @P2", params => ["Ada", 42], %conn)

# transaction batch (commit on success, rollback on any failure)
Mssql::batch(
    ["INSERT INTO audit (msg) VALUES ('x')", "UPDATE users SET seen = 1 WHERE id = 42"],
    %conn,
)
```

---

## \[0x02\] Connecting

`%conn` (or `$MSSQL_URL` as an ADO connection string fallback):

| Key          | Default       | Notes                                              |
| ------------ | ------------- | -------------------------------------------------- |
| `url`        | —             | ADO string, e.g. `Server=db;Database=app;User Id=sa;Password=...` |
| `host`       | `127.0.0.1`   |                                                    |
| `port`       | `1433`        |                                                    |
| `database`   | server default |                                                   |
| `username`   | —             | SQL auth user                                      |
| `password`   | —             | SQL auth password                                  |
| `encrypt`    | `required`    | `required`, `off`, or `not_supported`              |
| `trust_cert` | `false`       | `true` accepts a self-signed server cert (dev)     |

A `tiberius` `Client` is cached per `(host, port, db, auth, encrypt)`; a
connection that errors is evicted and reopened on the next call.

---

## \[0x03\] Architecture

- **Driver** — [`tiberius`](https://docs.rs/tiberius), the pure-Rust TDS
  implementation. No FreeTDS, no ODBC, no system driver.
- **Blocking facade** — tiberius is async, so the cdylib owns one tokio runtime
  and `block_on`s each call, matching the sync model the other stryke data
  packages use.
- **Typed rows** — each cell is converted to JSON by trying the TDS types in
  order (int/float/bool/string/decimal/uuid/datetime/binary), so arbitrary
  result sets round-trip without a per-query schema. Decimals preserve precision
  as strings; binary is base64; datetimes are ISO-8601.
- **Pure helpers** — the ADO parse/redact helpers take no connection and are
  unit-tested in-crate.

---

## \[0x04\] API reference

| Group         | Functions                                                              |
| ------------- | ---------------------------------------------------------------------- |
| Liveness      | `version`, `ping`, `server_version`                                     |
| Query         | `query`, `query_one`, `scalar`, `exists`, `simple_query`               |
| Write         | `execute`, `batch` (transaction)                                       |
| Introspection | `databases`, `tables`, `columns`                                       |
| SQL helpers   | `quote_ident`, `quote_literal`, `valid_identifier`, `escape_like`, `format_value`, `format_in_list`, `split_batch` |
| URL helpers   | `parse_url`, `redact_url`, `build_url`                                 |

Parametrized statements use `@P1`, `@P2`, … placeholders bound from `params`
(null / bool / integer / float / string).

---

## \[0x05\] Build & test

```sh
make debug       # cargo build
make test        # cargo test, then `s test t/` (live needs $MSSQL_HOST)
make install     # s pkg install -g .
```

`cargo test` runs the in-crate unit tests (ADO parse/redact, defaults, param
extraction) with no database. Point `$MSSQL_HOST` (+ `$MSSQL_USER` /
`$MSSQL_PASS`) at a throwaway SQL Server container to exercise the query path.

---

## \[0x06\] License

MIT &middot; MenkeTechnologies
