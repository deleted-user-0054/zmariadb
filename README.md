# MyZql
- MySQL and MariaDB driver in native zig

## Status
- Beta

## Zig Support
- Zig 0.16.0 only

## Features
- Native Zig code, no external dependencies
- TCP protocol
- Transactions
- Savepoints inside transactions
- Connection Pooling
- Automatic reconnect for dead sessions after a communication failure, before a later command (not inside a lost transaction)
- Prepared Statement
- Structs from query result
- Data insertion
- MySQL DateTime and Time support

## Requirements
- MySQL/MariaDB 5.7.5 and up

## Current Limitations
- Supported authentication plugins are `mysql_native_password`, `sha256_password`, and `caching_sha2_password`
- Authentication plugin switch requests are handled for `mysql_native_password`, `mysql_clear_password`, `sha256_password`, `caching_sha2_password`, and Windows SSPI `auth_gssapi_client`
- `auth_gssapi_client` currently requires Windows SSPI and links against `secur32`; non-Windows GSSAPI clients are not implemented yet
- `Config.multi_statements` is reserved for future work and currently returns `error.UnsupportedMultiStatements`

## TODOs
- Config from URL

## Add as dependency to your Zig project
### Fetch dependency
```bash
zig fetch --save git+https://github.com/speed2exe/myzql#main
```

### Import in your project
- `build.zig`
```zig
    //...
    const myzql_dep = b.dependency("myzql", .{});
    const myzql = myzql_dep.module("myzql");
    exe.root_module.addImport("myzql", myzql);
    //...
```

## Usage
- Project integration example: [Usage](https://github.com/speed2exe/myzql-example)
- README examples below focus on the stable high-level root API from `@import("myzql")`.

### Connection
```zig
const std = @import("std");
const myzql = @import("myzql");
const Conn = myzql.Conn;

pub fn main() !void {
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();

    // Setting up client
    var client = try Conn.init(
        io,
        allocator,
        &.{
            .username = "some-user",   // default: "root"
            .password = "password123", // default: ""
            .database = "customers",   // default: ""

            // Current default value.
            .address = myzql.Address.localhost(3306),
            .reconnect = .{
                .enabled = true,
                .max_attempts = 3,
                .retry_delay_ms = 100,
                .retry_backoff_multiplier = 2,
                .max_retry_delay_ms = 2_000,
                .jitter_ms = 25,
            },
            // ...
        },
    );
    defer client.deinit(allocator);

    // Connection and Authentication
    try client.ping();
}
```

### Transactions
```zig
var tx = try client.begin();
defer tx.deinit();

const query_res = try tx.query("INSERT INTO logs VALUES (1)");
_ = try query_res.expect(.ok);
try tx.commit();
```

### Connection Pooling
```zig
var threaded: std.Io.Threaded = .init_single_threaded;
const io = threaded.io();

var pool = try myzql.Pool.init(io, allocator, &.{
    .username = "some-user",
    .password = "password123",
    .database = "customers",
    .reconnect = .{ .enabled = true },
}, .{ .max_connections = 8 });
defer pool.deinit();

// Optional pool waiting behavior when all connections are busy:
// .{
//     .max_connections = 8,
//     .acquire_timeout_ms = 500,
//     .acquire_retry_interval_ms = 10,
//     .max_lifetime_ms = 30_000,
//     .max_idle_ms = 10_000,
//     .health_check_query = "SELECT 1",
// }

const pool_stats = pool.statsSnapshot();
// pool_stats.acquire_requests / acquire_timeouts / connections_discarded / ...

var lease = try pool.acquire();
defer lease.deinit();

const query_res = try lease.queryRows(allocator, "SELECT 1");
_ = try query_res.expect(.rows);
```

`Lease` owns the checked-out session lifetime. If you call `lease.begin()`, keep the `Tx` inside that lease scope and finish it before releasing the lease.

## Querying
```zig


pub fn main() !void {
    // ...
    // You can do a text query (text protocol) by using `query` method on `Conn`
    const result = try client.query("CREATE DATABASE testdb");

    // Query results can have a few variant:
    // - ok:   success packet => query is ok
    // - err:  error packet   => error occurred
    // In this example, res will either be `ok` or `err`.
    // We are using the convenient method `expect` for simplified error handling.
    // If the result variant does not match the kind of result you have specified,
    // a message will be printed and you will get an error instead.
    const ok = try result.expect(.ok);

    // Alternatively, you can also handle results manually for more control.
    // Here, we do a switch statement to handle all possible variant or results.
    switch (result) {
        .ok => |ok| {},

        // `asError` is also another convenient method to print message and return as zig error.
        // You may also choose to inspect individual fields for more control.
        .err => |err| return err.asError(),
    }
}
```

## Querying returning rows (Text Results)
- If you want to have query results to be represented by custom created structs,
this is not the section, scroll down to "Executing prepared statements returning results" instead.
```zig
const myzql = @import("myzql");

pub fn main() !void {
    const result = try c.queryRows(allocator, "SELECT * FROM customers.purchases");

    // This is a query that returns rows, you have to collect the result.
    // you can use `expect(.rows)` to interpret the query result as a row set
    const rows = try result.expect(.rows);

    // Allocation-free iterator over rows
    const rows_iter = rows.iter();
    while (try rows_iter.next()) |row| {
        // Option 1: Iterate through every element in the row
        var elems_iter = row.iter();
        while (elems_iter.next()) |elem| { // ?[]const u8
            std.debug.print("{?s} ", .{elem});
        }

        // Option 2: Collect all elements in the row into a slice
        const text_elems = try row.textElems(allocator);
        defer text_elems.deinit(allocator); // elems are valid until deinit is called
        const elems: []const ?[]const u8 = text_elems.elems;
        std.debug.print("elems: {any}\n", .{elems});
    }
}
```

```zig
    // You can also use `tableTexts` to collect all rows at once.
    // Under the hood, it does network calls and allocations, until EOF or error.
    // Results are valid until `deinit` is called on TableTexts.
    const result = try c.queryRows(allocator, "SELECT * FROM customers.purchases");
    const rows = try result.expect(.rows);
    const table = try rows.tableTexts(allocator);
    defer table.deinit(allocator); // table is valid until deinit is called
    std.debug.print("table: {any}\n", .{table.table});
```

### Data Insertion
- Let's assume that you have a table of this structure:
```sql
CREATE TABLE test.person (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
)
```

```zig
const myzql = @import("myzql");

pub fn main() !void {
    // In order to do a insertion, you would first need to do a prepared statement.
    // Allocation is required as we need to store metadata of parameters and return type
    const prep_res = try c.prepare(allocator, "INSERT INTO test.person (name, age) VALUES (?, ?)");
    defer prep_res.deinit(allocator);
    const prep_stmt = try prep_res.expect(.stmt);

    // Data to be inserted
    const params = .{
        .{ "John", 42 },
        .{ "Sam", 24 },
    };
    inline for (params) |param| {
        const exe_res = try c.execute(&prep_stmt, param);
        const ok = try exe_res.expect(.ok); // expecting ok here because there's no rows returned
        const last_insert_id: u64 = ok.last_insert_id;
        std.debug.print("last_insert_id: {any}\n", .{last_insert_id});
    }

    // Currently only tuples are supported as an argument for insertion.
    // There are plans to include named structs in the future.
}
```

### Executing prepared statements returning results as structs
```zig
fn main() !void {
    const prep_res = try c.prepare(allocator, "SELECT name, age FROM test.person");
    defer prep_res.deinit(allocator);
    const prep_stmt = try prep_res.expect(.stmt);

    // This is the struct that represents the columns of a single row.
    const Person = struct {
        name: []const u8,
        age: u8,
    };

    { // Iterating over rows, scanning into struct or creating struct
        const query_res = try c.executeRows(allocator, &prep_stmt, .{}); // no parameters because there's no ? in the query
        const rows = try query_res.expect(.rows);
        const rows_iter = rows.iter();
        while (try rows_iter.next()) |row| {
            var person: Person = undefined;
            try row.scan(&person);
            std.debug.print("person: {any}\n", .{person});
            // Important: if any field is a string, it stays valid until the next row is scanned
            // or the next query starts. Use `tableStructs` below if you need owned data.
        }
    }

    { // collect all rows into a table ([]const Person)
        const query_res = try c.executeRows(allocator, &prep_stmt, .{}); // no parameters because there's no ? in the query
        const rows = try query_res.expect(.rows);
        const rows_iter = rows.iter();
        const person_structs = try rows_iter.tableStructs(Person, allocator);
        defer person_structs.deinit(allocator); // data is valid until deinit is called
        std.debug.print("person_structs: {any}\n", .{person_structs.struct_list.items});
    }
}
```

### Temporal Types Support (DateTime, Time)
- Example of using DateTime and Time MySQL column types.
- Let's assume you already got this table set up:
```sql
CREATE TABLE test.temporal_types_example (
    event_time DATETIME(6) NOT NULL,
    duration TIME(6) NOT NULL
)
```


```zig

const DateTime = myzql.DateTime;
const Duration = myzql.Duration;

fn main() !void {
    { // Insert
        const prep_res = try c.prepare(allocator, "INSERT INTO test.temporal_types_example VALUES (?, ?)");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.stmt);

        const my_time: DateTime = .{
            .year = 2023,
            .month = 11,
            .day = 30,
            .hour = 6,
            .minute = 50,
            .second = 58,
            .microsecond = 123456,
        };
        const my_duration: Duration = .{
            .days = 1,
            .hours = 23,
            .minutes = 59,
            .seconds = 59,
            .microseconds = 123456,
        };
        const params = .{
            .{ my_time, my_duration },
        };
        inline for (params) |param| {
            const exe_res = try c.execute(&prep_stmt, param);
            _ = try exe_res.expect(.ok);
        }
    }

    { // Select
        const DateTimeDuration = struct {
            event_time: DateTime,
            duration: Duration,
        };
        const prep_res = try c.prepare(allocator, "SELECT * FROM test.temporal_types_example");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.stmt);
        const res = try c.executeRows(allocator, &prep_stmt, .{});
        const rows = try res.expect(.rows);
        const rows_iter = rows.iter();

        const structs = try rows_iter.tableStructs(DateTimeDuration, allocator);
        defer structs.deinit(allocator);
        std.debug.print("structs: {any}\n", .{structs.struct_list.items}); // structs.struct_list.items: []const DateTimeDuration
        // Do something with structs
    }
}
```

### Arrays Support
- Assume that you have the SQL table:
```sql
CREATE TABLE test.array_types_example (
    name VARCHAR(16) NOT NULL,
    mac_addr BINARY(6)
)
```

```zig
fn main() !void {
    { // Insert
        const prep_res = try c.prepare(allocator, "INSERT INTO test.array_types_example VALUES (?, ?)");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.stmt);

        const params = .{
            .{ "John", &[_]u8 { 0xFE } ** 6 },
            .{ "Alice", null }
        };
        inline for (params) |param| {
            const exe_res = try c.execute(&prep_stmt, param);
            _ = try exe_res.expect(.ok);
        }
    }

    { // Select
        const Client = struct {
            name: [16:1]u8,
            mac_addr: ?[6]u8,
        };
        const prep_res = try c.prepare(allocator, "SELECT * FROM test.array_types_example");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.stmt);
        const res = try c.executeRows(allocator, &prep_stmt, .{});
        const rows = try res.expect(.rows);
        const rows_iter = rows.iter();

        const structs = try rows_iter.tableStructs(Client, allocator);
        defer structs.deinit(allocator);
        std.debug.print("structs: {any}\n", .{structs.struct_list.items}); // structs.struct_list.items: []const Client
        // Do something with structs
    }
}
```
- Arrays will be initialized by their sentinel value. In this example, the value of the `name` field corresponding to `John`'s row will be `[16:1]u8 { 'J', 'o', 'h', 'n', 1, 1, 1, ... }`
- If the array doesn't have a sentinel value, it will be zero-initialized.
- Insufficiently sized arrays will silently truncate excess data

## Unit Tests
- `zig build unit_test`

## Integration Tests
- Start up mysql/mariadb in docker:
```bash
# MySQL
docker run --name some-mysql --env MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mysql
```
```bash
# MariaDB
docker run --name some-mariadb --env MARIADB_ROOT_PASSWORD=password -p 3306:3306 -d mariadb
```
- Run all the test: In root directory of project:
```bash
zig build integration_test
```

- If your test database is not on the default local port, pass build options such as:
```bash
zig build integration_test -Dtest-db-port=3307 -Dtest-db-user=root -Dtest-db-password=password -Dtest-db-name=mysql
```

## Philosophy
### Correctness
Focused on correct representation of server client protocol.
### Public API
The documented API is the high-level root surface from `@import("myzql")`.
Protocol internals and other lower-level implementation details are intentionally not covered here.

### Binary Column Types support
- MySQL Colums Types to Zig Values
```
- Null -> ?T
- Int -> u64, u32, u16, u8
- Float -> f32, f64
- String -> []u8, []const u8, enum
```
