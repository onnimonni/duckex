// SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
// SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
//
// SPDX-License-Identifier: Apache-2.0

#![allow(non_local_definitions)]

use std::sync::Mutex;

use base64::{engine::general_purpose, Engine as _};

use duckdb::arrow::datatypes::DataType;
use duckdb::params_from_iter;
use duckdb::types::Value;
use duckdb::Connection;

use rustler::{Encoder, Env, NifStruct, ResourceArc, Term};

mod cache;

// Resource to hold the DuckDB connection and prepared query strings
pub struct DuckDBResource {
    conn: Mutex<Connection>,
    queries: Mutex<cache::Cache<String>>,
}

// Elixir-friendly data structures
#[derive(NifStruct)]
#[module = "Duckex.Result"]
struct DuckexResult<'a> {
    columns: Vec<Vec<String>>,
    rows: Vec<Vec<Term<'a>>>,
    num_rows: usize,
}

fn duckdb_value_to_term<'a>(env: Env<'a>, value: Value) -> Term<'a> {
    match value {
        Value::Null => rustler::types::atom::nil().encode(env),
        Value::Boolean(b) => b.encode(env),
        Value::TinyInt(i) => i.encode(env),
        Value::SmallInt(i) => i.encode(env),
        Value::Int(i) => i.encode(env),
        Value::BigInt(i) => i.encode(env),
        Value::UTinyInt(i) => i.encode(env),
        Value::USmallInt(i) => i.encode(env),
        Value::UInt(i) => i.encode(env),
        Value::UBigInt(i) => (i as i64).encode(env),
        Value::Float(f) => (f as f64).encode(env),
        Value::Double(f) => f.encode(env),
        Value::Timestamp(unit, value) => unit.to_micros(value).encode(env),
        Value::Date32(days) => days.encode(env),
        Value::Text(s) => s.encode(env),
        Value::Blob(b) => general_purpose::STANDARD.encode(b).encode(env),
        Value::Time64(unit, value) => unit.to_micros(value).encode(env),
        Value::List(vec) => vec
            .into_iter()
            .map(|v| duckdb_value_to_term(env, v))
            .collect::<Vec<_>>()
            .encode(env),
        Value::Enum(s) => s.encode(env),
        Value::Struct(s) => {
            let vec: Vec<_> = s.iter().map(|(k, v)| (k.clone(), duckdb_value_to_term(env, v.clone()))).collect();
            vec.encode(env)
        }
        Value::Map(m) => {
            let vec: Vec<_> = m.iter().map(|(k, v)| (duckdb_value_to_string(k.clone()), duckdb_value_to_term(env, v.clone()))).collect();
            vec.encode(env)
        }
        Value::Array(vec) => vec
            .into_iter()
            .map(|v| duckdb_value_to_term(env, v))
            .collect::<Vec<_>>()
            .encode(env),
        Value::Union(val) => duckdb_value_to_term(env, *val),
        _ => format!("{:?}", value).encode(env),
    }
}

fn duckdb_value_to_string(value: Value) -> String {
    match value {
        Value::Text(s) => s,
        Value::Blob(b) => general_purpose::STANDARD.encode(b),
        _ => format!("{:?}", value),
    }
}

// NIF functions
#[rustler::nif]
fn new(database_path: String, cache_size: Option<usize>) -> Result<ResourceArc<DuckDBResource>, String> {
    let conn = if database_path == ":memory:" {
        Connection::open_in_memory()
            .map_err(|e| format!("Failed to create in-memory DuckDB connection: {}", e))?
    } else {
        Connection::open(&database_path)
            .map_err(|e| format!("Failed to open DuckDB database at '{}': {}", database_path, e))?
    };

    let size = cache_size.unwrap_or(1024);
    let resource = DuckDBResource {
        conn: Mutex::new(conn),
        queries: Mutex::new(cache::Cache::with_capacity(size)),
    };

    Ok(ResourceArc::new(resource))
}

#[rustler::nif]
fn prepare<'a>(
    env: Env<'a>,
    resource: ResourceArc<DuckDBResource>,
    query: String,
) -> Result<Term<'a>, String> {
    let conn = resource.conn.lock().map_err(|e| e.to_string())?;
    let mut queries = resource.queries.lock().map_err(|e| e.to_string())?;

    // Validate the query by trying to prepare it
    let _ = conn
        .prepare(&query)
        .map_err(|e| format!("SQL preparation error: {}", e))?;

    // Store the query string for later execution
    let id = queries
        .store(query)
        .ok_or_else(|| "Exhausted prepared statements cache".to_string())?;

    let columns = vec![vec!["ref".to_string(), DataType::UInt32.to_string()]];
    let rows = vec![vec![id.encode(env)]];

    let result = DuckexResult {
        columns,
        rows,
        num_rows: 1,
    };

    Ok(result.encode(env))
}

#[rustler::nif]
fn execute<'a>(
    env: Env<'a>,
    resource: ResourceArc<DuckDBResource>,
    stmt_id: u32,
    params: Vec<Term<'a>>,
) -> Result<Term<'a>, String> {
    let conn = resource.conn.lock().map_err(|e| e.to_string())?;
    let queries = resource.queries.lock().map_err(|e| e.to_string())?;

    // Get the query string
    let query = queries
        .get_ref(stmt_id as usize)
        .ok_or_else(|| "Invalid cache index".to_string())?;

    // Prepare the statement (short-lived)
    let mut stmt = conn
        .prepare(query)
        .map_err(|e| format!("SQL preparation error: {}", e))?;

    // Convert Elixir terms to DuckDB parameters
    let params_vec: Vec<Value> = params
        .into_iter()
        .map(|term| term_to_duckdb_value(term))
        .collect::<Result<Vec<_>, _>>()?;

    let rows_result = stmt
        .query_map(params_from_iter(params_vec.iter()), |row| {
            (0..)
                .map_while(|i| match row.get::<_, Value>(i) {
                    Ok(val) => Some(Ok(val)),
                    Err(_) => None,
                })
                .collect()
        })
        .map_err(|e| format!("SQL execution error: {}", e))?;

    let rows: Vec<Vec<Value>> = rows_result
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("SQL row processing error: {}", e))?;

    let num_rows = rows.len();

    let columns: Vec<Vec<String>> = stmt
        .column_names()
        .into_iter()
        .enumerate()
        .map(|(idx, name)| vec![name, stmt.column_type(idx).to_string()])
        .collect();

    let result_rows: Vec<Vec<Term<'a>>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| duckdb_value_to_term(env, v)).collect())
        .collect();

    let result = DuckexResult {
        columns,
        rows: result_rows,
        num_rows,
    };

    Ok(result.encode(env))
}

#[rustler::nif]
fn close(resource: ResourceArc<DuckDBResource>, stmt_id: u32) -> Result<String, String> {
    let mut queries = resource.queries.lock().map_err(|e| e.to_string())?;
    queries.remove(stmt_id as usize);
    Ok("ok".to_string())
}

#[rustler::nif]
fn begin(resource: ResourceArc<DuckDBResource>) -> Result<String, String> {
    let conn = resource.conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("BEGIN")
        .map_err(|e| format!("SQL preparation error: {}", e))?;

    stmt.execute([])
        .map_err(|e| format!("SQL execution error: {}", e))?;

    Ok("ok".to_string())
}

#[rustler::nif]
fn commit(resource: ResourceArc<DuckDBResource>) -> Result<String, String> {
    let conn = resource.conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("COMMIT")
        .map_err(|e| format!("SQL preparation error: {}", e))?;

    stmt.execute([])
        .map_err(|e| format!("SQL execution error: {}", e))?;

    Ok("ok".to_string())
}

#[rustler::nif]
fn rollback(resource: ResourceArc<DuckDBResource>) -> Result<String, String> {
    let conn = resource.conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("ROLLBACK")
        .map_err(|e| format!("SQL preparation error: {}", e))?;

    stmt.execute([])
        .map_err(|e| format!("SQL execution error: {}", e))?;

    Ok("ok".to_string())
}

#[rustler::nif]
fn status(_resource: ResourceArc<DuckDBResource>) -> Result<String, String> {
    Ok("ok".to_string())
}

// Helper function to convert Elixir terms to DuckDB values
fn term_to_duckdb_value(term: Term) -> Result<Value, String> {
    // Check for DateTime struct first (map with __struct__ key)
    if term.is_map() {
        // Try to decode as a DateTime by using MapIterator
        use rustler::types::map::MapIterator;
        if let Some(iter) = MapIterator::new(term) {
            let mut map_data: std::collections::HashMap<String, Term> = std::collections::HashMap::new();
            for (key, value) in iter {
                if let Ok(key_str) = key.atom_to_string() {
                    map_data.insert(key_str, value);
                }
            }

            // Check if this is a DateTime or Date struct
            if let Some(struct_term) = map_data.get("__struct__") {
                if let Ok(module_str) = struct_term.atom_to_string() {
                    // Handle Date struct
                    // Note: DuckDB Rust library doesn't support Date32 for parameter binding,
                    // so we convert to ISO 8601 date string format (YYYY-MM-DD)
                    if module_str == "Elixir.Date" {
                        let year = map_data.get("year").and_then(|t| t.decode::<i32>().ok());
                        let month = map_data.get("month").and_then(|t| t.decode::<u32>().ok());
                        let day = map_data.get("day").and_then(|t| t.decode::<u32>().ok());

                        if let (Some(year_val), Some(month_val), Some(day_val)) = (year, month, day) {
                            // Format as ISO 8601 date string
                            let date_string = format!("{:04}-{:02}-{:02}", year_val, month_val, day_val);
                            return Ok(Value::Text(date_string));
                        }
                    }
                    // Handle DateTime struct
                    else if module_str == "Elixir.DateTime" {
                        // Extract DateTime fields
                        let year = map_data.get("year").and_then(|t| t.decode::<i32>().ok());
                        let month = map_data.get("month").and_then(|t| t.decode::<u32>().ok());
                        let day = map_data.get("day").and_then(|t| t.decode::<u32>().ok());
                        let hour = map_data.get("hour").and_then(|t| t.decode::<u32>().ok());
                        let minute = map_data.get("minute").and_then(|t| t.decode::<u32>().ok());
                        let second = map_data.get("second").and_then(|t| t.decode::<u32>().ok());
                        let microsecond = map_data.get("microsecond").and_then(|t| {
                            // microsecond is a tuple {value, precision}
                            t.decode::<(i64, u32)>().ok().map(|(v, _)| v)
                        });

                        if let (Some(year_val), Some(month_val), Some(day_val), Some(hour_val), Some(minute_val), Some(second_val), Some(microsecond_val)) =
                            (year, month, day, hour, minute, second, microsecond) {
                            // Calculate Unix timestamp in microseconds
                            let mut days = 0i64;

                            // Add days for years
                            for y in 1970..year_val {
                                if (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0) {
                                    days += 366;
                                } else {
                                    days += 365;
                                }
                            }

                            // Add days for months in current year
                            let is_leap = (year_val % 4 == 0 && year_val % 100 != 0) || (year_val % 400 == 0);
                            let days_in_month = [31, if is_leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
                            for m in 1..month_val {
                                days += days_in_month[(m - 1) as usize] as i64;
                            }

                            // Add days in current month
                            days += (day_val - 1) as i64;

                            // Convert to microseconds
                            let total_microseconds = days * 86400 * 1_000_000
                                + (hour_val as i64) * 3600 * 1_000_000
                                + (minute_val as i64) * 60 * 1_000_000
                                + (second_val as i64) * 1_000_000
                                + microsecond_val;

                            return Ok(Value::Timestamp(duckdb::types::TimeUnit::Microsecond, total_microseconds));
                        }
                    }
                }
            }
        }
    }

    if term.is_number() {
        if let Ok(i) = term.decode::<i64>() {
            return Ok(Value::BigInt(i));
        }
        if let Ok(f) = term.decode::<f64>() {
            return Ok(Value::Double(f));
        }
    }

    if let Ok(s) = term.decode::<String>() {
        return Ok(Value::Text(s));
    }

    if let Ok(b) = term.decode::<bool>() {
        return Ok(Value::Boolean(b));
    }

    if term.is_atom() {
        if let Ok(atom_str) = term.atom_to_string() {
            if atom_str == "nil" {
                return Ok(Value::Null);
            }
        }
    }

    // Provide detailed type information in error message
    let type_info = if term.is_number() {
        "number (but failed to decode as i64 or f64)"
    } else if term.is_map() {
        "map (unsupported structure)"
    } else if term.is_list() {
        "list (not yet supported)"
    } else if term.is_atom() {
        "atom (not nil)"
    } else if term.is_binary() {
        "binary (failed to decode as string)"
    } else if term.is_tuple() {
        "tuple (not supported)"
    } else {
        "unknown type"
    };

    Err(format!("Unsupported parameter type: {}", type_info))
}

fn on_load(env: Env, _info: Term) -> bool {
    rustler::resource!(DuckDBResource, env)
}

rustler::init!("Elixir.Duckex.Native", load = on_load);
