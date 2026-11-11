// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A process-wide cache of parsed statements, off by default.
//!
//! A client that sends its values as `?` parameters sends the same few
//! statement texts again and again, and each request lexes and parses its
//! text anew. With the cache on, a request whose text, parameter count and
//! entry point match a statement parsed before gets a copy of that statement.
//!
//! The parse of a text is a function of the text, the parameter count and
//! whether the entry point refuses `EXISTS`, so a cached statement is the one a
//! new parse would give. Only statements that parse are cached. A refusal
//! parses again on each request.
//!
//! The cache holds at most the configured number of texts and does not evict.
//! A text that arrives when the cache is full is parsed and not stored. A
//! client that writes its values into the text, so that each text differs,
//! fills the cache and then parses as with the cache off.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, PoisonError, RwLock};

use super::super::ast::Statement;
use super::PResult;

/// The number of texts the cache may hold. 0 turns the cache off.
static CAPACITY: AtomicUsize = AtomicUsize::new(0);

/// The parses one text may keep: one for each parameter count and entry point
/// seen, up to this many. A client sends one text with one count.
const PARSES_PER_TEXT: usize = 4;

/// The parses of one text: one for each parameter count and entry point seen.
struct Parsed {
    parameters: usize,
    refuse_exists: bool,
    statement: Statement,
}

static ENTRIES: LazyLock<RwLock<HashMap<Box<str>, Vec<Parsed>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Set the number of statement texts the cache may hold, and drop what it
/// holds. 0 turns the cache off.
pub fn set_statement_cache_capacity(texts: usize) {
    CAPACITY.store(texts, Ordering::Relaxed);
    ENTRIES
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
}

/// The parse of `input` for `parameters` and `refuse_exists`: from the cache
/// when it holds one, else from `parse`, which the cache then stores if it
/// parsed and there is room.
pub(super) fn parse_cached(
    input: &str,
    parameters: usize,
    refuse_exists: bool,
    parse: impl FnOnce() -> PResult<Statement>,
) -> PResult<Statement> {
    let mut capacity = CAPACITY.load(Ordering::Relaxed);
    // A test build turns the cache on when the variable is set, so the parser
    // tests can run with it.
    if capacity == 0
        && cfg!(test)
        && std::env::var_os("EXTENDDB_TEST_PARTIQL_STATEMENT_CACHE").is_some()
    {
        capacity = 1024;
    }
    if capacity == 0 {
        return parse();
    }
    {
        let entries = ENTRIES.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(parsed) = entries.get(input).and_then(|all| {
            all.iter()
                .find(|p| p.parameters == parameters && p.refuse_exists == refuse_exists)
        }) {
            return Ok(parsed.statement.clone());
        }
    }
    let statement = parse()?;
    let mut entries = ENTRIES.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(all) = entries.get_mut(input) {
        if all.len() < PARSES_PER_TEXT
            && !all
                .iter()
                .any(|p| p.parameters == parameters && p.refuse_exists == refuse_exists)
        {
            all.push(Parsed {
                parameters,
                refuse_exists,
                statement: statement.clone(),
            });
        }
    } else if entries.len() < capacity {
        entries.insert(
            input.into(),
            vec![Parsed {
                parameters,
                refuse_exists,
                statement: statement.clone(),
            }],
        );
    }
    Ok(statement)
}

/// The number of texts the cache holds.
#[cfg(test)]
pub(super) fn cached_texts() -> usize {
    ENTRIES.read().unwrap_or_else(PoisonError::into_inner).len()
}

#[cfg(test)]
mod tests {
    use super::{cached_texts, set_statement_cache_capacity};
    use crate::partiql::parser::{parse_for_execute_statement, parse_for_request};

    /// The tests here set the capacity, so they run one at a time.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // The cache is process-wide, and other parser tests run alongside this one
    // with it on. A cached parse is the parse of its text, so they see the
    // results they would see with it off.
    #[test]
    fn a_cached_parse_is_the_parse_of_its_text() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let insert = "INSERT INTO \"tbl\" VALUE {'pk': ?, 'a': ?}";
        let update = "UPDATE \"tbl\" SET a = ? WHERE pk = ?";
        let exists = "EXISTS(SELECT * FROM \"tbl\" WHERE pk = 'a')";
        let refused = "INSERT INTO \"tbl\" VALUE {'pk': ";

        set_statement_cache_capacity(0);
        let uncached = [
            parse_for_execute_statement(insert, 2),
            parse_for_execute_statement(insert, 1),
            parse_for_request(update, 2),
            parse_for_execute_statement(exists, 0),
            parse_for_request(exists, 0),
            parse_for_execute_statement(refused, 1),
        ];

        set_statement_cache_capacity(1024);
        for _ in 0..3 {
            let cached = [
                parse_for_execute_statement(insert, 2),
                parse_for_execute_statement(insert, 1),
                parse_for_request(update, 2),
                parse_for_execute_statement(exists, 0),
                parse_for_request(exists, 0),
                parse_for_execute_statement(refused, 1),
            ];
            for (a, b) in uncached.iter().zip(&cached) {
                assert_eq!(
                    a.as_ref().map_err(|e| &e.message),
                    b.as_ref().map_err(|e| &e.message)
                );
            }
        }
        // ExecuteStatement refuses EXISTS and a transaction member accepts it,
        // so the two entry points keep separate parses of one text.
        assert!(uncached[0].is_ok() && uncached[1].is_ok() && uncached[2].is_ok());
        assert!(uncached[3].is_err());
        assert!(uncached[4].is_ok());
        assert!(uncached[5].is_err());
        set_statement_cache_capacity(0);
    }

    #[test]
    fn the_cache_holds_at_most_its_capacity() {
        let _serial = SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_statement_cache_capacity(2);
        for i in 0..10 {
            let text = format!("INSERT INTO \"tbl{i}\" VALUE {{'pk': ?}}");
            assert!(parse_for_request(&text, 1).is_ok());
            assert!(cached_texts() <= 2);
        }
        set_statement_cache_capacity(0);
    }
}
