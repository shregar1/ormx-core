//! Static SQL validation for migration scripts (runs before apply/rollback).
//!
//! The validator must never be **stricter** than the executor, which hands the whole
//! migration script to Postgres verbatim (see `apply_migration_records`). A full SQL
//! parser (sqlparser) is stricter than Postgres — it rejects many statements Postgres
//! runs happily, e.g. `CREATE TYPE ... AS ENUM`, `DROP TYPE`, `ALTER TYPE ... ADD VALUE`,
//! `REFRESH MATERIALIZED VIEW`, `LISTEN`/`NOTIFY`, and PL/pgSQL `DO $$ ... $$;` blocks.
//! Worse, its errors cannot distinguish "valid Postgres I don't support" from a genuine
//! typo (both `REFRESH MATERIALIZED VIEW mv;` and `CREAT TABLE t;` yield the identical
//! "Expected: an SQL statement, found: X"), so it cannot be used as a syntax gate without
//! rejecting valid, executable migrations.
//!
//! Instead we validate only what is provably safe to reject — things Postgres would also
//! reject — and defer statement *syntax* to the database, which is transactional and
//! authoritative:
//!   * empty scripts,
//!   * scripts with no statements,
//!   * lexically unterminated string literals, quoted identifiers, dollar-quoted blocks,
//!     or block comments.
//!
//! The lexer understands the PostgreSQL token forms that affect where a statement ends:
//! standard `'...'` strings, `E'...'` escape strings (backslash escapes), `"..."` quoted
//! identifiers, `$$ ... $$` / `$tag$ ... $tag$` dollar quotes, and `--` / nested `/* */`
//! comments. Anything inside those is opaque, so a `;`, `'`, or `$$` within them never
//! splits a statement or is mistaken for an unterminated delimiter.

use crate::error::MigrateError;
use crate::runner::MigrationRecord;

/// Validate a migration version stem matches ORMX naming: ``{timestamp}_{name}``.
pub fn check_version_format(version: &str) -> Result<(), MigrateError> {
    let Some((ts, name)) = version.split_once('_') else {
        return Err(MigrateError::SqlCheck {
            context: version.to_string(),
            message: "version must be {timestamp}_{description}".into(),
        });
    };
    if ts.len() != 14 || !ts.chars().all(|c| c.is_ascii_digit()) {
        return Err(MigrateError::SqlCheck {
            context: version.to_string(),
            message: format!("timestamp prefix must be 14 digits, got {ts:?}"),
        });
    }
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(MigrateError::SqlCheck {
            context: version.to_string(),
            message: format!("description suffix must be snake_case, got {name:?}"),
        });
    }
    Ok(())
}

/// True if ``s`` holds something other than semicolons and whitespace.
fn has_content(s: &str) -> bool {
    s.chars().any(|c| c != ';' && !c.is_whitespace())
}

/// True if ``c`` can appear inside an unquoted SQL identifier.
fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Length, in chars, of a dollar-quote opening delimiter starting at ``start``
/// (where ``chars[start] == '$'``), or ``None`` if this ``$`` does not open one.
///
/// Recognizes ``$$`` and tagged ``$tag$`` delimiters. A ``$`` followed by a digit
/// (e.g. ``$1``) is a positional parameter, not a dollar quote.
fn dollar_delim_len(chars: &[char], start: usize) -> Option<usize> {
    debug_assert_eq!(chars[start], '$');
    let mut j = start + 1;
    // Optional tag: identifier-like, must not start with a digit.
    if j < chars.len() && (chars[j].is_alphabetic() || chars[j] == '_') {
        j += 1;
        while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
    }
    if j < chars.len() && chars[j] == '$' {
        Some(j - start + 1)
    } else {
        None
    }
}

/// Consume a quoted run beginning at ``chars[start]`` (the opening ``quote``), appending
/// it to ``cur`` and counting newlines into ``line``. A doubled delimiter (``''`` / ``""``)
/// escapes itself. When ``backslash_escapes`` is true (PostgreSQL ``E'...'`` strings) a
/// backslash escapes the following char, so ``\'`` does not close the run.
///
/// Returns the index just past the closing quote, or ``None`` if the run is unterminated.
fn consume_quoted(
    chars: &[char],
    start: usize,
    quote: char,
    backslash_escapes: bool,
    cur: &mut String,
    line: &mut usize,
) -> Option<usize> {
    let n = chars.len();
    cur.push(chars[start]);
    let mut i = start + 1;
    while i < n {
        let c = chars[i];
        if backslash_escapes && c == '\\' {
            cur.push(c);
            if i + 1 < n {
                if chars[i + 1] == '\n' {
                    *line += 1;
                }
                cur.push(chars[i + 1]);
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if c == quote {
            // A doubled delimiter is an escaped literal, not the end of the run.
            if i + 1 < n && chars[i + 1] == quote {
                cur.push(c);
                cur.push(quote);
                i += 2;
                continue;
            }
            cur.push(c);
            return Some(i + 1);
        }
        if c == '\n' {
            *line += 1;
        }
        cur.push(c);
        i += 1;
    }
    None
}

/// Split ``sql`` into top-level statements, honoring string literals, quoted identifiers,
/// comments, and dollar-quoted blocks so that a ``;`` inside any of them (e.g. a PL/pgSQL
/// ``DO`` block body) does not split a statement.
///
/// Returns the content-bearing statement texts, or an error message if a string, quoted
/// identifier, dollar-quoted block, or block comment is left unterminated — a lexical
/// mistake Postgres rejects too, so it is safe for the validator to reject it as well.
fn scan_statements(sql: &str) -> Result<Vec<String>, String> {
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut statements = Vec::new();
    let mut cur = String::new();
    let mut line = 1usize;
    let mut i = 0;

    while i < n {
        let c = chars[i];

        // Line comment: ``-- ...`` to end of line (the newline is left for the outer loop).
        if c == '-' && i + 1 < n && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' {
                cur.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Block comment: ``/* ... */`` (Postgres allows nesting).
        if c == '/' && i + 1 < n && chars[i + 1] == '*' {
            let start_line = line;
            let mut depth = 0usize;
            while i < n {
                if chars[i] == '/' && i + 1 < n && chars[i + 1] == '*' {
                    depth += 1;
                    cur.push('/');
                    cur.push('*');
                    i += 2;
                } else if chars[i] == '*' && i + 1 < n && chars[i + 1] == '/' {
                    depth -= 1;
                    cur.push('*');
                    cur.push('/');
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    cur.push(chars[i]);
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(format!("unterminated block comment starting at line {start_line}"));
            }
            continue;
        }

        // String literal: ``'...'`` (``''`` escape), or PostgreSQL ``E'...'`` escape string
        // (backslash escapes, so ``\'`` does not close it).
        if c == '\'' {
            let start_line = line;
            let escape_string = i >= 1
                && matches!(chars[i - 1], 'e' | 'E')
                && (i < 2 || !is_ident_char(chars[i - 2]));
            match consume_quoted(&chars, i, '\'', escape_string, &mut cur, &mut line) {
                Some(next) => i = next,
                None => {
                    return Err(format!("unterminated string literal starting at line {start_line}"))
                }
            }
            continue;
        }

        // Quoted identifier: ``"..."`` (``""`` escape). May legally contain ``;``, ``'``, ``$``.
        if c == '"' {
            let start_line = line;
            match consume_quoted(&chars, i, '"', false, &mut cur, &mut line) {
                Some(next) => i = next,
                None => {
                    return Err(format!(
                        "unterminated quoted identifier starting at line {start_line}"
                    ))
                }
            }
            continue;
        }

        // Dollar-quoted block: ``$$ ... $$`` or ``$tag$ ... $tag$``.
        if c == '$' {
            if let Some(delim_len) = dollar_delim_len(&chars, i) {
                let start_line = line;
                let delim: Vec<char> = chars[i..i + delim_len].to_vec();
                cur.extend(delim.iter());
                i += delim_len;
                let mut closed = false;
                while i < n {
                    if chars[i] == '$' && chars[i..].starts_with(delim.as_slice()) {
                        cur.extend(delim.iter());
                        i += delim_len;
                        closed = true;
                        break;
                    }
                    if chars[i] == '\n' {
                        line += 1;
                    }
                    cur.push(chars[i]);
                    i += 1;
                }
                if !closed {
                    let tag: String = delim.iter().collect();
                    return Err(format!(
                        "unterminated dollar-quoted block {tag} starting at line {start_line}"
                    ));
                }
                continue;
            }
        }

        // Statement terminator.
        if c == ';' {
            cur.push(c);
            if has_content(&cur) {
                statements.push(std::mem::take(&mut cur));
            } else {
                cur.clear();
            }
            i += 1;
            continue;
        }

        if c == '\n' {
            line += 1;
        }
        cur.push(c);
        i += 1;
    }

    if has_content(&cur) {
        statements.push(cur);
    }

    Ok(statements)
}

/// Validate ``sql`` and reject empty scripts, scripts with no statements, or scripts
/// with lexically unterminated strings/identifiers/dollar-quotes/comments.
///
/// Statement *syntax* is intentionally **not** checked here: the executor hands the
/// script straight to Postgres, which supports constructs no static SQL parser fully
/// models (PL/pgSQL ``DO`` blocks, dollar-quoted function bodies, enum types, etc.).
/// Rejecting those at validation time would block valid, executable migrations.
pub fn check_sql_script(sql: &str, context: &str) -> Result<(), MigrateError> {
    if sql.trim().is_empty() {
        return Err(MigrateError::SqlCheck {
            context: context.to_string(),
            message: "SQL script is empty".into(),
        });
    }

    let statements = scan_statements(sql).map_err(|message| MigrateError::SqlCheck {
        context: context.to_string(),
        message,
    })?;

    if statements.is_empty() {
        return Err(MigrateError::SqlCheck {
            context: context.to_string(),
            message: "SQL script contains no statements".into(),
        });
    }

    Ok(())
}

/// Validate UP/DOWN SQL and version format for one migration record.
pub fn check_migration_record(record: &MigrationRecord) -> Result<(), MigrateError> {
    check_version_format(&record.version)?;
    check_sql_script(
        &record.up,
        &format!("{} UP", record.version),
    )?;
    check_sql_script(
        &record.down,
        &format!("{} DOWN", record.version),
    )?;
    Ok(())
}

/// Validate every migration record before apply/rollback.
pub fn check_migration_records(records: &[MigrationRecord]) -> Result<(), MigrateError> {
    for record in records {
        check_migration_record(record)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(version: &str, up: &str, down: &str) -> MigrationRecord {
        MigrationRecord {
            version: version.into(),
            up: up.into(),
            down: down.into(),
        }
    }

    #[test]
    fn accepts_valid_ddl() {
        let sql = "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL);";
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_multi_statement() {
        let sql = "CREATE TABLE a (id INT);\nCREATE INDEX idx_a ON a (id);";
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn rejects_empty() {
        assert!(check_sql_script("", "test").is_err());
        assert!(check_sql_script("   \n  ", "test").is_err());
    }

    #[test]
    fn rejects_only_semicolons() {
        assert!(check_sql_script(";;;", "test").is_err());
    }

    #[test]
    fn checks_record_up_and_down() {
        let record = rec(
            "20260524100001_users",
            "CREATE TABLE users (id BIGSERIAL PRIMARY KEY);",
            "DROP TABLE users;",
        );
        check_migration_record(&record).unwrap();
    }

    #[test]
    fn rejects_bad_version() {
        let record = rec("users", "SELECT 1;", "SELECT 1;");
        assert!(check_migration_record(&record).is_err());
    }

    #[test]
    fn accepts_plpgsql_do_block() {
        // Guarded/idempotent DDL via an anonymous PL/pgSQL block. A static SQL parser
        // cannot parse this, but Postgres runs it — the validator must not reject it.
        let sql = "DO $$\n\
                   BEGIN\n\
                   IF NOT EXISTS (\n\
                       SELECT 1 FROM information_schema.columns\n\
                       WHERE table_name = 'users' AND column_name = 'age'\n\
                   ) THEN\n\
                       ALTER TABLE users ADD COLUMN age INT;\n\
                   END IF;\n\
                   END $$;";
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_do_block_in_migration_record() {
        let record = rec(
            "20260524100001_guarded_mood_type",
            "DO $$ BEGIN\n\
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'mood') THEN\n\
                    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');\n\
                END IF;\n\
             END $$;",
            "DROP TYPE IF EXISTS mood;",
        );
        check_migration_record(&record).unwrap();
    }

    #[test]
    fn accepts_dollar_quoted_function_body() {
        let sql = "CREATE OR REPLACE FUNCTION bump() RETURNS trigger AS $$\n\
                   BEGIN\n\
                       NEW.updated_at = now();\n\
                       RETURN NEW;\n\
                   END;\n\
                   $$ LANGUAGE plpgsql;";
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_tagged_dollar_quote() {
        let sql = "DO $body$ BEGIN PERFORM 1; END $body$;";
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_plain_ddl_around_do_block() {
        // A DO block whose body contains ``;`` must not break statement splitting,
        // and the ordinary statements around it must still be recognized.
        let sql = "ALTER TABLE users ADD COLUMN flag BOOLEAN;\n\
                   DO $$ BEGIN UPDATE users SET flag = true; END $$;\n\
                   CREATE INDEX idx_users_flag ON users (flag);";
        let stmts = scan_statements(sql).unwrap();
        assert_eq!(stmts.len(), 3, "DO block must count as exactly one statement");
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_postgres_statements_a_sql_parser_rejects() {
        // All valid Postgres the executor runs but sqlparser 0.51 rejects. The whole
        // point of the fix: none of these may be blocked at validation time.
        for sql in [
            "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');",
            "DROP TYPE IF EXISTS mood;",
            "ALTER TYPE mood ADD VALUE 'great';",
            "REFRESH MATERIALIZED VIEW mv;",
            "LISTEN channel;",
            "NOTIFY channel, 'payload';",
        ] {
            check_sql_script(sql, "test").unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
        }
    }

    #[test]
    fn semicolon_inside_string_does_not_split() {
        let sql = "INSERT INTO notes (body) VALUES ('a; b; c');";
        let stmts = scan_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn accepts_escape_string_with_quote_and_backslash() {
        // E'...' escape strings: \' is an escaped quote (does not close the string),
        // \\ is an escaped backslash. Both are valid Postgres the executor runs.
        let sql = "INSERT INTO t (a, b) VALUES (E'it\\'s ok', E'line\\\\nbreak');";
        let stmts = scan_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn escape_string_prefix_is_standalone_token_only() {
        // The trailing 'e' of an identifier (here `value`) must NOT turn the adjacent
        // regular string into an escape string.
        check_sql_script("UPDATE t SET note = 'plain' WHERE value = 'x';", "test").unwrap();
    }

    #[test]
    fn accepts_quoted_identifier_with_special_chars() {
        // Double-quoted identifiers may contain ; ' and $ — none may split a statement
        // or be mistaken for an unterminated string.
        let sql = "CREATE TABLE \"weird;name\" (\"it's\" INT, \"a$b\" TEXT);";
        let stmts = scan_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        check_sql_script(sql, "test").unwrap();
    }

    #[test]
    fn rejects_unterminated_dollar_quote() {
        // Postgres also rejects this, so the validator may — and the error names the
        // context so the failing migration is identifiable.
        let err = check_sql_script("DO $$ BEGIN PERFORM 1;", "test").unwrap_err();
        assert!(err.to_string().contains("test"));
        assert!(err.to_string().contains("dollar-quoted"));
    }

    #[test]
    fn rejects_unterminated_string() {
        assert!(check_sql_script("INSERT INTO t (a) VALUES ('oops);", "test").is_err());
    }

    #[test]
    fn rejects_unterminated_quoted_identifier() {
        assert!(check_sql_script("CREATE TABLE \"oops (id INT);", "test").is_err());
    }

    #[test]
    fn rejects_unterminated_block_comment() {
        assert!(check_sql_script("/* never closed\nCREATE TABLE t (id INT);", "test").is_err());
    }

    #[test]
    fn scanner_never_panics_on_adversarial_input() {
        // The scanner indexes into a char buffer in several places; truncated or
        // pathological delimiters must degrade to Ok/Err, never panic.
        let inputs = [
            "$",
            "$$",
            "$tag",
            "$tag$ unterminated",
            "$$ body with ; and ' inside",
            "'",
            "''",
            "E'",
            "E'\\",
            "\"",
            "\"\"",
            "--",
            "/*",
            "/* /* nested */",
            "*/",
            ";",
            "$1 $2 $3",
            "naïve 'café' \"über\" $emoji$ 🦀 ;$emoji$;",
            "DO $$",
            "SELECT $$x$$ , $$ ; $$ ;",
        ];
        for sql in inputs {
            let _ = scan_statements(sql);
            let _ = check_sql_script(sql, "fuzz");
        }
    }
}
