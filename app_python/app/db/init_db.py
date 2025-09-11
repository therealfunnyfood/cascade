#!/usr/bin/env python3
"""
Idempotent SQLite migration runner for the app.
- Applies *.sql files in lexicographic order from a migrations folder
- Records applied migrations with checksum and timestamp
- Sets WAL mode and sensible PRAGMAs
Usage:
  python app/db/init_db.py --db app.db --migrations app/db/migrations
"""

from __future__ import annotations
import argparse
import hashlib
import os
import sqlite3
import sys
from datetime import datetime
from typing import Iterable, Tuple

def compute_checksum(sql_text: str) -> str:
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()

def ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations(
          id TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          applied_at TEXT NOT NULL
        );
    """)
    conn.commit()

def get_applied(conn: sqlite3.Connection) -> dict[str, Tuple[str, str]]:
    rows = conn.execute("SELECT id, checksum, applied_at FROM schema_migrations").fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}

def set_performance_pragmas(conn: sqlite3.Connection) -> None:
    # WAL must be set outside a transaction to be sure it sticks
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    # Optional: increase page cache (negative = KB). Tune to your needs.
    # conn.execute("PRAGMA cache_size=-80000;")
    conn.commit()

def assert_fts5_available(conn: sqlite3.Connection) -> None:
    # Quick sanity check that FTS5 is compiled in (most modern SQLite builds have it)
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS __fts5_check USING fts5(x);")
        conn.execute("DROP TABLE __fts5_check;")
    except sqlite3.OperationalError as e:
        raise RuntimeError("Your SQLite build does not support FTS5, required for fast search.") from e

def iter_sql_files(migrations_dir: str) -> Iterable[Tuple[str, str]]:
    files = [f for f in os.listdir(migrations_dir) if f.lower().endswith(".sql")]
    files.sort()  # lexicographic order
    for fname in files:
        path = os.path.join(migrations_dir, fname)
        with open(path, "r", encoding="utf-8") as fh:
            yield fname, fh.read()

def apply_migration(conn: sqlite3.Connection, mig_id: str, sql_text: str) -> None:
    # executescript() runs multiple statements within an implicit transaction
    conn.executescript(sql_text)
    conn.execute(
        "INSERT INTO schema_migrations(id, checksum, applied_at) VALUES(?,?,?)",
        (mig_id, compute_checksum(sql_text), datetime.utcnow().isoformat(timespec="seconds") + "Z"),
    )
    conn.commit()

def main():
    ap = argparse.ArgumentParser(description="SQLite migration runner")
    ap.add_argument("--db", default="app_python/app/db/app.db", help="Path to SQLite DB file")
    ap.add_argument("--migrations", default="app_python/app/db/migrations", help="Folder with *.sql migrations")
    ap.add_argument("--dry-run", action="store_true", help="Parse and list migrations without applying")
    ap.add_argument("--verbose", "-v", action="store_true", help="Verbose logs")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)
    os.makedirs(args.migrations, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        set_performance_pragmas(conn)
        assert_fts5_available(conn)
        ensure_schema_migrations(conn)

        applied = get_applied(conn)
        pending: list[Tuple[str, str]] = []

        for mig_id, sql_text in iter_sql_files(args.migrations):
            checksum = compute_checksum(sql_text)
            if mig_id in applied:
                old_checksum, _when = applied[mig_id]
                if old_checksum != checksum:
                    print(f"ERROR: Migration '{mig_id}' already applied but checksum differs.\n"
                          f" - applied: {old_checksum}\n - file:    {checksum}", file=sys.stderr)
                    sys.exit(1)
                if args.verbose:
                    print(f"[skip] {mig_id} (already applied)")
                continue
            pending.append((mig_id, sql_text))

        if not pending:
            print("No pending migrations. Database is up to date.")
            return

        print(f"Applying {len(pending)} migration(s):")
        for mig_id, _ in pending:
            print(f" - {mig_id}")

        if args.dry_run:
            print("Dry run complete. Nothing applied.")
            return

        for mig_id, sql_text in pending:
            if args.verbose:
                print(f"[apply] {mig_id}")
            apply_migration(conn, mig_id, sql_text)

        print("Migrations applied successfully.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
