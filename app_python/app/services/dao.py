from __future__ import annotations
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

# ---- module globals ----
_conn: Optional[sqlite3.Connection] = None
_lock = threading.RLock()

# ---- helpers ----
def _now_ts() -> int:
    return int(time.time())

def _cur() -> sqlite3.Cursor:
    if _conn is None:
        raise RuntimeError("DAO not initialized. Call dao.init(db_path) first.")
    return _conn.cursor()

# ---- init / teardown ----
def init(db_path: str) -> sqlite3.Connection:
    """
    Opens a shared connection with performance PRAGMAs and ensures helpful indexes.
    Safe to call multiple times; returns the live connection.
    """
    global _conn
    if _conn is not None:
        return _conn

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # Performance & safety
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    # Helpful unique index so one row per printing lives in collection_items
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniq_items_card ON collection_items(card_id);")

    conn.commit()
    _set_conn(conn)
    return conn

def _set_conn(conn: sqlite3.Connection) -> None:
    global _conn
    _conn = conn

def close() -> None:
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None

# ---- cards ----
def get_card(card_id: int) -> Optional[Dict[str, Any]]:
    with _lock:
        cur = _cur()
        cur.execute("""
            SELECT id, uuid, name, set_code, collector_no, type_line, oracle_text, image_small, image_large
            FROM cards WHERE id = ?;
        """, (card_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def search_cards(q: str, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Fast search using FTS5 if available and query length >= 3; falls back to LIKE.
    Returns minimal card columns suitable for lists.
    """
    q = (q or "").strip()
    with _lock:
        cur = _cur()

        # Prefer FTS when query isn't too short and FTS table exists
        use_fts = len(q) >= 3
        if use_fts:
            try:
                cur.execute("""
                    SELECT c.id, c.uuid, c.name, c.set_code, c.collector_no, c.type_line
                    FROM cards_fts f
                    JOIN cards c ON c.id = f.rowid
                    WHERE f MATCH ?
                    ORDER BY bm25(f) ASC, c.name ASC
                    LIMIT ? OFFSET ?;
                """, (f"{q}*", limit, offset))
                rows = cur.fetchall()
                if rows:
                    return [dict(r) for r in rows]
            except sqlite3.OperationalError:
                # FTS table missing or MATCH syntax issue; fall through to LIKE
                pass

        # Fallback: LIKE (case-insensitive-ish)
        cur.execute("""
            SELECT id, uuid, name, set_code, collector_no, type_line
            FROM cards
            WHERE name LIKE ?
            ORDER BY name ASC
            LIMIT ? OFFSET ?;
        """, (f"%{q}%", limit, offset))
        return [dict(r) for r in cur.fetchall()]

# ---- collection ----
def add_to_collection(card_id: int, qty_nonfoil: int = 1, qty_foil: int = 0) -> Dict[str, Any]:
    """
    Upserts a single collection row (one row per card_id).
    If row exists, increments counts by the provided deltas; floors at zero.
    """
    now = _now_ts()
    with _lock:
        cur = _cur()
        cur.execute("SELECT id, qty_nonfoil, qty_foil FROM collection_items WHERE card_id = ?;", (card_id,))
        row = cur.fetchone()
        if row:
            new_nf = max(0, row["qty_nonfoil"] + int(qty_nonfoil))
            new_f  = max(0, row["qty_foil"] + int(qty_foil))
            if new_nf == 0 and new_f == 0:
                cur.execute("DELETE FROM collection_items WHERE id = ?;", (row["id"],))
            else:
                cur.execute("""
                    UPDATE collection_items
                    SET qty_nonfoil = ?, qty_foil = ?, updated_at = ?
                    WHERE id = ?;
                """, (new_nf, new_f, now, row["id"]))
        else:
            cur.execute("""
                INSERT INTO collection_items(card_id, qty_nonfoil, qty_foil, updated_at)
                VALUES(?,?,?,?);
            """, (card_id, max(0, qty_nonfoil), max(0, qty_foil), now))
        _conn.commit()
        return {"ok": True, "card_id": card_id, "updated_at": now}

def set_collection_quantities(card_id: int, qty_nonfoil: int, qty_foil: int) -> Dict[str, Any]:
    """Hard-set quantities (useful for editors). Deletes the row if both become zero."""
    now = _now_ts()
    qty_nonfoil = max(0, int(qty_nonfoil))
    qty_foil    = max(0, int(qty_foil))
    with _lock:
        cur = _cur()
        if qty_nonfoil == 0 and qty_foil == 0:
            cur.execute("DELETE FROM collection_items WHERE card_id = ?;", (card_id,))
        else:
            cur.execute("""
                INSERT INTO collection_items(card_id, qty_nonfoil, qty_foil, updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(card_id) DO UPDATE SET
                    qty_nonfoil=excluded.qty_nonfoil,
                    qty_foil=excluded.qty_foil,
                    updated_at=excluded.updated_at;
            """, (card_id, qty_nonfoil, qty_foil, now))
        _conn.commit()
        return {"ok": True, "card_id": card_id, "updated_at": now}

def remove_from_collection(card_id: int, qty_nonfoil: int = 1, qty_foil: int = 0) -> Dict[str, Any]:
    """Convenience to decrement counts."""
    return add_to_collection(card_id, -abs(qty_nonfoil), -abs(qty_foil))

def get_collection_page(
    limit: int = 100,
    offset: int = 0,
    sort: str = "name",          # name | set | updated | price
    direction: str = "ASC",
    q: Optional[str] = None      # optional name filter (LIKE)
) -> Dict[str, Any]:
    """
    Returns a page of collection rows joined with card and latest price.
    """
    sort_map = {
        "name":   "c.name",
        "set":    "c.set_code",
        "updated":"i.updated_at",
        "price":  "p.price_cents"
    }
    order_col = sort_map.get(sort, "c.name")
    direction = "DESC" if str(direction).upper().startswith("D") else "ASC"

    where_sql = ""
    params: List[Any] = []
    if q:
        where_sql = "WHERE c.name LIKE ?"
        params.append(f"%{q}%")

    sql = f"""
      SELECT
        i.id               AS item_id,
        c.id               AS card_id,
        c.name, c.set_code, c.collector_no,
        i.qty_nonfoil, i.qty_foil, i.updated_at,
        COALESCE(p.price_cents, 0) AS price_cents,
        p.currency, p.as_of
      FROM collection_items i
      JOIN cards c ON c.id = i.card_id
      LEFT JOIN card_price_latest p ON p.card_id = i.card_id
      {where_sql}
      ORDER BY {order_col} {direction}, c.name ASC
      LIMIT ? OFFSET ?;
    """
    params.extend([limit, offset])

    with _lock:
        cur = _cur()
        cur.execute(sql, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]

        # quick counts for pagination
        count_sql = f"SELECT COUNT(*) FROM collection_items i JOIN cards c ON c.id=i.card_id {where_sql};"
        cur.execute(count_sql, tuple(params[:-2]))  # reuse only the filter param(s)
        total = int(cur.fetchone()[0])

    return {"rows": rows, "total": total, "limit": limit, "offset": offset}

def get_collection_summary() -> Dict[str, Any]:
    """Aggregate stats used for a header (unique, copies, total value)."""
    with _lock:
        cur = _cur()
        cur.execute("""
            SELECT
              COUNT(*)                                     AS unique_cards,
              COALESCE(SUM(i.qty_nonfoil + i.qty_foil),0)  AS total_copies,
              COALESCE(SUM((i.qty_nonfoil + i.qty_foil) * COALESCE(p.price_cents,0)),0) AS total_value_cents
            FROM collection_items i
            LEFT JOIN card_price_latest p ON p.card_id = i.card_id;
        """)
        row = cur.fetchone()
        return dict(row) if row else {"unique_cards": 0, "total_copies": 0, "total_value_cents": 0}

# ---- prices ----
def upsert_price_latest(card_id: int, price_cents: int, as_of_ts: int, currency: str = "USD") -> None:
    """
    Inserts or updates the latest price snapshot for a card.
    """
    with _lock:
        _cur().execute("""
            INSERT INTO card_price_latest(card_id, price_cents, currency, as_of)
            VALUES(?,?,?,?)
            ON CONFLICT(card_id) DO UPDATE SET
              price_cents=excluded.price_cents,
              currency=excluded.currency,
              as_of=excluded.as_of;
        """, (int(card_id), int(price_cents), currency, int(as_of_ts)))
        _conn.commit()

def get_price_latest(card_id: int) -> Optional[Dict[str, Any]]:
    with _lock:
        cur = _cur()
        cur.execute("""
            SELECT card_id, price_cents, currency, as_of
            FROM card_price_latest WHERE card_id = ?;
        """, (card_id,))
        row = cur.fetchone()
        return dict(row) if row else None

# ---- maintenance ----
def rebuild_cards_fts() -> None:
    """
    Rebuilds FTS index from cards table (useful after large imports).
    """
    with _lock:
        cur = _cur()
        cur.execute("DELETE FROM cards_fts;")
        cur.execute("""
            INSERT INTO cards_fts(rowid, name, type_line)
            SELECT id, name, type_line FROM cards;
        """)
        _conn.commit()
