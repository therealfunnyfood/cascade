import csv, sqlite3, sys

DB = "app_python/app/db/app.db"
CSV = "app_python/app/db/test_seed.csv"

with sqlite3.connect(DB) as conn, open(CSV, newline="", encoding="utf-8") as f:
    conn.execute("PRAGMA foreign_keys=ON;")
    r = csv.DictReader(f)
    rows = [(x["uuid"], x["name"], x.get("set_code"), x.get("collector_no"),
             x.get("type_line"), x.get("oracle_text"), x.get("image_small"), x.get("image_large")) for x in r]
    conn.executemany("""
      INSERT INTO cards(uuid,name,set_code,collector_no,type_line,oracle_text,image_small,image_large)
      VALUES(?,?,?,?,?,?,?,?)
      ON CONFLICT(uuid) DO UPDATE SET
        name=excluded.name, set_code=excluded.set_code, collector_no=excluded.collector_no,
        type_line=excluded.type_line, oracle_text=excluded.oracle_text,
        image_small=excluded.image_small, image_large=excluded.image_large
    """, rows)
    # rebuild FTS entries (fast for dev seeds)
    conn.execute("DELETE FROM cards_fts;")
    conn.execute("""
      INSERT INTO cards_fts(rowid, name, type_line)
      SELECT id, name, type_line FROM cards;
    """)
    conn.commit()
print(f"Seeded {len(rows)} cards into {DB}")

#TODO make sure uuid on cards is the scryfall unique id thing
#TODO make db of all unique cards 