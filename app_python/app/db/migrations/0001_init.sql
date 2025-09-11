-- Safe performance defaults
PRAGMA foreign_keys = ON;
BEGIN;
CREATE TABLE IF NOT EXISTS cards(
    id INTEGER PRIMARY KEY,
    uuid TEXT UNIQUE,
    name TEXT NOT NULL,
    set_code TEXT,
    collector_no TEXT,
    type_line TEXT,
    oracle_text TEXT,
    image_small TEXT,
    image_large TEXT
);
CREATE INDEX IF NOT EXISTS idx_cards_name_set ON cards(name, set_code);
CREATE TABLE IF NOT EXISTS collection_items(
    id INTEGER PRIMARY KEY,
    card_id INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    qty_nonfoil INTEGER NOT NULL DEFAULT 0,
    qty_foil INTEGER NOT NULL DEFAULT 0,
    condition TEXT,
    location_tag TEXT,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_items_card ON collection_items(card_id);
CREATE TABLE IF NOT EXISTS card_price_latest(
    card_id INTEGER PRIMARY KEY REFERENCES cards(id) ON DELETE CASCADE,
    price_cents INTEGER NOT NULL,
    currency TEXT NOT NULL,
    as_of INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_asof ON card_price_latest(as_of DESC);
CREATE TABLE IF NOT EXISTS price_points(
    id INTEGER PRIMARY KEY,
    card_id INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    price_cents INTEGER NOT NULL,
    as_of INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_card_asof ON price_points(card_id, as_of DESC);
-- FTS5 index (external content) for fast name/type search
CREATE VIRTUAL TABLE IF NOT EXISTS cards_fts USING fts5(
    name,
    type_line,
    content = 'cards',
    content_rowid = 'id'
);
-- Triggers to keep FTS in sync with 'cards'
CREATE TRIGGER IF NOT EXISTS cards_ai
AFTER
INSERT ON cards BEGIN
INSERT INTO cards_fts(rowid, name, type_line)
VALUES (new.id, new.name, new.type_line);
END;
CREATE TRIGGER IF NOT EXISTS cards_ad
AFTER DELETE ON cards BEGIN
INSERT INTO cards_fts(cards_fts, rowid, name, type_line)
VALUES ('delete', old.id, old.name, old.type_line);
END;
CREATE TRIGGER IF NOT EXISTS cards_au
AFTER
UPDATE ON cards BEGIN
INSERT INTO cards_fts(cards_fts, rowid, name, type_line)
VALUES ('delete', old.id, old.name, old.type_line);
INSERT INTO cards_fts(rowid, name, type_line)
VALUES (new.id, new.name, new.type_line);
END;
COMMIT;