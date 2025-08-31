import sqlite3
import sys

DDL = r"""
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS images(
  id INTEGER PRIMARY KEY,
  rel_path TEXT NOT NULL,
  sha256 TEXT NOT NULL UNIQUE,
  created_at TEXT,
  run_id INTEGER,
  parent_sha256 TEXT
);

CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY,
  operator_username TEXT,
  tool_name TEXT,
  tool_version TEXT,
  pipeline_hash TEXT,
  params_json TEXT,
  ran_at TEXT
);

CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT
);

-- ※ category を NOT NULL DEFAULT '' にして式を使わず UNIQUE を張る
CREATE TABLE IF NOT EXISTS tags(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL DEFAULT '',
  description TEXT,
  UNIQUE(name, category)
);

CREATE TABLE IF NOT EXISTS annotations(
  id INTEGER PRIMARY KEY,
  image_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  is_deleted INTEGER NOT NULL DEFAULT 0,
  UNIQUE(image_id, tag_id, user_id),
  FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
  FOREIGN KEY(tag_id)   REFERENCES tags(id)   ON DELETE CASCADE,
  FOREIGN KEY(user_id)  REFERENCES users(id)  ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS quality_votes(
  id INTEGER PRIMARY KEY,
  image_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  label TEXT NOT NULL CHECK(label IN ('good','review','bad')),
  score REAL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(image_id, user_id),
  FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
  FOREIGN KEY(user_id)  REFERENCES users(id)  ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attachments(
  id INTEGER PRIMARY KEY,
  image_id INTEGER NOT NULL,
  kind TEXT NOT NULL,               -- 'csv' | 'json' | 'txt' | ...
  rel_path TEXT NOT NULL,
  sha256 TEXT NOT NULL UNIQUE,
  created_at TEXT,
  FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metrics(
  image_id INTEGER PRIMARY KEY,
  json TEXT NOT NULL,
  FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS authoritative(
  parent_sha256 TEXT PRIMARY KEY,
  run_id INTEGER NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(id)
);

-- 1画像あたり最大5人までの注釈を許可（is_deleted=0 の DISTINCT user_id を数える）
CREATE TRIGGER IF NOT EXISTS trg_limit_annotators
BEFORE INSERT ON annotations
BEGIN
  SELECT CASE WHEN (
    (SELECT COUNT(DISTINCT user_id)
       FROM annotations
      WHERE image_id = NEW.image_id AND is_deleted = 0
    ) >= 5
  ) THEN RAISE(ABORT, 'Max 5 annotators per image') END;
END;
"""

if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "data/library/db.sqlite3"
    con = sqlite3.connect(db)
    con.executescript(DDL)
    con.commit()
    con.close()
    print(f"Initialized {db}")
