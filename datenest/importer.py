from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Sequence
from pathlib import Path

from .db import connect, get_or_create_tag_id


def sha256sum(path: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()


def import_files(
    db_path: Path,
    files: Iterable[Path],
    tags: Sequence[str] = (),
) -> dict:
    """
    Returns stats: {"inserted": n_new, "duplicates": n_dup, "tag_links": n_links}
    """
    conn = connect(db_path)
    inserted = duplicates = links = 0
    try:
        with conn:  # transaction
            tag_ids = [get_or_create_tag_id(conn, t.strip()) for t in tags if t.strip()]
            for p in files:
                if not p.exists() or not p.is_file():
                    continue
                digest = sha256sum(p)
                size = os.path.getsize(p)

                # try insert file (unique by sha256)
                cur = conn.execute(
                    "INSERT OR IGNORE INTO files(path, sha256, size) VALUES (?, ?, ?)",
                    (str(p), digest, size),
                )
                if cur.rowcount == 1:
                    inserted += 1
                    cur = conn.execute("SELECT id FROM files WHERE sha256 = ?", (digest,))
                    file_id = cur.fetchone()[0]
                else:
                    duplicates += 1
                    cur = conn.execute("SELECT id FROM files WHERE sha256 = ?", (digest,))
                    file_id = cur.fetchone()[0]

                # link tags (UNIQUE(file_id, tag_id) prevents dup links)
                for tid in tag_ids:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO file_tags(file_id, tag_id) VALUES (?, ?)",
                        (file_id, tid),
                    )
                    links += cur.rowcount
    finally:
        conn.close()
    return {"inserted": inserted, "duplicates": duplicates, "tag_links": links}
