from pathlib import Path

from datenest.importer import import_files


def test_dedup(tmp_path: Path):
    f1 = tmp_path / "a.txt"
    f1.write_text("hello")
    f2 = tmp_path / "b.txt"
    f2.write_text("hello")  # 同内容

    db = tmp_path / "t.db"
    s1 = import_files(db, [f1], tags=["t1"])
    s2 = import_files(db, [f2], tags=["t2"])

    assert s1["inserted"] == 1 and s1["duplicates"] == 0
    assert s2["inserted"] == 0 and s2["duplicates"] == 1
