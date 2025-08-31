import csv
import datetime
import hashlib
import json
import os
import runpy
import shutil
import sqlite3
import sys
import zipfile
from pathlib import Path

from PySide6.QtCore import QSize, QStringListModel, Qt, QUrl
from PySide6.QtGui import (
    QColor,
    QDesktopServices,
    QDragEnterEvent,
    QDropEvent,
    QIcon,
    QImageReader,
    QKeySequence,
    QPainter,
    QPixmap,
    QShortcut,
)
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QCompleter,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtWidgets import (
    QListWidget as TagList,
)


def runtime_root() -> Path:
    # PyInstaller exe のあるフォルダ（開発時はカレント）
    return Path(sys.executable).parent if getattr(sys, "frozen", False) else Path.cwd()


def ensure_runtime_db() -> bool:
    """起動前に必ず実行。成功なら True。失敗時のみ False を返す。"""
    root = runtime_root()
    db_path = root / "data" / "library" / "db.sqlite3"
    if db_path.exists():
        return True
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # exe に同梱した scripts.init_db を __main__ として実行
    cwd = os.getcwd()
    try:
        os.chdir(root)  # scripts.init_db の既定出力(data/library)を合わせる
        runpy.run_module("scripts.init_db", run_name="__main__")
    except Exception:
        return False
    finally:
        os.chdir(cwd)
    return db_path.exists()


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}
DB_PATH = "data/library/db.sqlite3"
LIB_ROOT = "data/library"
THUMB_ROOT = Path(LIB_ROOT) / ".thumbnails" / "256"

# ---- カテゴリと色対応
CATEGORY_COLORS: dict[str, QColor] = {
    "": QColor(180, 180, 180),  # 未分類
    "condition": QColor(239, 123, 80),  # 条件
    "result": QColor(80, 160, 255),  # 結果/所見
    "quality": QColor(72, 199, 116),  # 良否
    "date": QColor(190, 160, 255),  # 日付
    "method": QColor(255, 208, 80),  # 手法
    "people": QColor(255, 120, 200),  # 人
}

QUALITY_LABELS = [("good", "良"), ("review", "保留"), ("bad", "悪")]


# アプリ起動前に呼ぶ
ensure_runtime_db()


def color_dot_icon(color: QColor, size: int = 14) -> QIcon:
    pm = QPixmap(size, size)
    pm.fill(Qt.transparent)
    p = QPainter(pm)
    p.setRenderHint(QPainter.Antialiasing, True)
    p.setBrush(color)
    p.setPen(Qt.NoPen)
    p.drawEllipse(1, 1, size - 2, size - 2)
    p.end()
    return QIcon(pm)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---- export/import helpers ----
def _safe_ext(name: str) -> str:
    ext = Path(name).suffix.lower()
    return ext if ext else ".bin"


def _ingest_target_dir(root: Path, sub: str) -> Path:
    d = root / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


class DB:
    def __init__(self, db_path: str):
        self.con = sqlite3.connect(db_path)
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA foreign_keys=ON")

    # --- users ---
    def ensure_user(self, username: str, display_name: str | None = None) -> int:
        r = self.con.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
        if r:
            return r["id"]
        cur = self.con.execute(
            "INSERT INTO users(username, display_name) VALUES(?,?)",
            (username, display_name),
        )
        self.con.commit()
        return cur.lastrowid

    # --- images ---
    def upsert_image(self, rel_path: str, sha256: str, created_at: str | None) -> int:
        r = self.con.execute("SELECT id FROM images WHERE sha256=?", (sha256,)).fetchone()
        if r:
            self.con.execute("UPDATE images SET rel_path=? WHERE id=?", (rel_path, r["id"]))
            self.con.commit()
            return r["id"]
        cur = self.con.execute(
            "INSERT INTO images(rel_path, sha256, created_at) VALUES(?,?,?)",
            (rel_path, sha256, created_at),
        )
        self.con.commit()
        return cur.lastrowid

    # --- tags ---
    def upsert_tag(self, name: str, category: str = "", description: str | None = None) -> int:
        r = self.con.execute(
            "SELECT id FROM tags WHERE name=? AND category=?", (name, category)
        ).fetchone()
        if r:
            return r["id"]
        cur = self.con.execute(
            "INSERT INTO tags(name, category, description) VALUES(?,?,?)",
            (name, category, description),
        )
        self.con.commit()
        return cur.lastrowid

    def all_tag_names(self) -> list[str]:
        return [r["name"] for r in self.con.execute("SELECT DISTINCT name FROM tags ORDER BY name")]

    # --- annotations ---
    def get_tags_for_image(self, image_id: int) -> list[sqlite3.Row]:
        q = """
        SELECT t.name, t.category, a.user_id, a.is_deleted, a.created_at, u.username
          FROM annotations a
          JOIN tags t ON t.id=a.tag_id
          JOIN users u ON u.id=a.user_id
         WHERE a.image_id=? AND a.is_deleted=0
         ORDER BY t.category, t.name
        """
        return list(self.con.execute(q, (image_id,)))

    def add_tag_for_user(self, image_id: int, tag_name: str, user_id: int, category: str = ""):
        tag_id = self.upsert_tag(tag_name, category)
        r = self.con.execute(
            "SELECT id, is_deleted FROM annotations WHERE image_id=? AND tag_id=? AND user_id=?",
            (image_id, tag_id, user_id),
        ).fetchone()
        now = datetime.datetime.now().isoformat(timespec="seconds")
        if r:
            if r["is_deleted"] != 0:
                self.con.execute(
                    "UPDATE annotations SET is_deleted=0, created_at=? WHERE id=?",
                    (now, r["id"]),
                )
        else:
            self.con.execute(
                "INSERT INTO annotations(image_id, tag_id, user_id, created_at, is_deleted) VALUES(?,?,?,?,0)",
                (image_id, tag_id, user_id, now),
            )
        self.con.commit()

    def remove_tag_for_user(self, image_id: int, tag_name: str, user_id: int, category: str = ""):
        r = self.con.execute(
            "SELECT id FROM tags WHERE name=? AND category=?", (tag_name, category)
        ).fetchone()
        if not r:
            return
        tag_id = r["id"]
        now = datetime.datetime.now().isoformat(timespec="seconds")
        self.con.execute(
            "UPDATE annotations SET is_deleted=1, created_at=? WHERE image_id=? AND tag_id=? AND user_id=?",
            (now, image_id, tag_id, user_id),
        )
        self.con.commit()

    def active_tags_map(self) -> dict[int, set[str]]:
        m: dict[int, set[str]] = {}
        q = """
        SELECT a.image_id, t.name
          FROM annotations a JOIN tags t ON t.id=a.tag_id
         WHERE a.is_deleted=0
        """
        for r in self.con.execute(q):
            m.setdefault(r["image_id"], set()).add(r["name"])
        return m

    # --- attachments ---
    def upsert_attachment(
        self,
        image_id: int,
        kind: str,
        rel_path: str,
        sha256: str,
        created_at: str | None,
    ):
        r = self.con.execute("SELECT id FROM attachments WHERE sha256=?", (sha256,)).fetchone()
        if r:
            return r["id"]
        cur = self.con.execute(
            "INSERT INTO attachments(image_id, kind, rel_path, sha256, created_at) VALUES(?,?,?,?,?)",
            (image_id, kind, rel_path, sha256, created_at),
        )
        self.con.commit()
        return cur.lastrowid

    def get_csv_attachments(self, image_id: int):
        q = "SELECT id, rel_path FROM attachments WHERE image_id=? AND kind='csv' ORDER BY rel_path"
        return list(self.con.execute(q, (image_id,)))

    # --- quality votes ---
    def upsert_quality(
        self,
        image_id: int,
        user_id: int,
        label: str,
        score: float | None = None,
        when: str | None = None,
    ):
        when = when or datetime.datetime.now().isoformat(timespec="seconds")
        self.con.execute(
            "INSERT OR REPLACE INTO quality_votes(image_id,user_id,label,score,created_at) VALUES(?,?,?,?,?)",
            (image_id, user_id, label, score, when),
        )
        self.con.commit()

    def get_quality_for_image(self, image_id: int) -> list[sqlite3.Row]:
        q = """
        SELECT q.label, q.score, q.created_at, u.username
          FROM quality_votes q JOIN users u ON u.id=q.user_id
         WHERE q.image_id=? ORDER BY q.created_at DESC
        """
        return list(self.con.execute(q, (image_id,)))


class MainWindow(QMainWindow):
    def __init__(self, root=LIB_ROOT, thumb_size=256, db_path=DB_PATH):
        super().__init__()
        self.setWindowTitle("DateNest")
        self.root = Path(root)
        self.thumb_size = thumb_size
        THUMB_ROOT.mkdir(parents=True, exist_ok=True)

        self.db = DB(db_path)
        self.username = os.getenv("USERNAME") or os.getenv("USER") or "local"
        self.user_id = self.db.ensure_user(self.username, self.username)

        # ===== Left: gallery =====
        self.search = QLineEdit(
            placeholderText="検索: 例/#tag cat:result user:kager label:good has:csv date:2025-06-01..2025-06-30"
        )
        self.list = QListWidget()
        self.list.setViewMode(QListView.IconMode)
        self.list.setResizeMode(QListWidget.Adjust)
        self.list.setUniformItemSizes(True)
        self.list.setMovement(QListWidget.Static)
        self.list.setIconSize(QSize(self.thumb_size, self.thumb_size))
        self.list.setGridSize(QSize(self.thumb_size + 20, self.thumb_size + 40))
        self.list.setSpacing(8)
        self.list.setSelectionMode(QAbstractItemView.ExtendedSelection)

        left = QWidget()
        left_lay = QVBoxLayout(left)
        left_lay.addWidget(self.search)
        left_lay.addWidget(self.list)

        # ===== Right: details & tags & CSV（Tabs） =====
        self.path_label = QLabel("—")
        self.size_label = QLabel("—")
        self.time_label = QLabel("—")
        self.user_label = QLabel(f"User: {self.username}")

        meta_form = QFormLayout()
        meta_form.addRow("Path:", self.path_label)
        meta_form.addRow("Size:", self.size_label)
        meta_form.addRow("Time:", self.time_label)
        meta_form.addRow("", self.user_label)

        self.tags_view = TagList()
        self.tags_view.setSelectionMode(QAbstractItemView.SingleSelection)

        self.tag_input = QLineEdit()
        self.category_combo = QComboBox()
        for label, key in [
            ("未分類", ""),
            ("条件", "condition"),
            ("結果", "result"),
            ("品質", "quality"),
            ("日付", "date"),
            ("手法", "method"),
            ("人", "people"),
        ]:
            self.category_combo.addItem(label, userData=key)
        self.btn_add = QPushButton("選択タイルにタグ追加（自分）")
        self.btn_del = QPushButton("選択タイルからタグ削除（自分）")
        tag_bar = QHBoxLayout()
        tag_bar.addWidget(self.tag_input, 2)
        tag_bar.addWidget(self.category_combo)
        tag_bar.addWidget(self.btn_add)
        tag_bar.addWidget(self.btn_del)

        # Quality
        self.quality_list = QListWidget()
        q_btns = QHBoxLayout()
        self.btn_q_good = QPushButton("1: 良")
        self.btn_q_review = QPushButton("2: 保留")
        self.btn_q_bad = QPushButton("3: 悪")
        for b in (self.btn_q_good, self.btn_q_review, self.btn_q_bad):
            q_btns.addWidget(b)

        # CSV
        self.csv_list = QListWidget()
        self.csv_table = QTableWidget()
        self.csv_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.csv_table.setAlternatingRowColors(True)
        self.btn_rescan = QPushButton("再スキャン (F5)")
        self.btn_attach = QPushButton("CSVを手動で添付")
        self.btn_export = QPushButton("選択をエクスポート")
        self.btn_import = QPushButton("アーカイブをインポート")

        # Tabs
        self.tabs = QTabWidget()
        details_w = QWidget()
        details_l = QVBoxLayout(details_w)
        details_l.addLayout(meta_form)
        details_l.addWidget(QLabel("Quality votes:"))
        details_l.addWidget(self.quality_list)
        details_l.addLayout(q_btns)
        details_l.addStretch(1)

        tags_w = QWidget()
        tags_l = QVBoxLayout(tags_w)
        tags_l.addWidget(QLabel("Tags (active):"))
        tags_l.addWidget(self.tags_view, 1)
        tags_l.addLayout(tag_bar)

        csv_w = QWidget()
        csv_l = QVBoxLayout(csv_w)
        csv_btns = QHBoxLayout()
        for b in (self.btn_rescan, self.btn_attach, self.btn_export, self.btn_import):
            csv_btns.addWidget(b)
        csv_l.addLayout(csv_btns)
        csv_l.addWidget(QLabel("CSV attachments:"))
        csv_l.addWidget(self.csv_list)
        csv_l.addWidget(QLabel("Preview (head):"))
        csv_l.addWidget(self.csv_table, 1)

        self.tabs.addTab(details_w, "Details")
        self.tabs.addTab(tags_w, "Tags")
        self.tabs.addTab(csv_w, "CSV")

        right = QWidget()
        right_lay = QVBoxLayout(right)
        right_lay.addWidget(self.tabs)
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([900, 500])
        cw = QWidget()
        main_lay = QVBoxLayout(cw)
        main_lay.addWidget(splitter)
        self.setCentralWidget(cw)
        self.resize(1320, 900)

        # events
        self.search.textChanged.connect(self.on_search)
        self.list.itemDoubleClicked.connect(self.open_item)
        self.list.itemSelectionChanged.connect(self.update_right_panel)
        self.btn_add.clicked.connect(self.add_tag_clicked)
        self.btn_del.clicked.connect(self.del_tag_clicked)
        # quality
        self.btn_q_good.clicked.connect(lambda: self.vote_quality("good"))
        self.btn_q_review.clicked.connect(lambda: self.vote_quality("review"))
        self.btn_q_bad.clicked.connect(lambda: self.vote_quality("bad"))
        QShortcut(QKeySequence("1"), self, activated=lambda: self.vote_quality("good"))
        QShortcut(QKeySequence("2"), self, activated=lambda: self.vote_quality("review"))
        QShortcut(QKeySequence("3"), self, activated=lambda: self.vote_quality("bad"))
        # CSV
        self.btn_rescan.clicked.connect(self._rescan)
        self.btn_attach.clicked.connect(self.attach_csv_manual)
        self.btn_export.clicked.connect(lambda: self.export_selection(True, True))
        self.btn_import.clicked.connect(self.import_archive)
        QShortcut(QKeySequence.Refresh, self, activated=self._rescan)  # F5
        self.csv_list.itemDoubleClicked.connect(self.open_csv_item)
        self.csv_list.currentItemChanged.connect(self.preview_csv)

        # D&D
        self.setAcceptDrops(True)

        # data
        self.all_items: list[tuple[str, QListWidgetItem]] = []
        self.image_tags: dict[int, set[str]] = {}
        # 検索用のインデックス
        self.map_tags_lower: dict[int, set[str]] = {}
        self.map_cats: dict[int, set[str]] = {}
        self.map_users: dict[int, set[str]] = {}
        self.map_labels: dict[int, set[str]] = {}
        self.map_has_csv: dict[int, bool] = {}
        self.map_created_at: dict[int, datetime.datetime] = {}

        self.reload_all()

        # 補完（タグ入力 & 検索）
        self.completer_model = QStringListModel(self.db.all_tag_names())
        self.tag_input.setCompleter(QCompleter(self.completer_model))
        all_tags = self.db.all_tag_names()
        all_files = [rel for rel, _ in self.all_items]
        ops = ["#", "cat:", "user:", "label:", "has:csv", "date:"]
        self.search_completer = QCompleter(ops + all_tags + ["#" + t for t in all_tags] + all_files)
        self.search_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.search.setCompleter(self.search_completer)

    # ===== D&D handlers =====
    def dragEnterEvent(self, e: QDragEnterEvent):
        if e.mimeData().hasUrls():
            urls = [u.toLocalFile() for u in e.mimeData().urls()]
            if any(Path(p).suffix.lower() == ".csv" for p in urls):
                e.acceptProposedAction()
                return
        e.ignore()

    def dropEvent(self, e: QDropEvent):
        urls = [u.toLocalFile() for u in e.mimeData().urls()]
        csvs = [Path(p) for p in urls if Path(p).suffix.lower() == ".csv"]
        if not csvs:
            e.ignore()
            return
        self.attach_csv_paths(csvs)
        e.acceptProposedAction()

    # ===== helpers =====
    def _rescan(self):
        self.reload_all()
        self.update_right_panel()
        # 検索候補の更新
        all_tags = self.db.all_tag_names()
        all_files = [rel for rel, _ in self.all_items]
        ops = ["#", "cat:", "user:", "label:", "has:csv", "date:"]
        self.search_completer.model().setStringList(
            ops + all_tags + ["#" + t for t in all_tags] + all_files
        )

    def attach_csv_paths(self, paths: list[Path]):
        items = self.current_selection()
        if not items:
            QMessageBox.information(
                self, "DateNest", "画像を選択してから CSV をドロップしてください。"
            )
            return
        for it in items:
            img_abs = Path(it.data(Qt.UserRole))
            img_id = it.data(Qt.UserRole + 1)
            img_dir = img_abs.parent
            for p in paths:
                try:
                    src = p
                    if not str(src).startswith(str(self.root)):
                        dst = img_dir / src.name
                        if dst.exists():
                            dst = img_dir / (dst.stem + " (copy)" + dst.suffix)
                        shutil.copy2(src, dst)
                        csv_abs = dst
                    else:
                        csv_abs = src
                    csha = sha256_of(csv_abs)
                    rel_csv = csv_abs.relative_to(self.root).as_posix()
                    cts = datetime.datetime.fromtimestamp(csv_abs.stat().st_mtime).isoformat(
                        timespec="seconds"
                    )
                    self.db.upsert_attachment(img_id, "csv", rel_csv, csha, cts)
                except Exception as ex:
                    QMessageBox.warning(self, "DateNest", f"CSV 添付に失敗: {p}{ex}")
        self.update_right_panel()

    # ===== quality vote =====
    def vote_quality(self, label: str):
        items = self.current_selection()
        if not items:
            QMessageBox.information(self, "DateNest", "画像を選択してください。")
            return
        if label not in {"good", "review", "bad"}:
            return
        for it in items:
            image_id = it.data(Qt.UserRole + 1)
            self.db.upsert_quality(image_id, self.user_id, label)
        self.update_right_panel()

    # ===== load & thumbnails =====
    def reload_all(self):
        self.list.clear()
        self.all_items.clear()
        paths = []
        for p in Path(self.root).rglob("*"):
            if p.suffix.lower() not in IMAGE_EXTS:
                continue
            if THUMB_ROOT in p.parents:
                continue
            paths.append(p)
        paths.sort(key=lambda p: p.name.lower())

        self.map_tags_lower.clear()
        self.map_cats.clear()
        self.map_users.clear()
        self.map_labels.clear()
        self.map_has_csv.clear()
        self.map_created_at.clear()

        for p in paths:
            rel = p.relative_to(self.root).as_posix()
            try:
                sha = sha256_of(p)
            except Exception as ex:
                print("hash failed:", p, ex)
                continue
            mtime = p.stat().st_mtime
            ts = datetime.datetime.fromtimestamp(mtime).isoformat(timespec="seconds")
            image_id = self.db.upsert_image(rel, sha, ts)

            # --- CSV 自動紐づけ（強化版） ---
            stem = p.stem
            parent = p.parent
            cands = list(parent.glob(stem + ".csv"))
            if not cands:
                only_csv = list(parent.glob("*.csv"))
                if len(only_csv) == 1:
                    cands = only_csv
            if not cands:
                cands = list(parent.glob(stem + "_*.csv"))
                if len(cands) > 1:
                    cands.sort(key=lambda x: abs(x.stat().st_mtime - mtime))
                    cands = [cands[0]]
            for cand in cands:
                try:
                    csha = sha256_of(cand)
                    rel_csv = cand.relative_to(self.root).as_posix()
                    cts = datetime.datetime.fromtimestamp(cand.stat().st_mtime).isoformat(
                        timespec="seconds"
                    )
                    self.db.upsert_attachment(image_id, "csv", rel_csv, csha, cts)
                except Exception as ex:
                    print("attach csv failed:", cand, ex)

            # UI item
            it = QListWidgetItem(rel)
            it.setToolTip(rel)
            it.setIcon(QIcon(self.thumbnail_for(p, sha)))
            it.setData(Qt.UserRole, str(p))
            it.setData(Qt.UserRole + 1, image_id)
            self.list.addItem(it)
            self.all_items.append((rel, it))

            # 検索インデックス
            tag_rows = self.db.get_tags_for_image(image_id)
            self.map_tags_lower[image_id] = {r["name"].lower() for r in tag_rows}
            self.map_cats[image_id] = {(r["category"] or "").lower() for r in tag_rows}
            self.map_users[image_id] = {r["username"].lower() for r in tag_rows}
            self.map_labels[image_id] = {
                r["label"].lower() for r in self.db.get_quality_for_image(image_id)
            }
            self.map_has_csv[image_id] = len(self.db.get_csv_attachments(image_id)) > 0
            self.map_created_at[image_id] = datetime.datetime.fromtimestamp(mtime)

        self.image_tags = self.db.active_tags_map()

    def thumbnail_for(self, path: Path, sha: str) -> QPixmap:
        THUMB_ROOT.mkdir(parents=True, exist_ok=True)
        cache = THUMB_ROOT / f"{sha}.jpg"
        src_mtime = int(path.stat().st_mtime)
        if cache.exists():
            try:
                if int(cache.stat().st_mtime) >= src_mtime:
                    pm = QPixmap(str(cache))
                    if not pm.isNull():
                        return pm
            except Exception:
                pass
        reader = QImageReader(str(path))
        reader.setAutoTransform(True)
        img = reader.read()
        if img.isNull():
            pm = QPixmap(self.thumb_size, self.thumb_size)
            pm.fill(Qt.darkGray)
            return pm
        pm = QPixmap.fromImage(img)
        scaled = pm.scaled(
            self.thumb_size,
            self.thumb_size,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        canvas = QPixmap(self.thumb_size, self.thumb_size)
        canvas.fill(Qt.darkGray)
        x = (self.thumb_size - scaled.width()) // 2
        y = (self.thumb_size - scaled.height()) // 2
        painter = QPainter(canvas)
        painter.setRenderHint(QPainter.SmoothPixmapTransform, True)
        painter.drawPixmap(x, y, scaled)
        painter.end()
        try:
            canvas.save(str(cache), "JPG", quality=85)
            os.utime(cache, (src_mtime, src_mtime))
        except Exception:
            pass
        return canvas

    # ===== search =====
    def _parse_date(self, token: str):
        # token like 'date:YYYY-MM-DD..YYYY-MM-DD' or 'date>=YYYY-MM-DD' or 'date<=YYYY-MM-DD' or 'date:YYYY-MM-DD'
        v = token[5:].strip()
        try:
            if ".." in v:
                a, b = v.split("..", 1)
                d1 = datetime.datetime.fromisoformat(a.strip()) if a.strip() else None
                d2 = datetime.datetime.fromisoformat(b.strip()) if b.strip() else None
                return (d1, d2)
            if v.startswith(">="):
                return (datetime.datetime.fromisoformat(v[2:].strip()), None)
            if v.startswith("<="):
                return (None, datetime.datetime.fromisoformat(v[2:].strip()))
            # exact day -> day range
            d = datetime.datetime.fromisoformat(v)
            return (d, d + datetime.timedelta(days=1))
        except Exception:
            return (None, None)

    def on_search(self, text: str):
        q = (text or "").strip()
        # Build filters
        tokens = [t for t in q.split() if t]
        tag_subs = []
        name_subs = []
        cats = []
        users = []
        labels = []
        has_csv = None
        date_range = (None, None)
        for t in tokens:
            tl = t.lower()
            if tl.startswith("#") and len(tl) > 1:
                tag_subs.append(tl[1:])
            elif tl.startswith("cat:"):
                cats.append(tl[4:])
            elif tl.startswith("user:"):
                users.append(tl[5:])
            elif tl.startswith("label:"):
                labels.append(tl[6:])
            elif tl == "has:csv":
                has_csv = True
            elif tl.startswith("date:") or tl.startswith("date>=") or tl.startswith("date<="):
                date_range = self._parse_date(
                    "date:" + tl.split(":", 1)[1] if tl.startswith("date:") else tl
                )
            else:
                name_subs.append(tl)

        for rel, item in self.all_items:
            img_id = item.data(Qt.UserRole + 1)
            ok = True
            # filename
            for s in name_subs:
                if s not in rel.lower():
                    ok = False
                    break
            if not ok:
                item.setHidden(True)
                continue
            # tag substrings
            if tag_subs:
                tlset = self.map_tags_lower.get(img_id, set())
                if not all(any(s in t for t in tlset) for s in tag_subs):
                    item.setHidden(True)
                    continue
            # category
            if cats:
                if not set(cats).issubset(self.map_cats.get(img_id, set())):
                    item.setHidden(True)
                    continue
            # users
            if users:
                if not set(users).issubset(self.map_users.get(img_id, set())):
                    item.setHidden(True)
                    continue
            # labels
            if labels:
                if not set(labels).issubset(self.map_labels.get(img_id, set())):
                    item.setHidden(True)
                    continue
            # has csv
            if has_csv is True and not self.map_has_csv.get(img_id, False):
                item.setHidden(True)
                continue
            # date range
            d1, d2 = date_range
            if d1 or d2:
                ts = self.map_created_at.get(img_id)
                if not ts or (d1 and ts < d1) or (d2 and ts >= d2):
                    item.setHidden(True)
                    continue
            item.setHidden(False)

    # ===== open =====
    def open_item(self, item: QListWidgetItem):
        path = item.data(Qt.UserRole)
        if path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def current_selection(self) -> list[QListWidgetItem]:
        return self.list.selectedItems()

    # ===== right panel update =====
    def update_right_panel(self):
        items = self.current_selection()
        if not items:
            self.path_label.setText("—")
            self.size_label.setText("—")
            self.time_label.setText("—")
            self.tags_view.clear()
            self.csv_list.clear()
            self.quality_list.clear()
            self.csv_table.clear()
            self.csv_table.setRowCount(0)
            self.csv_table.setColumnCount(0)
            return
        it = items[-1]
        abs_path = Path(it.data(Qt.UserRole))
        image_id = it.data(Qt.UserRole + 1)
        try:
            st = abs_path.stat()
            size_mb = st.st_size / (1024 * 1024)
            ts = datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except FileNotFoundError:
            size_mb, ts = 0, "N/A"
        self.path_label.setText(str(abs_path))
        self.size_label.setText(f"{size_mb:.2f} MB")
        self.time_label.setText(ts)
        # タグ
        self.tags_view.clear()
        for row in self.db.get_tags_for_image(image_id):
            li = QListWidgetItem(f"{row['name']}  (by {row['username']})")
            cat = row["category"] or ""
            col = CATEGORY_COLORS.get(cat, CATEGORY_COLORS[""])
            li.setIcon(color_dot_icon(col))
            li.setData(Qt.UserRole, cat)
            self.tags_view.addItem(li)
        # 品質投票
        self.quality_list.clear()
        for q in self.db.get_quality_for_image(image_id):
            jp = {"good": "良", "review": "保留", "bad": "悪"}.get(q["label"], q["label"])
            self.quality_list.addItem(f"{jp}  (by {q['username']})  {q['created_at']}")
        # CSV
        self.csv_list.clear()
        for row in self.db.get_csv_attachments(image_id):
            li = QListWidgetItem(row["rel_path"])
            li.setData(Qt.UserRole, str(Path(self.root) / row["rel_path"]))
            self.csv_list.addItem(li)
        if self.csv_list.count() > 0:
            self.csv_list.setCurrentRow(0)
        else:
            self.csv_table.clear()
            self.csv_table.setRowCount(0)
            self.csv_table.setColumnCount(0)

    # ===== tag ops =====
    def add_tag_clicked(self):
        items = self.current_selection()
        if not items:
            QMessageBox.information(self, "DateNest", "画像を選択してください。")
            return
        tag = (self.tag_input.text() or "").strip()
        if not tag:
            QMessageBox.information(self, "DateNest", "タグ名を入力してください。")
            return
        cat = self.category_combo.currentData() or ""
        for it in items:
            img_id = it.data(Qt.UserRole + 1)
            try:
                self.db.add_tag_for_user(img_id, tag, self.user_id, category=cat)
                self.image_tags.setdefault(img_id, set()).add(tag)
            except sqlite3.IntegrityError as e:
                QMessageBox.warning(self, "DateNest", f"タグ追加に失敗: {e}")
        self.completer_model.setStringList(self.db.all_tag_names())
        self.tag_input.clear()
        self.update_right_panel()
        if (self.search.text() or "").startswith("#"):
            self.on_search(self.search.text())

    def del_tag_clicked(self):
        items = self.current_selection()
        if not items:
            QMessageBox.information(self, "DateNest", "画像を選択してください。")
            return
        sel = self.tags_view.currentItem()
        if not sel:
            QMessageBox.information(self, "DateNest", "右のタグ一覧から削除対象を選んでください。")
            return
        tag_name = sel.text().split("  (by")[0].strip()
        cat = sel.data(Qt.UserRole) or ""
        for it in items:
            img_id = it.data(Qt.UserRole + 1)
            self.db.remove_tag_for_user(img_id, tag_name, self.user_id, category=cat)
        self.update_right_panel()
        if (self.search.text() or "").startswith("#"):
            self.on_search(self.search.text())

    # ===== CSV handlers =====
    def open_csv_item(self, item: QListWidgetItem):
        path = item.data(Qt.UserRole)
        if path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def preview_csv(self, current: QListWidgetItem, _prev: QListWidgetItem):
        if not current:
            self.csv_table.clear()
            self.csv_table.setRowCount(0)
            self.csv_table.setColumnCount(0)
            return
        path = Path(current.data(Qt.UserRole))
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                rows = []
                for i, row in enumerate(reader):
                    rows.append(row)
                    if i >= 49:
                        break
        except Exception as ex:
            QMessageBox.warning(self, "DateNest", f"CSV 読み込み失敗: {ex}")
            return
        if not rows:
            self.csv_table.clear()
            self.csv_table.setRowCount(0)
            self.csv_table.setColumnCount(0)
            return
        header = rows[0]
        data = rows[1:] if all((h or "").strip() for h in header) else rows
        if data is rows:
            header = [f"col{i + 1}" for i in range(len(rows[0]))]
        self.csv_table.clear()
        self.csv_table.setColumnCount(len(header))
        self.csv_table.setHorizontalHeaderLabels(header)
        self.csv_table.setRowCount(len(data))
        for r, row in enumerate(data):
            for c, val in enumerate(row):
                self.csv_table.setItem(r, c, QTableWidgetItem(str(val)))
        self.csv_table.resizeColumnsToContents()

    def attach_csv_manual(self):
        items = self.current_selection()
        if not items:
            QMessageBox.information(self, "DateNest", "画像を選択してください。")
            return
        paths, _ = QFileDialog.getOpenFileNames(
            self, "CSV を選択", str(self.root), "CSV Files (*.csv)"
        )
        if not paths:
            return
        self.attach_csv_paths([Path(p) for p in paths])

    # ===== Export =====
    def export_selection(self, include_images=True, include_attachments=True):
        items = self.current_selection()
        if not items:
            QMessageBox.information(self, "DateNest", "画像を選択してください。")
            return

        image_rows = []
        seen_imgs, seen_atts = set(), set()
        for it in items:
            # img_abs = Path(it.data(Qt.UserRole))
            img_id = it.data(Qt.UserRole + 1)
            r = self.db.con.execute(
                "SELECT rel_path, sha256, created_at FROM images WHERE id=?", (img_id,)
            ).fetchone()
            if not r:
                continue
            row = {
                "sha256": r["sha256"],
                "rel_path": r["rel_path"],
                "created_at": r["created_at"],
                "attachments": [],
                "annotations": [],
                "quality": [],
            }
            for a in self.db.get_tags_for_image(img_id):
                row["annotations"].append(
                    {
                        "username": a["username"],
                        "tag": a["name"],
                        "category": a["category"],
                        "created_at": a["created_at"],
                    }
                )
            for q in self.db.con.execute(
                "SELECT label, score, created_at, u.username FROM quality_votes q JOIN users u ON u.id=q.user_id WHERE image_id=?",
                (img_id,),
            ):
                row["quality"].append(
                    {
                        "username": q["username"],
                        "label": q["label"],
                        "score": q["score"],
                        "created_at": q["created_at"],
                    }
                )
            if include_attachments:
                for att in self.db.con.execute(
                    "SELECT kind, rel_path, sha256 FROM attachments WHERE image_id=?",
                    (img_id,),
                ):
                    row["attachments"].append(
                        {
                            "kind": att["kind"],
                            "sha256": att["sha256"],
                            "ext": _safe_ext(att["rel_path"]),
                        }
                    )
                    seen_atts.add(att["sha256"])
            image_rows.append(row)
            seen_imgs.add(r["sha256"])

        manifest = {
            "version": 1,
            "exported_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "exporter": {
                "username": self.username,
                "tool": "DateNest",
                "tool_version": "1.0.0b",
            },
            "options": {
                "include_images": include_images,
                "include_attachments": include_attachments,
            },
            "users": [{"username": self.username, "display_name": self.username}],
            "tags": [
                {
                    "name": r["name"],
                    "category": r["category"],
                    "description": r["description"],
                }
                for r in self.db.con.execute("SELECT name,category,description FROM tags")
            ],
            "images": image_rows,
        }

        path, _ = QFileDialog.getSaveFileName(
            self, "エクスポート先", str(self.root / "DateNestExport.zip"), "Zip (*.zip)"
        )
        if not path:
            return
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
            if include_images:
                for sha in seen_imgs:
                    r = self.db.con.execute(
                        "SELECT rel_path FROM images WHERE sha256=?", (sha,)
                    ).fetchone()
                    if not r:
                        continue
                    src = self.root / r["rel_path"]
                    if src.exists():
                        z.write(src, arcname=f"images/{sha}{_safe_ext(src.name)}")
            if include_attachments:
                for sha in seen_atts:
                    r = self.db.con.execute(
                        "SELECT rel_path FROM attachments WHERE sha256=?", (sha,)
                    ).fetchone()
                    if not r:
                        continue
                    src = self.root / r["rel_path"]
                    if src.exists():
                        z.write(src, arcname=f"attachments/{sha}{_safe_ext(src.name)}")
        QMessageBox.information(self, "DateNest", f"エクスポート完了{path}")

    # ===== Import =====
    def import_archive(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "アーカイブをインポート", str(self.root), "Zip (*.zip)"
        )
        if not path:
            return
        with zipfile.ZipFile(path, "r") as z:
            try:
                manifest = json.loads(z.read("manifest.json").decode("utf-8"))
            except Exception as e:
                QMessageBox.critical(self, "DateNest", f"manifest.json が読めません: {e}")
                return

            for u in manifest.get("users", []):
                self.db.ensure_user(u.get("username"), u.get("display_name"))
            for t in manifest.get("tags", []):
                self.db.upsert_tag(t.get("name", ""), t.get("category", ""), t.get("description"))

            for im in manifest.get("images", []):
                sha = im["sha256"]
                r = self.db.con.execute("SELECT id FROM images WHERE sha256=?", (sha,)).fetchone()
                if r:
                    img_id = r["id"]
                else:
                    member = f"images/{sha}{_safe_ext(im.get('rel_path', ''))}"
                    if member in z.namelist():
                        dst_dir = _ingest_target_dir(self.root, "imported")
                        dst = dst_dir / Path(member).name
                        with z.open(member) as src, open(dst, "wb") as out:
                            shutil.copyfileobj(src, out)
                        rel = dst.relative_to(self.root).as_posix()
                        ts = im.get("created_at")
                        img_id = self.db.upsert_image(rel, sha, ts)
                    else:
                        continue

                for att in im.get("attachments", []):
                    a_sha, ext = att["sha256"], att.get("ext", ".bin")
                    r2 = self.db.con.execute(
                        "SELECT id FROM attachments WHERE sha256=?", (a_sha,)
                    ).fetchone()
                    if not r2 and f"attachments/{a_sha}{ext}" in z.namelist():
                        dst_dir = _ingest_target_dir(self.root, "attachments_imported")
                        dst = dst_dir / f"{a_sha}{ext}"
                        with z.open(f"attachments/{a_sha}{ext}") as src, open(dst, "wb") as out:
                            shutil.copyfileobj(src, out)
                        rel = dst.relative_to(self.root).as_posix()
                        self.db.upsert_attachment(img_id, att.get("kind", "csv"), rel, a_sha, None)

                for an in im.get("annotations", []):
                    uid = self.db.ensure_user(an.get("username"))
                    self.db.add_tag_for_user(
                        img_id, an.get("tag", ""), uid, category=an.get("category", "")
                    )
                for q in im.get("quality", []):
                    uid = self.db.ensure_user(q.get("username"))
                    self.db.upsert_quality(
                        img_id,
                        uid,
                        q.get("label", "review"),
                        q.get("score"),
                        q.get("created_at"),
                    )

        self.reload_all()
        self.update_right_panel()
        QMessageBox.information(self, "DateNest", "インポート完了")


if __name__ == "__main__":
    import traceback

    app = QApplication(sys.argv)

    def excepthook(exctype, value, tb):
        msg = "".join(traceback.format_exception(exctype, value, tb))
        print(msg)
        QMessageBox.critical(None, "DateNest - Unhandled Error", msg)
        sys.exit(1)

    sys.excepthook = excepthook

    # ★ DBを自動生成（失敗時のみメッセージを出して終了）
    if not ensure_runtime_db():
        QMessageBox.critical(
            None,
            "DateNest",
            "DB 初期化に失敗しました。実行フォルダへの書き込み権限を確認してください。",
        )
        sys.exit(1)

    # 必要なら資材フォルダを用意
    Path(LIB_ROOT).mkdir(parents=True, exist_ok=True)

    w = MainWindow(root=LIB_ROOT, db_path=DB_PATH)
    w.show()
    sys.exit(app.exec())
