from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError, ValidationError
from .rules import ENTITY, ID_PREFIX, STATES


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    primary_id INTEGER NOT NULL REFERENCES items(id),
                    secondary_id INTEGER NOT NULL REFERENCES items(id),
                    primary_version INTEGER NOT NULL,
                    secondary_version INTEGER NOT NULL,
                    moved_records TEXT NOT NULL DEFAULT '[]',
                    conflict_records TEXT NOT NULL DEFAULT '[]',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(primary_id, secondary_id),
                    UNIQUE(secondary_id)
                );
            """)

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def _get_merge_locked(self, primary_id: int, secondary_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM merges WHERE primary_id=? AND secondary_id=?",
            (primary_id, secondary_id),
        ).fetchone()
        if row is None:
            return None
        return self._merge_row(row)

    def _is_secondary_locked(self, item_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM merges WHERE secondary_id=? LIMIT 1", (item_id,)
        ).fetchone()
        return row is not None

    def _list_records_locked(self, item_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _merge_row(row: sqlite3.Row) -> Dict[str, Any]:
        d = dict(row)
        d["moved_records"] = json.loads(d["moved_records"])
        d["conflict_records"] = json.loads(d["conflict_records"])
        return d

    @staticmethod
    def _conflict_entry(record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "record_id": record["id"],
            "kind": record["kind"],
            "external_ref": record["external_ref"],
            "detail": record["detail"],
            "status": record["status"],
            "created_by": record["created_by"],
            "created_at": record["created_at"],
            "reason": "external_ref_duplicate",
        }

    @staticmethod
    def _dedupe_records(primary_records: List[Dict[str, Any]],
                        secondary_records: List[Dict[str, Any]]
                        ) -> tuple:
        primary_refs = {
            r["external_ref"]: r for r in primary_records
            if r["external_ref"] is not None
        }
        moved: List[Dict[str, Any]] = []
        conflicts: List[Dict[str, Any]] = []
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for r in secondary_records:
            if r["external_ref"] is None:
                moved.append(r)
            else:
                groups.setdefault(r["external_ref"], []).append(r)
        for ref, recs in groups.items():
            if ref in primary_refs:
                for r in recs:
                    conflicts.append(Repository._conflict_entry(r))
                continue
            ordered = sorted(recs, key=lambda r: (r["created_at"], r["id"]))
            moved.append(ordered[0])
            for r in ordered[1:]:
                conflicts.append(Repository._conflict_entry(r))
        return moved, conflicts

    def _append_audit_in_tx(self, action: str, entity_type: str, entity_id: int,
                            actor: str, detail: dict) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        cur = self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"]),
        )
        event["id"] = int(cur.lastrowid)
        return event

    def merge_items(self, primary_id: int, secondary_id: int,
                    primary_version: int, secondary_version: int, actor: str
                    ) -> tuple:
        now = utc_now()
        with self._lock, self.conn:
            existing = self._get_merge_locked(primary_id, secondary_id)
            if existing is not None:
                return existing, False
            primary = self.get_item(primary_id)
            secondary = self.get_item(secondary_id)
            if primary_id == secondary_id:
                raise ValidationError("主事件和从属事件不能相同")
            if self._is_secondary_locked(primary_id):
                raise ConflictError("主事件已被归并，不能作为主事件")
            if self._is_secondary_locked(secondary_id):
                raise ConflictError("从属事件已被归并，不能重复归并")
            if primary["version"] != primary_version or secondary["version"] != secondary_version:
                raise ConflictError("版本冲突，请刷新后重试")
            primary_records = self._list_records_locked(primary_id)
            secondary_records = self._list_records_locked(secondary_id)
            moved, conflicts = self._dedupe_records(primary_records, secondary_records)
            moved_ids = [r["id"] for r in moved]
            if moved_ids:
                placeholders = ",".join("?" for _ in moved_ids)
                self.conn.execute(
                    f"UPDATE records SET item_id=? WHERE id IN ({placeholders})",
                    [primary_id] + moved_ids,
                )
            self.conn.execute(
                "UPDATE items SET version=version+1, updated_at=? WHERE id=?",
                (now, primary_id),
            )
            self.conn.execute(
                "UPDATE items SET version=version+1, updated_at=? WHERE id=?",
                (now, secondary_id),
            )
            moved_json = json.dumps(moved_ids)
            conflicts_json = json.dumps(conflicts, ensure_ascii=False)
            try:
                self.conn.execute(
                    """INSERT INTO merges(primary_id, secondary_id, primary_version,
                       secondary_version, moved_records, conflict_records, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (primary_id, secondary_id, primary_version, secondary_version,
                     moved_json, conflicts_json, actor, now),
                )
            except sqlite3.IntegrityError:
                return self._get_merge_locked(primary_id, secondary_id), False
            self._append_audit_in_tx("merge", ENTITY, primary_id, actor, {
                "secondary_id": secondary_id,
                "moved_records": moved_ids,
                "conflict_records": conflicts,
                "primary_version": primary_version,
                "secondary_version": secondary_version,
            })
            merge = self._get_merge_locked(primary_id, secondary_id)
        return merge, True

    def get_merge(self, primary_id: int, secondary_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM merges WHERE primary_id=? AND secondary_id=?",
                (primary_id, secondary_id),
            ).fetchone()
        if row is None:
            return None
        return self._merge_row(row)

    def list_merges(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM merges ORDER BY id").fetchall()
        return [self._merge_row(row) for row in rows]

    def merge_info_map(self, item_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not item_ids:
            return {}
        with self._lock:
            placeholders = ",".join("?" for _ in item_ids)
            rows = self.conn.execute(
                f"""SELECT primary_id, secondary_id FROM merges
                    WHERE primary_id IN ({placeholders}) OR secondary_id IN ({placeholders})""",
                list(item_ids) + list(item_ids),
            ).fetchall()
        info: Dict[int, Dict[str, Any]] = {i: {} for i in item_ids}
        for row in rows:
            pid, sid = row["primary_id"], row["secondary_id"]
            if pid in info:
                info[pid].setdefault("merged_from", []).append(sid)
            if sid in info:
                info[sid]["merged_into"] = pid
        return info

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
