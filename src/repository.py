from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, STATES

MERGE_REQUEST_TABLE = "item_merges"


class MergeAlreadyExists(Exception):
    """同一组（主事件, 从属事件）的归并已经成立；携带当前关系。"""

    def __init__(self, merge: Optional[Dict[str, Any]]):
        super().__init__("该组事件已经归并")
        self.merge = merge


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
                    updated_at TEXT NOT NULL,
                    merged_into_item_id INTEGER REFERENCES items(id)
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
                    origin_item_id INTEGER NOT NULL,
                    conflict_status TEXT NOT NULL DEFAULT 'active'
                        CHECK(conflict_status IN ('active','duplicate_conflict')),
                    conflict_of_record_id INTEGER REFERENCES records(id),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS {MERGE_REQUEST_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merge_key TEXT NOT NULL UNIQUE,
                    primary_item_id INTEGER NOT NULL REFERENCES items(id),
                    subordinate_item_id INTEGER NOT NULL REFERENCES items(id),
                    primary_expected_version INTEGER NOT NULL,
                    subordinate_expected_version INTEGER NOT NULL,
                    primary_result_version INTEGER NOT NULL,
                    subordinate_result_version INTEGER NOT NULL,
                    moved_record_ids TEXT NOT NULL,
                    conflicts TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_item_merges_pair
                    ON item_merges(primary_item_id, subordinate_item_id);
                CREATE UNIQUE INDEX IF NOT EXISTS ux_item_merges_subordinate
                    ON item_merges(subordinate_item_id);
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
            """)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        item_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(items)")}
        if "merged_into_item_id" not in item_cols:
            with self.conn:
                self.conn.execute(
                    "ALTER TABLE items ADD COLUMN merged_into_item_id INTEGER REFERENCES items(id)"
                )
        record_cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(records)")}
        indexes = {r["name"] for r in self.conn.execute("PRAGMA index_list(records)")}
        if "origin_item_id" not in record_cols:
            self.conn.execute("PRAGMA foreign_keys=OFF")
            with self.conn:
                self.conn.executescript("""
                    ALTER TABLE records RENAME TO records_legacy;
                    CREATE TABLE records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                        kind TEXT NOT NULL,
                        detail TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'open'
                            CHECK(status IN ('open','closed')),
                        external_ref TEXT,
                        origin_item_id INTEGER NOT NULL,
                        conflict_status TEXT NOT NULL DEFAULT 'active'
                            CHECK(conflict_status IN ('active','duplicate_conflict')),
                        conflict_of_record_id INTEGER REFERENCES records(id),
                        created_by TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    INSERT INTO records(id, item_id, kind, detail, status, external_ref,
                        origin_item_id, conflict_status, conflict_of_record_id,
                        created_by, created_at)
                    SELECT id, item_id, kind, detail, status, external_ref,
                        item_id, 'active', NULL, created_by, created_at FROM records_legacy;
                    DROP TABLE records_legacy;
                """)
            self.conn.execute("PRAGMA foreign_keys=ON")
        if "ux_records_external_ref" not in indexes:
            with self.conn:
                self.conn.execute("DROP INDEX IF EXISTS ux_records_item_ref")
                self.conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_records_external_ref
                    ON records(item_id, external_ref)
                    WHERE external_ref IS NOT NULL AND conflict_status='active'
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
                       origin_item_id, conflict_status, created_by, created_at)
                       VALUES(?,?,?,?,?,?,'active',?,?)""",
                    (item_id, kind, detail, status, external_ref, item_id, actor, now),
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
                """SELECT * FROM records
                   WHERE item_id=? AND conflict_status='active' ORDER BY id""",
                (item_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_record(self, record_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFoundError("记录不存在")
        return dict(row)

    def records_for_item_history(self, item_id: int) -> List[Dict[str, Any]]:
        """原始或当前归属在该事件上的全部记录，含冲突留档。"""
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                """SELECT * FROM records
                   WHERE origin_item_id=? OR item_id=? ORDER BY id""",
                (item_id, item_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                """SELECT COUNT(*) AS n FROM records
                   WHERE item_id=? AND status='open' AND conflict_status='active'""",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def get_merge_by_pair(self, primary_item_id: int,
                          subordinate_item_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                f"SELECT * FROM {MERGE_REQUEST_TABLE} WHERE primary_item_id=? AND subordinate_item_id=?",
                (primary_item_id, subordinate_item_id),
            ).fetchone()
        return self._merge_row(row) if row else None

    def get_merge_by_subordinate(self, subordinate_item_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                f"SELECT * FROM {MERGE_REQUEST_TABLE} WHERE subordinate_item_id=?",
                (subordinate_item_id,),
            ).fetchone()
        return self._merge_row(row) if row else None

    @staticmethod
    def _merge_row(row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        result["moved_record_ids"] = json.loads(result["moved_record_ids"])
        result["conflicts"] = json.loads(result["conflicts"])
        return result

    def merge_items(self, merge_key: str, primary_item_id: int, subordinate_item_id: int,
                    primary_expected_version: int, subordinate_expected_version: int,
                    actor: str, fail_after_insert: bool = False) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            primary = self.conn.execute(
                "SELECT * FROM items WHERE id=?", (primary_item_id,)).fetchone()
            subordinate = self.conn.execute(
                "SELECT * FROM items WHERE id=?", (subordinate_item_id,)).fetchone()
            if primary is None or subordinate is None:
                raise NotFoundError("事件不存在")
            if primary["merged_into_item_id"] is not None:
                raise ConflictError("主事件本身已被归并，不能作为主事件")
            if subordinate["merged_into_item_id"] is not None:
                existing = self.get_merge_by_subordinate(subordinate_item_id)
                raise MergeAlreadyExists(existing)
            already_primary = self.conn.execute(
                f"SELECT 1 FROM {MERGE_REQUEST_TABLE} WHERE primary_item_id=? LIMIT 1",
                (subordinate_item_id,),
            ).fetchone()
            if already_primary is not None:
                raise ConflictError("该事件已作为其他事件的主事件，不能再被归并")
            if primary["status"] == "closed" or subordinate["status"] == "closed":
                raise ConflictError("已关闭事件不能归并")
            bumped = self.conn.execute(
                """UPDATE items SET version=version+1, updated_at=?
                   WHERE id=? AND version=? AND merged_into_item_id IS NULL""",
                (now, primary_item_id, primary_expected_version),
            ).rowcount
            if bumped == 0:
                raise ConflictError("主事件版本冲突，请刷新后重试")
            bumped = self.conn.execute(
                """UPDATE items SET version=version+1, updated_at=?, merged_into_item_id=?
                   WHERE id=? AND version=? AND merged_into_item_id IS NULL""",
                (now, primary_item_id, subordinate_item_id, subordinate_expected_version),
            ).rowcount
            if bumped == 0:
                raise ConflictError("从属事件版本冲突，请刷新后重试")

            all_records = [dict(r) for r in self.conn.execute(
                """SELECT * FROM records WHERE item_id IN (?,?) ORDER BY id""",
                (primary_item_id, subordinate_item_id),
            ).fetchall()]
            groups: Dict[Any, List[Dict[str, Any]]] = {}
            no_ref: List[Dict[str, Any]] = []
            for record in all_records:
                if record["conflict_status"] != "active":
                    continue
                if record["external_ref"] is None:
                    no_ref.append(record)
                else:
                    groups.setdefault(record["external_ref"], []).append(record)

            moved_ids: List[int] = []
            kept_ids: List[int] = []
            conflicts: List[Dict[str, Any]] = []
            for record in no_ref:
                if record["item_id"] == subordinate_item_id:
                    self._move_record(record["id"], primary_item_id)
                    moved_ids.append(record["id"])
                else:
                    kept_ids.append(record["id"])
            for external_ref, members in groups.items():
                winner = members[0]  # ORDER BY id：最早一条
                losers = members[1:]
                if winner["item_id"] == subordinate_item_id:
                    self._move_record(winner["id"], primary_item_id)
                    moved_ids.append(winner["id"])
                else:
                    kept_ids.append(winner["id"])
                for loser in losers:
                    self.conn.execute(
                        """UPDATE records SET conflict_status='duplicate_conflict',
                           conflict_of_record_id=? WHERE id=?""",
                        (winner["id"], loser["id"]),
                    )
                    conflicts.append({
                        "record_id": loser["id"],
                        "origin_item_id": loser["item_id"],
                        "external_ref": external_ref,
                        "kept_record_id": winner["id"],
                        "kept_origin_item_id": winner["item_id"],
                        "kind": loser["kind"],
                        "created_by": loser["created_by"],
                    })
            moved_ids.sort()
            kept_ids.sort()
            conflicts.sort(key=lambda c: c["record_id"])

            try:
                cur = self.conn.execute(
                    f"""INSERT INTO {MERGE_REQUEST_TABLE}(merge_key, primary_item_id,
                       subordinate_item_id, primary_expected_version,
                       subordinate_expected_version, primary_result_version,
                       subordinate_result_version, moved_record_ids, conflicts,
                       actor, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (merge_key, primary_item_id, subordinate_item_id,
                     primary_expected_version, subordinate_expected_version,
                     primary_expected_version + 1, subordinate_expected_version + 1,
                     json.dumps(moved_ids), json.dumps(conflicts, ensure_ascii=False),
                     actor, now),
                )
                merge_id = int(cur.lastrowid)
            except sqlite3.IntegrityError as exc:
                existing = self.get_merge_by_pair(primary_item_id, subordinate_item_id)
                raise MergeAlreadyExists(existing) from exc

            if fail_after_insert:
                raise RuntimeError("注入失败：归并写入中断")

            self._append_audit_in_tx("merge", ENTITY, primary_item_id, actor, {
                "merge_id": merge_id,
                "merge_key": merge_key,
                "primary_item_id": primary_item_id,
                "subordinate_item_id": subordinate_item_id,
                "expected_versions": {
                    "primary": primary_expected_version,
                    "subordinate": subordinate_expected_version,
                },
                "result_versions": {
                    "primary": primary_expected_version + 1,
                    "subordinate": subordinate_expected_version + 1,
                },
                "moved_record_ids": moved_ids,
                "kept_record_ids": kept_ids,
                "conflict_record_ids": [c["record_id"] for c in conflicts],
                "conflicts": conflicts,
            })
            self._append_audit_in_tx("merge_link", ENTITY, subordinate_item_id, actor, {
                "merge_id": merge_id,
                "merge_key": merge_key,
                "primary_item_id": primary_item_id,
                "subordinate_item_id": subordinate_item_id,
                "moved_record_ids": moved_ids,
                "conflict_record_ids": [c["record_id"] for c in conflicts],
            })

        return self.get_merge_by_pair(primary_item_id, subordinate_item_id)

    def _move_record(self, record_id: int, target_item_id: int) -> None:
        # origin_item_id 保持首次登记事件不变，item_id 更新为当前归属
        self.conn.execute(
            "UPDATE records SET item_id=? WHERE id=?", (target_item_id, record_id))

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
