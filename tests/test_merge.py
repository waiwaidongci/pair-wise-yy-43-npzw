import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import (ConflictError, NotFoundError, PermissionDenied,
                        ValidationError)
from src.repository import Repository
from src.service import Service


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.primary = self.service.create_item(
            {"title": "primary spill", "description": "first report",
             "severity": "major", "external_ref": "MRG-P"},
            "creator", "observer")
        self.subordinate = self.service.create_item(
            {"title": "same spill", "description": "second report",
             "severity": "minor", "external_ref": "MRG-S"},
            "creator", "observer")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _record(self, item, ref, status="closed", kind="containment", detail="d"):
        return self.service.add_record(
            item["id"],
            {"kind": kind, "detail": detail, "status": status, "external_ref": ref},
            "recorder", "response_commander")

    def _payload(self, primary_version=None, subordinate_version=None):
        return {
            "primary_item_id": self.primary["id"],
            "subordinate_item_id": self.subordinate["id"],
            "primary_expected_version": primary_version or self.primary["version"],
            "subordinate_expected_version": subordinate_version or self.subordinate["version"],
        }

    def test_merge_moves_records_and_lists_conflicts(self):
        kept = self._record(self.primary, "SITE-1")
        duplicate = self._record(self.subordinate, "SITE-1", status="open",
                                 kind="monitoring")
        moved = self._record(self.subordinate, "SITE-2", kind="monitoring")
        no_ref = self.service.add_record(
            self.subordinate["id"],
            {"kind": "containment", "detail": "no ref", "status": "closed"},
            "recorder", "response_commander")

        manifest = self.service.merge(self._payload(), "chief", "response_commander")

        self.assertFalse(manifest["replayed"])
        self.assertEqual(manifest["result_versions"],
                         {"primary": self.primary["version"] + 1,
                          "subordinate": self.subordinate["version"] + 1})
        self.assertEqual(sorted(manifest["moved_record_ids"]),
                         sorted([moved["id"], no_ref["id"]]))
        self.assertEqual([c["record_id"] for c in manifest["conflicts"]],
                         [duplicate["id"]])
        conflict = manifest["conflicts"][0]
        self.assertEqual(conflict["kept_record_id"], kept["id"])
        self.assertEqual(conflict["origin_item_id"], self.subordinate["id"])
        self.assertEqual(conflict["kept_origin_item_id"], self.primary["id"])

        active = sorted(r["id"] for r in self.service.list_records(
            self.primary["id"], "viewer"))
        self.assertEqual(active, sorted([kept["id"], moved["id"], no_ref["id"]]))
        self.assertEqual(self.service.list_records(self.subordinate["id"], "viewer"), [])
        moved_row = self.repo.get_record(moved["id"])
        self.assertEqual(moved_row["item_id"], self.primary["id"])
        self.assertEqual(moved_row["origin_item_id"], self.subordinate["id"])
        dup_row = self.repo.get_record(duplicate["id"])
        self.assertEqual(dup_row["item_id"], self.subordinate["id"])
        self.assertEqual(dup_row["conflict_status"], "duplicate_conflict")
        self.assertEqual(dup_row["conflict_of_record_id"], kept["id"])

        primary = self.service.get_item(self.primary["id"], "viewer")
        subordinate = self.service.get_item(self.subordinate["id"], "viewer")
        self.assertIsNone(primary["merged_into_item_id"])
        self.assertEqual(subordinate["merged_into_item_id"], self.primary["id"])
        self.assertTrue(subordinate["is_subordinate"])
        self.assertEqual(subordinate["version"], self.subordinate["version"] + 1)

    def test_primary_side_duplicate_wins(self):
        kept = self._record(self.primary, "DUP")
        duplicate = self._record(self.subordinate, "DUP", status="open")
        manifest = self.service.merge(self._payload(), "chief", "response_commander")
        self.assertEqual(manifest["moved_record_ids"], [])
        self.assertEqual(manifest["conflicts"][0]["record_id"], duplicate["id"])
        self.assertEqual(manifest["conflicts"][0]["kept_record_id"], kept["id"])
        # 冲突记录（原为open）不计入主事件未结事项
        self.assertEqual(self.repo.open_record_count(self.primary["id"]), 0)

    def test_open_items_keep_blocking_closure_after_merge(self):
        self._record(self.subordinate, "OPEN", status="open")
        manifest = self.service.merge(self._payload(), "chief", "response_commander")
        current = self.service.get_item(self.primary["id"], "viewer")
        self.assertEqual(self.repo.open_record_count(current["id"]), 1)
        for target, role in [("assessing", "response_commander"),
                             ("containing", "response_commander"),
                             ("recovering", "operations"),
                             ("monitoring", "operations")]:
            current = self.service.transition(
                current["id"], target, current["version"], "r", role)
        with self.assertRaises(ConflictError):
            self.service.transition(
                current["id"], "closed", current["version"],
                "r", "response_commander")
        del manifest

    def test_versions_must_match_on_both_sides(self):
        with self.assertRaises(ConflictError):
            self.service.merge(
                self._payload(primary_version=self.primary["version"] + 1),
                "chief", "response_commander")
        with self.assertRaises(ConflictError):
            self.service.merge(
                self._payload(subordinate_version=self.subordinate["version"] + 1),
                "chief", "response_commander")
        # 版本不匹配没有留下任何归并关系，版本也未自增
        self.assertIsNone(self.repo.get_merge_by_pair(
            self.primary["id"], self.subordinate["id"]))
        self.assertEqual(self.repo.get_item(self.primary["id"])["version"], 1)
        self.assertEqual(self.repo.get_item(self.subordinate["id"])["version"], 1)

    def test_duplicate_submission_reuses_first_result(self):
        first = self.service.merge(self._payload(), "chief-one",
                                   "response_commander")
        # 另一指挥员用完全相同的原始请求重复提交
        second = self.service.merge(self._payload(), "chief-two",
                                    "response_commander")
        # 即便版本早已变化，丢单/重复方仍拿到当前关系
        stale = self.service.merge(
            self._payload(primary_version=999, subordinate_version=999),
            "chief-three", "response_commander")
        for result in (second, stale):
            self.assertTrue(result["replayed"])
            self.assertEqual(result["merge_id"], first["merge_id"])
            self.assertEqual(result["merge"]["primary_item_id"],
                             self.primary["id"])
            self.assertEqual(result["merge"]["subordinate_item_id"],
                             self.subordinate["id"])
            self.assertEqual(result["primary_item"]["version"],
                             self.primary["version"] + 1)
        self.assertEqual(
            self.repo.conn.execute(
                "SELECT COUNT(*) AS n FROM item_merges").fetchone()["n"], 1)

    def test_concurrent_commanders_produce_single_merge(self):
        payload = self._payload()
        results = {}
        barrier = threading.Barrier(2)

        def submit(name):
            barrier.wait()
            results[name] = self.service.merge(
                payload, f"chief-{name}", "response_commander")

        threads = [threading.Thread(target=submit, args=(n,))
                   for n in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [n for n, r in results.items() if not r["replayed"]]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len({r["merge_id"] for r in results.values()}), 1)
        self.assertEqual(
            self.repo.conn.execute(
                "SELECT COUNT(*) AS n FROM item_merges").fetchone()["n"], 1)

    def test_write_failure_rolls_back_and_original_request_succeeds_on_retry(self):
        moved = self._record(self.subordinate, "SITE-9")
        with self.assertRaises(RuntimeError):
            self.service.merge(self._payload(), "chief",
                               "response_commander", fail_after_insert=True)
        # 原子回滚：无关系、无版本变化、记录未移动、无残留审计
        self.assertIsNone(self.repo.get_merge_by_pair(
            self.primary["id"], self.subordinate["id"]))
        self.assertEqual(self.repo.get_item(self.primary["id"])["version"], 1)
        self.assertIsNone(
            self.repo.get_item(self.subordinate["id"])["merged_into_item_id"])
        self.assertEqual(self.repo.get_record(moved["id"])["item_id"],
                         self.subordinate["id"])
        self.assertEqual(
            self.repo.conn.execute(
                "SELECT COUNT(*) AS n FROM audit_events WHERE action IN "
                "('merge','merge_link')").fetchone()["n"], 0)
        # 按原请求重试即成功
        manifest = self.service.merge(self._payload(), "chief",
                                      "response_commander")
        self.assertFalse(manifest["replayed"])
        self.assertEqual(manifest["moved_record_ids"], [moved["id"]])

    def test_subordinate_keeps_original_status_record_numbers_and_relation(self):
        original = self._record(self.subordinate, "ORIG", kind="monitoring")
        self.service.merge(self._payload(), "chief", "response_commander")
        history = self.service.item_history(self.subordinate["id"], "viewer")
        self.assertEqual(history["item"]["status"], "reported")
        self.assertEqual(history["merge"]["primary_item_id"], self.primary["id"])
        self.assertEqual(history["merge"]["subordinate_item_id"],
                         self.subordinate["id"])
        ids = [r["id"] for r in history["records"]]
        self.assertEqual(ids, [original["id"]])
        self.assertEqual(history["records"][0]["origin_item_id"],
                         self.subordinate["id"])
        self.assertEqual(history["records"][0]["item_id"], self.primary["id"])

        # 从属事件被冻结：不能登记记录、不能流转
        with self.assertRaises(ConflictError):
            self.service.add_record(
                self.subordinate["id"], {"kind": "x", "detail": "y"},
                "r", "response_commander")
        sub = self.service.get_item(self.subordinate["id"], "viewer")
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.subordinate["id"], "assessing", sub["version"],
                "r", "response_commander")

    def test_audit_events_cross_reference_manifest(self):
        self._record(self.subordinate, "SITE-A")
        duplicate = self._record(self.primary, "SITE-B")
        self._record(self.subordinate, "SITE-B", status="open")
        manifest = self.service.merge(self._payload(), "chief",
                                      "response_commander")
        events = self.service.audit("viewer")
        merge_events = [e for e in events if e["action"] == "merge"]
        link_events = [e for e in events if e["action"] == "merge_link"]
        self.assertEqual(len(merge_events), 1)
        self.assertEqual(len(link_events), 1)
        merge_event, link_event = merge_events[0], link_events[0]
        self.assertEqual(merge_event["entity_id"], self.primary["id"])
        self.assertEqual(link_event["entity_id"], self.subordinate["id"])
        self.assertEqual(merge_event["detail"]["merge_id"], manifest["merge_id"])
        self.assertEqual(link_event["detail"]["merge_id"], manifest["merge_id"])
        self.assertEqual(merge_event["detail"]["moved_record_ids"],
                         manifest["moved_record_ids"])
        self.assertEqual(merge_event["detail"]["conflict_record_ids"],
                         [c["record_id"] for c in manifest["conflicts"]])
        self.assertEqual(
            [c["record_id"] for c in merge_event["detail"]["conflicts"]],
            [c["record_id"] for c in manifest["conflicts"]])
        self.assertTrue(self.repo.verify_audit_chain())
        self.assertTrue(any(e["action"] == "merge"
                            for e in self.service.audit("viewer", self.primary["id"])))
        self.assertTrue(any(e["action"] == "merge_link"
                            for e in self.service.audit("viewer", self.subordinate["id"])))

    def test_permission_validation_and_relation_lookup(self):
        with self.assertRaises(PermissionDenied):
            self.service.merge(self._payload(), "x", "viewer")
        with self.assertRaises(ValidationError):
            payload = self._payload()
            payload["subordinate_item_id"] = self.primary["id"]
            self.service.merge(payload, "chief", "response_commander")
        with self.assertRaises(NotFoundError):
            self.service.get_merge(self.primary["id"], self.subordinate["id"],
                                   "viewer")
        self.service.merge(self._payload(), "chief", "response_commander")
        relation = self.service.get_merge(
            self.primary["id"], self.subordinate["id"], "viewer")
        self.assertEqual(relation["merge"]["primary_item_id"], self.primary["id"])


if __name__ == "__main__":
    unittest.main()
