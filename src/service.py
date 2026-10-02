from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

from .domain import (ConflictError, NotFoundError, ValidationError, ensure_role,
                     normalize_severity, require_id, require_number, require_text)
from .repository import MergeAlreadyExists, Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, MERGE_ROLES, RECORD_ROLES,
                    TITLE, VIEW_ROLES, completion_blockers, escalation_required,
                    priority_score, response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        if item.get("merged_into_item_id") is not None:
            raise ConflictError("从属事件已归并，记录请到主事件登记")
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        if item.get("merged_into_item_id") is not None:
            raise ConflictError("从属事件已归并，状态由主事件延续")
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    @staticmethod
    def _merge_key(primary_item_id: int, subordinate_item_id: int) -> str:
        raw = json.dumps(
            {"primary": primary_item_id, "subordinate": subordinate_item_id},
            sort_keys=True, separators=(",", ":"),
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def merge(self, payload: Dict[str, Any], actor: str, role: str,
              fail_after_insert: bool = False) -> Dict[str, Any]:
        ensure_role(role, MERGE_ROLES)
        actor = require_text(actor, "actor", 100)
        primary_item_id = require_id(payload.get("primary_item_id"), "primary_item_id")
        subordinate_item_id = require_id(
            payload.get("subordinate_item_id"), "subordinate_item_id")
        if primary_item_id == subordinate_item_id:
            raise ValidationError("主事件与从属事件不能是同一事件")
        primary_version = require_id(
            payload.get("primary_expected_version"), "primary_expected_version")
        subordinate_version = require_id(
            payload.get("subordinate_expected_version"), "subordinate_expected_version")
        merge_key = self._merge_key(primary_item_id, subordinate_item_id)
        try:
            stored = self.repository.merge_items(
                merge_key, primary_item_id, subordinate_item_id,
                primary_version, subordinate_version, actor,
                fail_after_insert=fail_after_insert,
            )
            replayed = False
        except MergeAlreadyExists as exc:
            stored = exc.merge
            replayed = True
        return self._merge_manifest(stored, replayed=replayed)

    def get_merge(self, primary_item_id: int, subordinate_item_id: int,
                  role: str) -> Dict[str, Any]:
        self._view(role)
        stored = self.repository.get_merge_by_pair(primary_item_id, subordinate_item_id)
        if stored is None:
            raise NotFoundError("归并关系不存在")
        return self._merge_manifest(stored, replayed=True)

    def _merge_manifest(self, stored: Dict[str, Any], replayed: bool) -> Dict[str, Any]:
        primary = self.enrich(self.repository.get_item(stored["primary_item_id"]))
        subordinate = self.enrich(self.repository.get_item(stored["subordinate_item_id"]))
        moved_records = []
        for record_id in stored["moved_record_ids"]:
            record = self._record_by_id(record_id)
            moved_records.append({
                "record_id": record["id"],
                "origin_item_id": record["origin_item_id"],
                "current_item_id": record["item_id"],
                "external_ref": record["external_ref"],
                "kind": record["kind"],
                "status": record["status"],
            })
        return {
            "merge_id": stored["id"],
            "merge_key": stored["merge_key"],
            "replayed": replayed,
            "actor": stored["actor"],
            "created_at": stored["created_at"],
            "primary_item": primary,
            "subordinate_item": subordinate,
            "expected_versions": {
                "primary": stored["primary_expected_version"],
                "subordinate": stored["subordinate_expected_version"],
            },
            "result_versions": {
                "primary": stored["primary_result_version"],
                "subordinate": stored["subordinate_result_version"],
            },
            "moved_records": moved_records,
            "moved_record_ids": list(stored["moved_record_ids"]),
            "conflicts": list(stored["conflicts"]),
            "merge": {
                "primary_item_id": stored["primary_item_id"],
                "subordinate_item_id": stored["subordinate_item_id"],
                "merge_id": stored["id"],
            },
        }

    def _record_by_id(self, record_id: int) -> Dict[str, Any]:
        return self.repository.get_record(record_id)

    def item_history(self, item_id: int, role: str) -> Dict[str, Any]:
        """从属事件仍可查：原状态、原记录编号和归并关系。"""
        self._view(role)
        item = self.enrich(self.repository.get_item(item_id))
        records = self.repository.records_for_item_history(item_id)
        merge = None
        if item.get("merged_into_item_id") is not None:
            stored = self.repository.get_merge_by_subordinate(item_id)
            if stored is not None:
                merge = {
                    "merge_id": stored["id"],
                    "merge_key": stored["merge_key"],
                    "primary_item_id": stored["primary_item_id"],
                    "subordinate_item_id": stored["subordinate_item_id"],
                    "created_at": stored["created_at"],
                    "actor": stored["actor"],
                }
        return {"item": item, "records": records, "merge": merge}

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        merged_into = item.get("merged_into_item_id")
        result["merged_into_item_id"] = merged_into
        result["is_subordinate"] = merged_into is not None
        return result
