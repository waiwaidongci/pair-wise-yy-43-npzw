from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import ensure_role, normalize_severity, require_number, require_text
from .repository import Repository
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
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        item = self.repository.get_item(item_id)
        merge_info = self.repository.merge_info_map([item_id]).get(item_id, {})
        return self.enrich(item, merge_info)

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        items = self.repository.list_items(status)
        ids = [item["id"] for item in items]
        infos = self.repository.merge_info_map(ids)
        return [self.enrich(item, infos.get(item["id"], {})) for item in items]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    def list_merges(self, role: str) -> list:
        self._view(role)
        return self.repository.list_merges()

    @staticmethod
    def _require_positive_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError(f"{field}必须是正整数")
        return value

    def merge(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, MERGE_ROLES)
        actor = require_text(actor, "actor", 100)
        primary_id = self._require_positive_int(payload.get("primary_id"), "primary_id")
        secondary_id = self._require_positive_int(payload.get("secondary_id"), "secondary_id")
        primary_version = self._require_positive_int(
            payload.get("primary_version"), "primary_version")
        secondary_version = self._require_positive_int(
            payload.get("secondary_version"), "secondary_version")
        if primary_id == secondary_id:
            from .domain import ValidationError
            raise ValidationError("主事件和从属事件不能相同")
        merge, _created = self.repository.merge_items(
            primary_id, secondary_id, primary_version, secondary_version, actor)
        return merge

    def enrich(self, item: Dict[str, Any], merge_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        if merge_info:
            result.update(merge_info)
        return result
