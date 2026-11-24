"""共享测点授权领域服务：维护合作产线对共享测点的受控授权区间与允许使用粒度。

台账保存每次授权登记与更正的版本痕迹（旧版本置为「已更正」），并把共享起止时间
与允许粒度回写到测点台账，供序列导出按实际采样时间核对可交付窗口。
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def parse_ts(value: str) -> datetime:
    """解析 ISO8601 时间字符串，兼容尾部 Z 与空格分隔。"""
    return datetime.fromisoformat(value.strip().replace("Z", "").replace(" ", "T"))


def parse_granularity_seconds(granularity: str) -> int:
    """将允许使用粒度（如 1min/1m/60s）解析为秒数，无法识别时拒绝授权。"""
    text = str(granularity).strip().lower()
    if text.endswith("ms"):
        return max(1, int(text[:-2]) // 1000)
    if text.endswith("min"):
        return int(text[:-3]) * 60
    if text.endswith("m"):
        return int(text[:-1]) * 60
    if text.endswith("s"):
        return int(text[:-1])
    if text.endswith("h"):
        return int(text[:-1]) * 3600
    raise ValueError(f"无法识别的粒度：{granularity}")


class SharedGrantService(BaseModuleService):
    MODULE = "shared_grant"
    ENTITY = "共享授权"
    OPERATION_TERM = "授权"
    LIST_FIELDS = ["授权编号", "测点编号", "所属产线", "共享起始时间", "共享截止时间",
                   "允许使用粒度", "授权版本", "授权依据"]
    REQUIRED_FIELDS = ["授权编号", "测点编号", "所属产线", "共享起始时间",
                       "共享截止时间", "允许使用粒度"]
    STATUS_ORDER = ["草稿", "生效中", "已更正", "已停用"]
    ACTION_RULES = {"确认生效": "生效中", "更正授权": "已更正", "停用授权": "已停用"}
    NEGATIVE_ACTIONS = []

    def register_grant(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记一条共享测点授权：校验区间与粒度，并在测点台账标出共享起止与粒度。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            return {"error": f"缺少必填字段：{'、'.join(missing)}"}
        try:
            start = parse_ts(entry["共享起始时间"])
            end = parse_ts(entry["共享截止时间"])
            if end <= start:
                store.rows(self.MODULE).remove(entry)
                return {"error": "共享截止时间必须晚于共享起始时间"}
            granule_seconds = parse_granularity_seconds(entry["允许使用粒度"])
        except ValueError as exc:
            store.rows(self.MODULE).remove(entry)
            return {"error": str(exc)}

        entry["授权版本"] = entry.get("授权版本") or "v1"
        entry["授权依据"] = entry.get("授权依据") or "产线共享协议"
        entry["粒度秒数"] = granule_seconds
        self.run_action(entry["id"], "确认生效")

        self._mark_point_ledger(entry)
        # 授权登记凭据：写入共享授权登记文件，供合作方对账
        self._write_jsonl(
            self._data_dir("shared_grants") / "grant_registry.jsonl",
            [{"action": "register", "grant_id": entry["授权编号"],
              "point": entry["测点编号"], "line": entry["所属产线"],
              "start": entry["共享起始时间"], "end": entry["共享截止时间"],
              "granularity": entry["允许使用粒度"], "version": entry["授权版本"],
              "at": time.time()}],
        )
        return {"grant": entry, "ledger_marked": True}

    def correct_grant(self, grant_id: int, new_start: str, new_end: str,
                      new_granularity: str | None = None,
                      basis: str | None = None) -> dict[str, Any]:
        """授权范围更正：旧版本置「已更正」并留痕，按扩区后的区间签发新版本。

        新旧版本都保留在台账中，历史授权区间可继续用于历史对账。
        """
        old = store.find(self.MODULE, grant_id)
        if old is None:
            return {"error": f"共享授权 {grant_id} 不存在"}
        try:
            start = parse_ts(new_start)
            end = parse_ts(new_end)
            if end <= start:
                return {"error": "更正后的截止时间必须晚于起始时间"}
            granularity = new_granularity or old["允许使用粒度"]
            granule_seconds = parse_granularity_seconds(granularity)
        except ValueError as exc:
            return {"error": str(exc)}

        # 旧版本留痕，不再作为现时授权
        old["status"] = "已更正"
        old["pending"] = False
        old["abnormal"] = False

        version_prefix = (old.get("授权版本") or "v1").rstrip("0123456789") or "v"
        old_version_num = "".join(ch for ch in str(old.get("授权版本", "v1")) if ch.isdigit()) or "1"
        new_version = f"{version_prefix}{int(old_version_num) + 1}"

        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        new_entry = {
            "id": new_id,
            "授权编号": old["授权编号"],
            "测点编号": old["测点编号"],
            "所属产线": old["所属产线"],
            "共享起始时间": new_start,
            "共享截止时间": new_end,
            "允许使用粒度": granularity,
            "粒度秒数": granule_seconds,
            "授权版本": new_version,
            "授权依据": basis or f"授权更正（原版本 {old.get('授权版本')}）",
            "status": "生效中",
            "pending": False,
            "abnormal": False,
        }
        rows.append(new_entry)
        self._mark_point_ledger(new_entry)
        self._write_jsonl(
            self._data_dir("shared_grants") / "grant_registry.jsonl",
            [{"action": "correct", "grant_id": new_entry["授权编号"],
              "point": new_entry["测点编号"], "line": new_entry["所属产线"],
              "old_start": old["共享起始时间"], "old_end": old["共享截止时间"],
              "new_start": new_start, "new_end": new_end,
              "granularity": granularity, "version": new_version,
              "supersedes": old.get("授权版本"), "at": time.time()}],
        )
        return {"old_grant": old, "new_grant": new_entry}

    def effective_grant(self, point: str, line: str,
                        at: datetime | None = None) -> dict[str, Any] | None:
        """返回测点在指定时刻（默认现时）的生效授权，已更正/停用版本不参与。

        授权有效性由登记的共享区间与状态决定，不依赖服务器墙钟，保证历史区间
        仍可按原始授权范围进行对账；`at` 给定时再按时刻交集过滤。
        """
        candidates = [
            row for row in sorted(store.rows(self.MODULE), key=lambda r: int(r.get("id", 0)), reverse=True)
            if row.get("测点编号") == point
            and row.get("所属产线") == line
            and row.get("status") == "生效中"
        ]
        if at is None:
            return candidates[0] if candidates else None
        for row in candidates:
            try:
                if parse_ts(row["共享起始时间"]) <= at < parse_ts(row["共享截止时间"]):
                    return row
            except (KeyError, ValueError):
                continue
        return None

    def _mark_point_ledger(self, grant: dict[str, Any]) -> None:
        """把共享起止时间和允许使用粒度回写到测点台账（受控共享标记）。"""
        point_svc = self._safe_import("app.services.sensor_point", "SensorPointService")()
        point_svc.mark_shared(
            grant["测点编号"],
            start=grant["共享起始时间"],
            end=grant["共享截止时间"],
            granularity=grant["允许使用粒度"],
            line=grant["所属产线"],
            version=grant.get("授权版本", ""),
        )
