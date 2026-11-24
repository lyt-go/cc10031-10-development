"""序列导出领域服务：维护时序结果按测点与时间范围的分片导出任务与导出水位。

在原有降采样/归并数据导出之外，新增共享测点的受控交付：
- 按测点台账中的共享授权区间与允许使用粒度，依据原始观测的实际采样时间核对可交付窗口；
- 每个已领取分片对应一个授权窗口，交付即登记凭据（含校验值）供合作方对账；
- 中断再次领取时，在现时有效授权范围内只续发尚未领取的分片，已确认分片不重复登记。
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.services.shared_grant import parse_granularity_seconds, parse_ts
from app.store import store


class SeriesExportService(BaseModuleService):
    MODULE = "series_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["导出编号", "测点范围", "时间范围", "导出格式", "采样粒度", "记录条数", "分片大小", "导出水位",
                   "已领取分片", "授权窗口总数"]
    REQUIRED_FIELDS = ["导出编号", "测点范围", "时间范围"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    # 数据导出操作：按测点范围时间窗口导出降采样与归并结果到分片导出文件
    ACTION_RULES = {"启动导出": "导出中", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 原始观测登记：受控交付只认带有实际采样时间的原始观测
    # ------------------------------------------------------------------

    def register_raw_observations(self, point: str,
                                  observations: list[dict[str, Any]]) -> dict[str, Any]:
        """登记共享测点的原始观测（含实际采样时间），作为受控交付的唯一可核对来源。"""
        cleaned = []
        for obs in observations:
            ts = str(obs.get("timestamp", "")).strip()
            if not ts:
                continue
            parse_ts(ts)  # 触发格式校验，非法时间直接拒绝
            cleaned.append({
                "point": point,
                "timestamp": ts,
                "value": obs.get("value", 0),
                "source": obs.get("source", "raw"),
            })
        cleaned.sort(key=lambda r: r["timestamp"])
        output = self._data_dir("raw_observations") / f"{point}_raw.jsonl"
        written = self._write_jsonl(output, cleaned)
        return {"point": point, "registered": written, "file": str(output)}

    def _load_raw_observations(self, point: str) -> list[dict[str, Any]]:
        return self._read_jsonl(self._data_dir("raw_observations") / f"{point}_raw.jsonl")

    # ------------------------------------------------------------------
    # 受控导出：请求创建与按授权核对的分片规划
    # ------------------------------------------------------------------

    def create_controlled_export(self, point: str, line: str, start: str, end: str,
                                 granularity: str, fmt: str = "jsonl") -> dict[str, Any]:
        """创建受控导出请求。请求先登记，真正可交付范围在每次领取时按现时授权核对。"""
        try:
            req_start = parse_ts(start)
            req_end = parse_ts(end)
            if req_end <= req_start:
                return {"error": "导出截止时间必须晚于起始时间"}
            granule_seconds = parse_granularity_seconds(granularity)
        except ValueError as exc:
            return {"error": str(exc)}

        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        export_no = f"EXPORT-{point}-{new_id}"
        entry = {
            "id": new_id,
            "导出编号": export_no,
            "测点范围": point,
            "所属产线": line,
            "时间范围": f"{start} ~ {end}",
            "请求起始时间": start,
            "请求截止时间": end,
            "导出格式": fmt,
            "采样粒度": granularity,
            "粒度秒数": granule_seconds,
            "记录条数": "0",
            "分片大小": "0",
            "导出水位": "0",
            "已领取分片": "0",
            "授权窗口总数": "0",
            "受控交付": True,
            "status": "待导出",
            "pending": True,
            "abnormal": False,
        }
        rows.append(entry)
        self._write_jsonl(
            self._data_dir("exports") / "controlled_export_requests.jsonl",
            [{"action": "create_request", "export_id": export_no, "point": point,
              "line": line, "start": start, "end": end,
              "granularity": granularity, "at": time.time()}],
        )
        return {"export_id": export_no, "entry_id": new_id, "point": point, "line": line,
                "request_range": [start, end], "granularity": granularity}

    def _plan_shards(self, task: dict[str, Any]) -> list[dict[str, Any]]:
        """按现时有效授权与原始观测的实际采样时间规划可交付分片（一窗一片）。

        授权之外的观测既不进入任何窗口，也不产生任何可反推的汇总值。
        """
        point = task["测点范围"]
        line = task.get("所属产线", "")
        granularity = task["采样粒度"]
        granule_seconds = int(task.get("粒度秒数") or parse_granularity_seconds(granularity))

        grant_svc = self._safe_import("app.services.shared_grant", "SharedGrantService")()
        grant = grant_svc.effective_grant(point, line)
        if grant is None:
            return []
        # 允许使用粒度：导出粒度不得细于授权粒度
        if int(grant.get("粒度秒数") or parse_granularity_seconds(grant["允许使用粒度"])) > granule_seconds:
            return []

        grant_start = parse_ts(grant["共享起始时间"])
        grant_end = parse_ts(grant["共享截止时间"])
        req_start = parse_ts(task["请求起始时间"])
        req_end = parse_ts(task["请求截止时间"])
        # 实际可核对区间 = 请求范围 ∩ 现时授权范围
        lo = max(req_start, grant_start)
        hi = min(req_end, grant_end)
        if hi <= lo:
            return []

        observations = self._load_raw_observations(point)
        step = timedelta(seconds=granule_seconds)
        shards: list[dict[str, Any]] = []
        # 窗口按授权起点对齐，保证扩区前后同一窗口编号稳定、可续领
        window_start = grant_start + ((lo - grant_start) // step) * step
        while window_start < hi:
            window_end = min(window_start + step, hi)
            records = [
                r for r in observations
                if window_start <= parse_ts(r["timestamp"]) < window_end
            ]
            if records:  # 只有存在可核对原始观测的可交付窗口才登记分片
                window_iso = window_start.isoformat()
                payload = json.dumps(records, ensure_ascii=False, sort_keys=True)
                shards.append({
                    "shard_id": f"{task['导出编号']}-W{window_iso}",
                    "export_id": task["导出编号"],
                    "point": point,
                    "line": line,
                    "window_start": window_iso,
                    "window_end": window_end.isoformat(),
                    "granularity": granularity,
                    "grant_id": grant["授权编号"],
                    "grant_version": grant.get("授权版本", ""),
                    "record_count": len(records),
                    "records": records,
                    "checksum": self._compute_checksum(payload),
                })
            window_start += step
        return shards

    def _receipts_path(self, task: dict[str, Any]) -> "Any":
        from pathlib import Path
        return self._data_dir("delivery_receipts") / f"{task['导出编号']}_receipts.jsonl"

    def _claimed_shard_ids(self, task: dict[str, Any]) -> set[str]:
        return {r["shard_id"] for r in self._read_jsonl(self._receipts_path(task))}

    def claim_shards(self, export_id: int, max_shards: int | None = None) -> dict[str, Any]:
        """领取并交付分片：只发送现时授权范围内尚未领取的分片，逐片留下交付凭据。

        中断后重复调用是幂等的：已确认分片仅可用于历史对账，不会再次登记交付。
        """
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        if not task.get("受控交付"):
            return {"error": "该导出任务不是受控共享交付，请使用 export_series"}

        point, line = task["测点范围"], task.get("所属产线", "")
        grant_svc = self._safe_import("app.services.shared_grant", "SharedGrantService")()
        grant = grant_svc.effective_grant(point, line)
        if grant is None:
            return {"error": "当前不存在覆盖该测点与产线的有效共享授权，授权之外不得交付任何观测"}
        granule_seconds = int(task.get("粒度秒数") or parse_granularity_seconds(task["采样粒度"]))
        if int(grant.get("粒度秒数") or parse_granularity_seconds(grant["允许使用粒度"])) > granule_seconds:
            return {"error": f"导出粒度 {task['采样粒度']} 细于授权允许粒度 {grant['允许使用粒度']}，拒绝交付"}

        # 请求区间与现时授权完全不相交：授权之外不得交付任何观测或可反推的汇总值
        grant_start = parse_ts(grant["共享起始时间"])
        grant_end = parse_ts(grant["共享截止时间"])
        if min(parse_ts(task["请求截止时间"]), grant_end) <= max(parse_ts(task["请求起始时间"]), grant_start):
            return {"error": "请求时间范围完全落在现时授权区间之外，授权之外不交付任何观测或汇总值"}

        self.run_action(export_id, "启动导出")
        planned = self._plan_shards(task)
        claimed = self._claimed_shard_ids(task)
        pending = [shard for shard in planned if shard["shard_id"] not in claimed]
        if max_shards is not None and max_shards >= 0:
            pending = pending[:max_shards]

        delivery_file = self._data_dir("exports") / f"{task['导出编号']}_delivery.jsonl"
        receipts: list[dict[str, Any]] = []
        for shard in pending:
            records = shard.pop("records")
            # 交付数据：逐片写入交付文件，记录实际采样时间与原始观测
            self._write_jsonl(delivery_file, records)
            receipt = {
                "receipt_id": f"RCPT-{shard['shard_id']}",
                "export_id": shard["export_id"],
                "shard_id": shard["shard_id"],
                "point": shard["point"],
                "line": shard["line"],
                "window_start": shard["window_start"],
                "window_end": shard["window_end"],
                "granularity": shard["granularity"],
                "record_count": shard["record_count"],
                "grant_id": shard["grant_id"],
                "grant_version": shard["grant_version"],
                "checksum": shard["checksum"],
                "delivered_at": datetime.now().isoformat(),
            }
            self._write_jsonl(self._receipts_path(task), [receipt])
            receipts.append(receipt)

        all_receipts = self._read_jsonl(self._receipts_path(task))
        delivered_records = sum(int(r["record_count"]) for r in all_receipts)
        task["已领取分片"] = str(len(all_receipts))
        task["授权窗口总数"] = str(len(planned))
        task["记录条数"] = str(delivered_records)
        task["分片大小"] = str(delivered_records * 80)
        task["导出水位"] = str(len(all_receipts))
        task["最近授权版本"] = grant.get("授权版本", "")
        if len(all_receipts) >= len(planned) and planned:
            self.run_action(export_id, "确认交付")

        return {
            "export_id": task["导出编号"],
            "effective_grant": {"grant_id": grant["授权编号"], "version": grant.get("授权版本", ""),
                                "start": grant["共享起始时间"], "end": grant["共享截止时间"],
                                "granularity": grant["允许使用粒度"]},
            "delivered_this_claim": len(receipts),
            "claimed_total": len(all_receipts),
            "deliverable_windows": len(planned),
            "remaining_shards": len(planned) - len(all_receipts),
            "records_delivered_total": delivered_records,
            "receipts": receipts,
        }

    # ------------------------------------------------------------------
    # 对账：凭据与原始观测核对
    # ------------------------------------------------------------------

    def delivery_receipts(self, export_id: int) -> dict[str, Any]:
        """返回一份导出的全部交付凭据，供合作方按窗口对账。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        receipts = self._read_jsonl(self._receipts_path(task))
        return {"export_id": task["导出编号"], "receipt_count": len(receipts), "receipts": receipts}

    def reconcile_export(self, export_id: int) -> dict[str, Any]:
        """按凭据逐窗重算原始观测校验值，核对交付记录数与内容是否可对账。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        observations = self._load_raw_observations(task["测点范围"])
        receipts = self._read_jsonl(self._receipts_path(task))
        windows = []
        all_match = True
        for receipt in receipts:
            records = [
                r for r in observations
                if receipt["window_start"] <= r["timestamp"] < receipt["window_end"]
            ]
            checksum = self._compute_checksum(
                json.dumps(records, ensure_ascii=False, sort_keys=True))
            matched = checksum == receipt["checksum"] and len(records) == receipt["record_count"]
            all_match = all_match and matched
            windows.append({
                "shard_id": receipt["shard_id"],
                "window_start": receipt["window_start"],
                "window_end": receipt["window_end"],
                "receipt_records": receipt["record_count"],
                "raw_records": len(records),
                "receipt_checksum": receipt["checksum"],
                "recomputed_checksum": checksum,
                "matched": matched,
            })
        return {"export_id": task["导出编号"], "window_count": len(windows),
                "all_matched": all_match, "windows": windows}

    # ------------------------------------------------------------------
    # 原有降采样/归并导出链路（保持兼容）
    # ------------------------------------------------------------------

    def export_series(self, export_id: int) -> dict[str, Any]:
        """按测点范围和时间窗口导出降采样与归并结果数据到分片导出交付文件。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        self.run_action(export_id, "启动导出")
        point_range = task.get("测点范围", "")
        time_range = task.get("时间范围", "")
        export_fmt = task.get("导出格式", "jsonl")

        # 扫描归并窗口和降采样数据文件导出
        exported = []
        rollup_dir = self._data_dir("rollup_windows")
        if rollup_dir.exists():
            for f in rollup_dir.glob("*_rollup.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    exported.append({
                        "export_id": task["导出编号"],
                        # 数据产物输出：写入导出结果为分片交付JSONL数据文件
                        "source": "rollup_window",
                        "point": r.get("point", ""),
                        "window_index": r.get("window_start", 0),
                        "value": r.get("rollup_value", 0),
                        "metric": r.get("metric", ""),
                    })

        ds_dir = self._data_dir("downsampled")
        if ds_dir.exists():
            for f in ds_dir.glob("*_downsampled.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    exported.append({
                        "export_id": task["导出编号"],
                        "source": "downsample",
                        "point": r.get("point", ""),
                        "window_index": r.get("window_index", 0),
                        "value": r.get("aggregated_value", 0),
                        "method": r.get("method", ""),
                    })

        # 将导出结果写入分片导出交付文件
        output_file = self._data_dir("exports") / f"{task['导出编号']}_export.{export_fmt}"
        written = self._write_jsonl(output_file, exported)
        task["记录条数"] = str(written)
        task["分片大小"] = str(len(exported) * 80)
        task["导出水位"] = str(written)
        self.run_action(export_id, "确认交付")
        return {"export_id": task["导出编号"], "format": export_fmt,
                "records_exported": written, "total_size_estimate": len(exported) * 80}

    def export_by_time_window(self, point_range: str, time_window: str,
                               fmt: str = "jsonl") -> dict[str, Any]:
        """按指定时间窗口和测点范围创建导出任务并输出交付数据。"""
        tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        self._auto_create_seed(self.MODULE, {
            "导出编号": f"EXPORT-{point_range}-{tid}",
            "测点范围": point_range,
            "时间范围": time_window,
            "导出格式": fmt,
            "采样粒度": "1h",
            "记录条数": "0",
            "分片大小": "0",
            "导出水位": "0",
        })
        return self.export_series(tid)
