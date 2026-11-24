"""共享测点受控交付集成测试。

场景一：同一请求在扩区（10:00-10:20 → 10:00-10:30，粒度不变）前发起、扩区后发送，
       校验实际交付范围、单窗记录数、中断续领与已确认分片不重复登记。
场景二：跨越现时授权边界的请求，验证授权之外无任何观测或可反推的汇总值。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            yield Path(tmp)


POINT = "SENS-SHARE-A"
LINE = "LINE-P1"
DAY = "2026-11-24"


def _observations(start_minute: int = 0, end_minute: int = 40):
    """每分钟一个可核对的原始观测，时间戳为实际采样时间。"""
    return [
        {"timestamp": f"{DAY}T10:{minute:02d}:00", "value": round(20.0 + minute * 0.1, 3),
         "source": "edge-raw"}
        for minute in range(start_minute, end_minute)
    ]


@pytest.fixture()
def provisioned(setup_data_dir):
    from app.services.sensor_point import SensorPointService
    from app.services.series_export import SeriesExportService
    from app.services.shared_grant import SharedGrantService

    grant_svc = SharedGrantService()
    export_svc = SeriesExportService()
    point_svc = SensorPointService()

    # 测点台账登记并投运
    point = point_svc.onboard_sensor({
        "测点编号": POINT, "所属产线": LINE, "传感器型号": "PT100",
        "采样频率": "1Hz", "计量单位": "C",
    })
    point_svc.activate_point(point["id"])
    # 原始观测：10:00 至 10:40 共 40 条，覆盖扩区后区间与授权外区间
    export_svc.register_raw_observations(POINT, _observations(0, 40))
    # 初始授权：10:00-10:20，粒度 1min
    grant_result = grant_svc.register_grant({
        "授权编号": "GRANT-SHARE-A", "测点编号": POINT, "所属产线": LINE,
        "共享起始时间": f"{DAY}T10:00:00", "共享截止时间": f"{DAY}T10:20:00",
        "允许使用粒度": "1min", "授权依据": "产线共享协议",
    })
    return {
        "grant_svc": grant_svc,
        "export_svc": export_svc,
        "point_svc": point_svc,
        "initial_grant": grant_result["grant"],
    }


class TestGrantLedgerMarking:
    def test_ledger_marks_shared_window_and_granularity(self, provisioned):
        from app.store import store

        point_row = next(r for r in store.rows("sensor_point") if r["测点编号"] == POINT)
        assert point_row["共享起始时间"] == f"{DAY}T10:00:00"
        assert point_row["共享截止时间"] == f"{DAY}T10:20:00"
        assert point_row["允许使用粒度"] == "1min"
        assert "受控共享" in point_row["共享标记"]

        grant_row = provisioned["initial_grant"]
        assert grant_row["status"] == "生效中"
        assert grant_row["授权版本"] == "v1"

    def test_invalid_grant_window_rejected(self, provisioned):
        result = provisioned["grant_svc"].register_grant({
            "授权编号": "GRANT-BAD", "测点编号": POINT, "所属产线": LINE,
            "共享起始时间": f"{DAY}T10:30:00", "共享截止时间": f"{DAY}T10:20:00",
            "允许使用粒度": "1min",
        })
        assert "error" in result


class TestControlledDeliveryWithGrantCorrection:
    """场景一：扩区前发起、扩区后发送；中断续领；已确认分片不重复登记。"""

    def test_request_before_correction_delivers_full_extended_range(self, provisioned, setup_data_dir):
        grant_svc = provisioned["grant_svc"]
        export_svc = provisioned["export_svc"]

        # 1) 扩区前发起请求：请求范围即覆盖 30 分钟，但此刻授权只有 20 分钟
        created = export_svc.create_controlled_export(
            POINT, LINE, f"{DAY}T10:00:00", f"{DAY}T10:30:00", "1min")
        assert "error" not in created
        export_id = created["entry_id"]

        # 2) 授权更正：10:00-10:20 扩大到 10:00-10:30，粒度不变
        grant_id = provisioned["initial_grant"]["id"]
        correction = grant_svc.correct_grant(
            grant_id, f"{DAY}T10:00:00", f"{DAY}T10:30:00",
            new_granularity="1min", basis="合作产线扩区")
        assert "error" not in correction
        assert correction["old_grant"]["status"] == "已更正"
        assert correction["new_grant"]["status"] == "生效中"
        assert correction["new_grant"]["授权版本"] == "v2"
        # 旧的 20 分钟版本仍保留在台账中，可作历史对账
        assert correction["old_grant"]["共享截止时间"] == f"{DAY}T10:20:00"

        # 3) 扩区后发送（首次领取被中断，只领到 12 个分片）
        first = export_svc.claim_shards(export_id, max_shards=12)
        assert first["effective_grant"]["version"] == "v2"
        assert first["delivered_this_claim"] == 12
        assert first["deliverable_windows"] == 30  # 现时授权 30 分钟 → 30 个窗口
        assert first["remaining_shards"] == 18
        # 单窗记录数：每个 1min 窗口恰好 1 条原始观测
        assert all(r["record_count"] == 1 for r in first["receipts"])

        # 实际交付范围从 10:00 开始，且首批只到 10:12
        assert first["receipts"][0]["window_start"] == f"{DAY}T10:00:00"
        assert first["receipts"][-1]["window_start"] == f"{DAY}T10:11:00"

        # 4) 中断后再次领取：在有效授权范围内只续发未领取分片
        second = export_svc.claim_shards(export_id, max_shards=10)
        assert second["delivered_this_claim"] == 10
        assert second["receipts"][0]["window_start"] == f"{DAY}T10:12:00"
        assert second["claimed_total"] == 22
        assert second["remaining_shards"] == 8

        # 5) 第三次领取剩余全部分片，任务转为已交付
        third = export_svc.claim_shards(export_id)
        assert third["delivered_this_claim"] == 8
        assert third["claimed_total"] == 30
        assert third["remaining_shards"] == 0

        from app.store import store
        task = store.find("series_export", export_id)
        assert task["status"] == "已交付"
        assert task["已领取分片"] == "30"
        assert task["授权窗口总数"] == "30"
        assert task["记录条数"] == "30"

        # 6) 已确认分片不会再次登记交付：重复领取零新增，凭据不重复
        repeat = export_svc.claim_shards(export_id)
        assert repeat["delivered_this_claim"] == 0
        assert repeat["claimed_total"] == 30
        receipts = export_svc.delivery_receipts(export_id)["receipts"]
        shard_ids = [r["shard_id"] for r in receipts]
        assert len(shard_ids) == len(set(shard_ids)) == 30

        # 实际交付范围：覆盖 10:00 至 10:30 的全部 30 个窗口，无一越界
        windows = sorted(r["window_start"] for r in receipts)
        assert windows[0] == f"{DAY}T10:00:00"
        assert windows[-1] == f"{DAY}T10:29:00"

        # 交付文件中只有授权内原始观测，且每条带实际采样时间
        delivery_file = setup_data_dir / "exports" / f"{task['导出编号']}_delivery.jsonl"
        delivered_records = [json.loads(line) for line in delivery_file.read_text(encoding="utf-8").splitlines()]
        assert len(delivered_records) == 30
        assert all(f"{DAY}T10:00:00" <= r["timestamp"] < f"{DAY}T10:30:00" for r in delivered_records)

        # 7) 对账：凭据校验值可由原始观测重算核对
        recon = export_svc.reconcile_export(export_id)
        assert recon["all_matched"] is True
        assert recon["window_count"] == 30
        assert all(w["matched"] and w["receipt_records"] == w["raw_records"] == 1
                   for w in recon["windows"])


class TestControlledDeliveryAcrossGrantBoundary:
    """场景二：跨越现时授权边界的请求，授权之外无观测、无汇总值。"""

    def test_no_observation_or_summary_outside_grant(self, provisioned, setup_data_dir):
        grant_svc = provisioned["grant_svc"]
        export_svc = provisioned["export_svc"]

        # 授权保持 10:00-10:20（不扩区）
        created = export_svc.create_controlled_export(
            POINT, LINE, f"{DAY}T10:20:00", f"{DAY}T10:40:00", "1min")
        export_id = created["entry_id"]

        # 请求整体落在授权之外：无有效授权覆盖该请求区间 → 拒绝交付
        result = export_svc.claim_shards(export_id)
        assert "error" in result
        receipts = export_svc.delivery_receipts(export_id)["receipts"]
        assert receipts == []
        assert not (setup_data_dir / "exports" / f"{created['export_id']}_delivery.jsonl").exists()

    def test_partial_overlap_only_delivers_inside_grant(self, provisioned, setup_data_dir):
        export_svc = provisioned["export_svc"]

        # 请求 10:10-10:40，与授权 10:00-10:20 的交集只有 10:10-10:20
        created = export_svc.create_controlled_export(
            POINT, LINE, f"{DAY}T10:10:00", f"{DAY}T10:40:00", "1min")
        result = export_svc.claim_shards(created["entry_id"])
        assert result["deliverable_windows"] == 10
        assert result["claimed_total"] == 10
        receipts = result["receipts"]
        assert receipts[0]["window_start"] == f"{DAY}T10:10:00"
        assert receipts[-1]["window_start"] == f"{DAY}T10:19:00"

        # 授权之外（10:20-10:40）没有任何交付记录，也没有任何窗口/汇总行可反推
        delivery_file = setup_data_dir / "exports" / f"{created['export_id']}_delivery.jsonl"
        delivered = [json.loads(line) for line in delivery_file.read_text(encoding="utf-8").splitlines()]
        assert all(r["timestamp"] < f"{DAY}T10:20:00" for r in delivered)
        assert len(delivered) == 10
        recon = export_svc.reconcile_export(created["entry_id"])
        assert all(w["window_end"] <= f"{DAY}T10:20:00" for w in recon["windows"])

    def test_finer_than_allowed_granularity_rejected(self, provisioned):
        export_svc = provisioned["export_svc"]
        created = export_svc.create_controlled_export(
            POINT, LINE, f"{DAY}T10:00:00", f"{DAY}T10:20:00", "30s")
        result = export_svc.claim_shards(created["entry_id"])
        assert "error" in result and "粒度" in result["error"]

    def test_no_grant_for_other_line_delivers_nothing(self, provisioned):
        export_svc = provisioned["export_svc"]
        created = export_svc.create_controlled_export(
            POINT, "LINE-OTHER", f"{DAY}T10:00:00", f"{DAY}T10:20:00", "1min")
        result = export_svc.claim_shards(created["entry_id"])
        assert "error" in result
        assert export_svc.delivery_receipts(created["entry_id"])["receipts"] == []
