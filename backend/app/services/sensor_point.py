"""测点台账领域服务：维护工业传感器测点的量程、采样频率与计量单位台账登记。
当测点投运后自动生成边缘缓冲种子数据，存储到测点采集配置文件。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SensorPointService(BaseModuleService):
    MODULE = "sensor_point"
    ENTITY = "测点档案"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["测点编号", "所属产线", "传感器型号", "量程范围", "采样频率", "计量单位", "投运日期", "台账版本",
                   "共享标记", "共享起始时间", "共享截止时间", "允许使用粒度"]
    REQUIRED_FIELDS = ["测点编号", "所属产线", "传感器型号"]
    STATUS_ORDER = ["草稿", "待校验", "已投运", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认投运": "已投运", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = []

    def onboard_sensor(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记新测点并初始化台账配置数据集，写入测点采集配置文件。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        # 数据采集操作：登记测点台账配置数据集并写入测点采集文件
        point_key = entry["测点编号"]
        self._write_checkpoint(point_key, {
            "point_key": point_key,
            "采样频率": entry.get("采样频率", "1Hz"),
            "量程范围": entry.get("量程范围", "0-100"),
            "计量单位": entry.get("计量单位", "V"),
            "status": "草稿",
            "created_at": time.time(),
        })
        # 将测点台账记录写入持久化数据文件
        self._write_jsonl(
            self._data_dir("sensor_points") / "point_ledger.jsonl",
            [{"action": "register", "point_key": point_key, "产线": entry["所属产线"], "at": time.time()}]
        )
        return entry

    def activate_point(self, point_id: int) -> dict[str, Any]:
        """确认测点投运并自动创建配套边缘缓冲种子数据与采集心跳记录。"""
        entry, msg = self.run_action(point_id, "确认投运")
        if entry is None:
            return {"error": msg, "point_id": point_id}
        point_key = entry["测点编号"]
        triggers = {}
        # 自动创建边缘缓冲分片种子数据
        try:
            buffer_svc = self._safe_import("app.services.edge_buffer", "EdgeBufferService")()
            buf_entry = self._auto_create_seed("edge_buffer", {
                "分片编号": f"EDGE-{point_key}",
                "测点编号": point_key,
                "起始时刻": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "结束时刻": "",
                "采样点数": "0",
                # 数据产物输出：写入投运事件日志到台账数据文件
                "缓冲字节": "0",
                "落盘方式": "定时落盘",
                "分片水位": "0",
            })
            if buf_entry:
                triggers["edge_buffer"] = {"created": buf_entry["分片编号"], "status": "已生效"}
        except Exception as e:
            triggers["edge_buffer"] = {"error": str(e)}
        # 自动创建采集心跳种子数据
        try:
            health_svc = self._safe_import("app.services.device_health", "DeviceHealthService")()
            hb_entry = self._auto_create_seed("device_health", {
                "心跳编号": f"DEVI-{point_key}",
                "采集终端": point_key,
                "上报时刻": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "间隔秒数": "30",
                "丢包次数": "0",
                "缓存积压": "0",
                "判定阈值": "120",
                "心跳状态": "正常",
            })
            if hb_entry:
                triggers["device_health"] = {"created": hb_entry["心跳编号"], "status": "正常"}
        except Exception as e:
            triggers["device_health"] = {"error": str(e)}
        # 写入投运事件日志
        self._write_jsonl(
            self._data_dir("sensor_points") / "point_ledger.jsonl",
            [{"action": "activate", "point_key": point_key, "at": time.time()}]
        )
        return {
            "point_entry": entry,
            "point_key": point_key,
            "activated_at": time.time(),
            "auto_triggers": triggers,
        }

    def mark_shared(self, point_key: str, *, start: str, end: str,
                    granularity: str, line: str, version: str) -> dict[str, Any]:
        """在测点台账标出共享起止时间与允许使用粒度（受控共享授权回写）。"""
        target = next((row for row in store.rows(self.MODULE)
                       if row.get("测点编号") == point_key), None)
        if target is None:
            # 授权先于台账登记到达时，落一条占位档案，保证共享标记可被列表与对账看到
            target = {
                "id": max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1,
                "测点编号": point_key,
                "所属产线": line,
                "传感器型号": "",
                "status": "已投运",
                "pending": False,
                "abnormal": False,
            }
            store.rows(self.MODULE).append(target)
        target["共享标记"] = f"受控共享（{line}）"
        target["共享起始时间"] = start
        target["共享截止时间"] = end
        target["允许使用粒度"] = granularity
        target["共享授权版本"] = version
        # 台账留痕：共享标记变更写入测点台账日志文件
        self._write_jsonl(
            self._data_dir("sensor_points") / "point_ledger.jsonl",
            [{"action": "mark_shared", "point_key": point_key, "line": line,
              "shared_start": start, "shared_end": end,
              "granularity": granularity, "version": version, "at": time.time()}],
        )
        return {"point_key": point_key, "shared": True, "version": version}