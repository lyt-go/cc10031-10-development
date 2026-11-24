"""共享测点受控交付接口：授权登记/更正、原始观测登记、受控导出、分片领取与对账。

这些固定路径必须在通用资源路由 /api/{module} 之前注册，避免被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.series_export import SeriesExportService
from app.services.shared_grant import SharedGrantService

router = APIRouter(prefix="/api", tags=["共享受控交付"])


class GrantPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class CorrectGrantPayload(BaseModel):
    new_start: str
    new_end: str
    new_granularity: str | None = None
    basis: str | None = None


class ObservationPayload(BaseModel):
    point: str
    observations: list[dict[str, Any]]


class ControlledExportPayload(BaseModel):
    point: str
    line: str
    start: str
    end: str
    granularity: str
    fmt: str = "jsonl"


class ClaimPayload(BaseModel):
    max_shards: int | None = None


def _unwrap(result: dict[str, Any]) -> dict[str, Any]:
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/shared-grants")
def register_grant(payload: GrantPayload) -> dict[str, Any]:
    return _unwrap(SharedGrantService().register_grant(payload.values))


@router.post("/shared-grants/{grant_id}/correct")
def correct_grant(grant_id: int, payload: CorrectGrantPayload) -> dict[str, Any]:
    return _unwrap(SharedGrantService().correct_grant(
        grant_id, payload.new_start, payload.new_end,
        payload.new_granularity, payload.basis))


@router.post("/raw-observations")
def register_raw_observations(payload: ObservationPayload) -> dict[str, Any]:
    try:
        return _unwrap(SeriesExportService().register_raw_observations(
            payload.point, payload.observations))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"原始观测时间格式非法：{exc}") from exc


@router.post("/controlled-exports")
def create_controlled_export(payload: ControlledExportPayload) -> dict[str, Any]:
    return _unwrap(SeriesExportService().create_controlled_export(
        payload.point, payload.line, payload.start, payload.end,
        payload.granularity, payload.fmt))


@router.post("/controlled-exports/{export_id}/claim")
def claim_shards(export_id: int, payload: ClaimPayload) -> dict[str, Any]:
    return _unwrap(SeriesExportService().claim_shards(export_id, payload.max_shards))


@router.get("/controlled-exports/{export_id}/receipts")
def delivery_receipts(export_id: int) -> dict[str, Any]:
    return _unwrap(SeriesExportService().delivery_receipts(export_id))


@router.get("/controlled-exports/{export_id}/reconcile")
def reconcile_export(export_id: int) -> dict[str, Any]:
    return _unwrap(SeriesExportService().reconcile_export(export_id))
