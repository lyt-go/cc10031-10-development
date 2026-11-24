"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import controlled_delivery, resource

# 受控交付固定路径优先注册，避免被通用 /api/{module} 动态路径抢先匹配。
ROUTERS = [controlled_delivery, resource]
