"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class WatermarkMgrService(BaseModuleService):
    MODULE = "watermark_mgr"
    ENTITY = "采集水位"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["数据源", "最后成功批次", "当前水位值", "水位类型", "更新时间"]
    REQUIRED_FIELDS = ["数据源", "最后成功批次", "当前水位值"]
    STATUS_ORDER = ["正常", "停滞", "异常回退"]
    ACTION_RULES = {"重置水位": "正常", "回滚水位": "正常"}
    NEGATIVE_ACTIONS = ["回滚水位"]
