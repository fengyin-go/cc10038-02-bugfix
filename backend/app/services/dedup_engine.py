"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DedupEngineService(BaseModuleService):
    MODULE = "dedup_engine"
    ENTITY = "去重策略"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["策略名称", "去重键字段", "去重窗口", "保留策略", "生效数据源"]
    REQUIRED_FIELDS = ["策略名称", "去重键字段", "去重窗口"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]
