"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class SourceRegistryService(BaseModuleService):
    MODULE = "source_registry"
    ENTITY = "数据源"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["来源标识", "接入协议", "连接凭证", "采集区域", "状态"]
    REQUIRED_FIELDS = ["来源标识", "接入协议", "连接凭证"]
    STATUS_ORDER = ["待验证", "已验证", "已停用"]
    ACTION_RULES = {"验证连通": "已验证", "停用接入": "已停用"}
    NEGATIVE_ACTIONS = ["停用接入"]
