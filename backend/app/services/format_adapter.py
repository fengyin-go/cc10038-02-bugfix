"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class FormatAdapterService(BaseModuleService):
    MODULE = "format_adapter"
    ENTITY = "格式适配"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["输入格式名", "适配器类型", "目标格式", "转换配置", "版本"]
    REQUIRED_FIELDS = ["输入格式名", "适配器类型", "目标格式"]
    STATUS_ORDER = ["草稿", "已启用", "已弃用"]
    ACTION_RULES = {"启用适配": "已启用", "弃用适配": "已弃用"}
    NEGATIVE_ACTIONS = []
