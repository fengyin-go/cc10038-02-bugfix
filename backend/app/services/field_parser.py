"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class FieldParserService(BaseModuleService):
    MODULE = "field_parser"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则名称", "适用数据源", "输入格式", "输出字段列表", "解析引擎"]
    REQUIRED_FIELDS = ["规则名称", "适用数据源", "输入格式"]
    STATUS_ORDER = ["草稿", "已发布", "已弃用"]
    ACTION_RULES = {"发布规则": "已发布", "弃用规则": "已弃用"}
    NEGATIVE_ACTIONS = []
