"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class SchemaVersionService(BaseModuleService):
    MODULE = "schema_version"
    ENTITY = "Schema版本"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["版本号", "适用数据源", "字段定义", "向后兼容标记", "发布说明"]
    REQUIRED_FIELDS = ["版本号", "适用数据源", "字段定义"]
    STATUS_ORDER = ["草稿", "已发布", "已过时"]
    ACTION_RULES = {"发布版本": "已发布", "标记过时": "已过时"}
    NEGATIVE_ACTIONS = []
