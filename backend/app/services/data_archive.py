"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataArchiveService(BaseModuleService):
    MODULE = "data_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["策略名称", "归档周期", "保留时长", "归档格式", "目标存储"]
    REQUIRED_FIELDS = ["策略名称", "归档周期", "保留时长"]
    STATUS_ORDER = ["待执行", "已执行", "已暂停"]
    ACTION_RULES = {"执行归档": "已执行", "暂停归档": "已暂停"}
    NEGATIVE_ACTIONS = []
