"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataLineageService(BaseModuleService):
    MODULE = "data_lineage"
    ENTITY = "血缘关系"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["来源批次", "解析规则版本", "标准化版本", "下游消费标识", "记录时间"]
    REQUIRED_FIELDS = ["来源批次", "解析规则版本", "标准化版本"]
    STATUS_ORDER = ["已记录", "待验证", "验证异常"]
    ACTION_RULES = {"验证血缘": "已记录", "标记断裂": "验证异常"}
    NEGATIVE_ACTIONS = []
