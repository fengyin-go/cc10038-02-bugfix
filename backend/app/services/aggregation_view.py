"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AggregationViewService(BaseModuleService):
    MODULE = "aggregation_view"
    ENTITY = "聚合视图"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["视图名称", "聚合维度", "指标定义", "数据源范围", "刷新间隔"]
    REQUIRED_FIELDS = ["视图名称", "聚合维度", "指标定义"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用视图": "已生效", "停用视图": "已停用"}
    NEGATIVE_ACTIONS = ["停用视图"]
