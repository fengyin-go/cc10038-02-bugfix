"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AlertPolicyService(BaseModuleService):
    MODULE = "alert_policy"
    ENTITY = "告警策略"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["策略名称", "监控指标", "告警阈值", "通知方式", "生效范围"]
    REQUIRED_FIELDS = ["策略名称", "监控指标", "告警阈值"]
    STATUS_ORDER = ["待启用", "已启用", "已关闭"]
    ACTION_RULES = {"启用策略": "已启用", "关闭策略": "已关闭"}
    NEGATIVE_ACTIONS = []
