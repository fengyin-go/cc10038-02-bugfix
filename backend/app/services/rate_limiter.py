"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class RateLimiterService(BaseModuleService):
    MODULE = "rate_limiter"
    ENTITY = "限流规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["数据源", "每秒上限", "突发并发", "限流动作", "生效时段"]
    REQUIRED_FIELDS = ["数据源", "每秒上限", "突发并发"]
    STATUS_ORDER = ["待生效", "已生效", "已过期"]
    ACTION_RULES = {"启用限流": "已生效", "停用限流": "已过期"}
    NEGATIVE_ACTIONS = ["停用限流"]
