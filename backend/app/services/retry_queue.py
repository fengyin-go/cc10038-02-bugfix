"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class RetryQueueService(BaseModuleService):
    MODULE = "retry_queue"
    ENTITY = "重试任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["失败记录ID", "失败原因", "重试次数", "最大重试", "下次重试时间"]
    REQUIRED_FIELDS = ["失败记录ID", "失败原因", "重试次数"]
    STATUS_ORDER = ["待重试", "重试中", "超过上限"]
    ACTION_RULES = {"立即重试": "重试中", "放弃重试": "超过上限"}
    NEGATIVE_ACTIONS = []
