"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ParseJournalService(BaseModuleService):
    MODULE = "parse_journal"
    ENTITY = "解析日志"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["操作类型", "批次编号", "执行时间", "影响的记录数", "执行结果"]
    REQUIRED_FIELDS = ["操作类型", "批次编号", "执行时间"]
    STATUS_ORDER = ["成功", "失败", "部分成功"]
    ACTION_RULES = {"重新执行": "成功", "忽略异常": "失败"}
    NEGATIVE_ACTIONS = ["忽略异常"]
