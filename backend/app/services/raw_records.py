"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class RawRecordsService(BaseModuleService):
    MODULE = "raw_records"
    ENTITY = "日志记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "原始行号", "采集时间戳", "原始正文", "解析状态"]
    REQUIRED_FIELDS = ["批次编号", "原始行号", "采集时间戳"]
    STATUS_ORDER = ["待解析", "已解析", "解析失败"]
    ACTION_RULES = {"触发解析": "已解析", "标记忽略": "解析失败"}
    NEGATIVE_ACTIONS = []
