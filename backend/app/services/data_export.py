"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DataExportService(BaseModuleService):
    MODULE = "data_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["导出范围", "目标格式", "过滤条件", "导出状态", "文件路径"]
    REQUIRED_FIELDS = ["导出范围", "目标格式", "过滤条件"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"开始导出": "执行中", "重试导出": "执行中"}
    NEGATIVE_ACTIONS = []
