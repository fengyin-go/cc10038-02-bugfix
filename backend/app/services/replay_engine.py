"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "重放任务"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["历史批次范围", "新解析规则版本", "重放状态", "产出记录数", "执行时间"]
    REQUIRED_FIELDS = ["历史批次范围", "新解析规则版本", "重放状态"]
    STATUS_ORDER = ["待重放", "重放中", "已完成", "部分失败"]
    ACTION_RULES = {"创建重放": "重放中", "取消重放": "部分失败"}
    NEGATIVE_ACTIONS = []
