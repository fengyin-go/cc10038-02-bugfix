"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class RpaWorkflowService(BaseModuleService):
    MODULE = "rpa_workflow"
    ENTITY = "采集流程"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["流程名称", "目标系统", "触发方式", "步骤定义", "最后执行状态"]
    REQUIRED_FIELDS = ["流程名称", "目标系统", "触发方式"]
    STATUS_ORDER = ["待激活", "已激活", "已停用", "异常中断"]
    ACTION_RULES = {"激活流程": "已激活", "停用流程": "已停用"}
    NEGATIVE_ACTIONS = ["停用流程"]
