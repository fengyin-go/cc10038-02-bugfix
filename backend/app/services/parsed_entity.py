"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ParsedEntityService(BaseModuleService):
    MODULE = "parsed_entity"
    ENTITY = "标准化记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["来源批次", "原始记录ID", "解析规则版本", "标准化字段集", "校验状态"]
    REQUIRED_FIELDS = ["来源批次", "原始记录ID", "解析规则版本"]
    STATUS_ORDER = ["待校验", "校验通过", "校验失败"]
    ACTION_RULES = {"执行校验": "校验通过", "手动修正": "校验通过"}
    NEGATIVE_ACTIONS = []
