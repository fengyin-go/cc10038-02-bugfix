"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestBatchService(BaseModuleService):
    MODULE = "ingest_batch"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "所属数据源", "采集时间窗口", "记录总数", "批次状态"]
    REQUIRED_FIELDS = ["批次编号", "所属数据源", "采集时间窗口"]
    STATUS_ORDER = ["待采集", "采集中", "已完成", "部分失败"]
    ACTION_RULES = {"启动采集": "采集中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []
