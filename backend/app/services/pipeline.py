"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from typing import Any

from app.store import store

# 去重改写与聚合刷新共用同一把锁：保证一次去重判定的写入和一次聚合读取
# 彼此串行，已发布的聚合结果不会混入两次判定的中间状态。
_PIPELINE_LOCK = threading.RLock()

# 去重明细表名（仅在内存仓库中使用，不进入前端业务模块清单）。
DEDUP_DETAIL_TABLE = "dedup_detail"
GLOBAL_DEDUP_SCOPE = "global"
# 明细判定结果取值。
VERDICT_RETAIN = "保留"
VERDICT_ELIMINATED = "淘汰"
VERDICT_RESTORED = "恢复"
# 去重运行与聚合视图的状态取值。
RUN_RUNNING = "运行中"
RUN_COMPLETED = "已完成"
VIEW_CONSUMABLE = "可消费"
VIEW_CANDIDATE = "候选中"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _load_fields(entity: dict[str, Any]) -> dict[str, Any]:
    """安全读取标准化明细中的字段集。"""
    try:
        data = json.loads(entity.get("标准化字段集", "{}"))
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _dedup_key(entity: dict[str, Any], key_fields: list[str]) -> str:
    fields_data = _load_fields(entity)
    return "||".join(str(fields_data.get(k, "")) for k in key_fields)


def _dedup_scopes(window: str) -> list[str]:
    """按窗口枚举去重判定范围。

    batch 窗口沿用批次取数逻辑：每条合格明细按自身批次进入独立范围，
    两个批次的同键日志落在不同范围、各自保留一条，互不淘汰。
    已经出过判定结果的批次即使当前记录全被淘汰，也要继续纳入，保证重跑
    （如切换保留策略）时能恢复被置换的记录。
    其它窗口（如 global）维持全局同一判定范围。
    """
    if str(window).strip().lower() == "batch":
        batches = {
            str(rec.get("来源批次") or "")
            for rec in store.rows("parsed_entity")
            if rec.get("status") == "校验通过"
        }
        batches |= {
            str(d.get("批次编号") or "")
            for d in store.rows(DEDUP_DETAIL_TABLE)
            if d.get("run_status") == RUN_COMPLETED and d.get("批次编号")
        }
        return sorted(b for b in batches if b)
    return [GLOBAL_DEDUP_SCOPE]


def _dedup_runs_for_scope(scope: str) -> list[dict[str, Any]]:
    """返回某判定范围下的去重明细（含历史各轮，按 run 与 id 排序）。"""
    details = store.rows(DEDUP_DETAIL_TABLE)
    rows = [d for d in details if d.get("dedup_scope") == scope]
    return sorted(rows, key=lambda d: (str(d.get("run_id")), int(d.get("id", 0))))


def _latest_completed_run(scope: str) -> str | None:
    runs = {d.get("run_id") for d in _dedup_runs_for_scope(scope)
            if d.get("run_status") == RUN_COMPLETED}
    return max(runs) if runs else None


def _running_scopes() -> set[str]:
    return {d.get("dedup_scope") for d in store.rows(DEDUP_DETAIL_TABLE)
            if d.get("run_status") == RUN_RUNNING}


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """对标准化明细执行去重，并按判定范围生成去重明细。

        - window=batch 时按「来源批次」拆分判定范围，同键日志只在本批内竞争，
          两个批次的同键记录各留一条，不会互相淘汰；兼容历史批次编号。
        - 每轮判定生成带 run_id 的去重明细（保留/淘汰/恢复），聚合视图只从
          明细中的胜出记录取数。
        - 保留策略改变后重跑时，上一轮被置换（淘汰）的记录会被恢复，历史明细
          仍保留可查。
        """
        with _PIPELINE_LOCK:
            dedup_strategies = store.rows("dedup_engine")
            parsed = store.rows("parsed_entity")
            details = store.rows(DEDUP_DETAIL_TABLE)

            is_batch_window = str(window).strip().lower() == "batch"
            # 兼容历史批次编号：只允许处理已经存在于标准化明细中的批次。
            scopes = _dedup_scopes(window)
            if is_batch_window and batch_no and batch_no not in scopes:
                return {
                    "ok": False,
                    "message": f"批次 {batch_no} 没有可去重的校验通过明细",
                    "strategy": "",
                    "dedup_key_fields": dedup_key_fields,
                    "duplicates_found": 0,
                    "unique_keys": 0,
                }
            if is_batch_window and batch_no:
                scopes = [batch_no]

            # 构建去重策略
            strategy_entry = {
                "id": _next_id("dedup_engine"),
                "策略名称": f"dedup-{_now_iso()}",
                "去重键字段": ",".join(dedup_key_fields),
                "去重窗口": window,
                "保留策略": retain_strategy,
                "生效数据源": "全部",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
            }
            dedup_strategies.append(strategy_entry)

            run_id = f"RUN-{_next_id(DEDUP_DETAIL_TABLE):05d}"
            total_duplicates = 0
            total_retained = 0
            total_restored = 0
            per_scope: dict[str, dict[str, Any]] = {}

            for scope in scopes:
                # 先恢复上一轮被本范围淘汰的记录：保留策略改变重跑时，旧淘汰者
                # （当前因去重被置为校验失败）重新参与本轮判定，历史 run 明细保留可查。
                prev_run = _latest_completed_run(scope)
                prev_loser_ids: set[int] = set()
                if prev_run:
                    prev_loser_ids = {
                        int(d.get("parsed_entity_id"))
                        for d in _dedup_runs_for_scope(scope)
                        if d.get("run_id") == prev_run
                        and d.get("判定结果") == VERDICT_ELIMINATED
                    }
                    for rec in parsed:
                        if (int(rec["id"]) in prev_loser_ids
                                and (not is_batch_window
                                     or str(rec.get("来源批次") or "") == scope)):
                            rec["status"] = "校验通过"
                            rec["校验状态"] = "校验通过"
                            rec["abnormal"] = False
                            rec["pending"] = False
                            rec.pop("dedup_id", None)
                            total_restored += 1

                # 候选 = 本范围当前校验通过的记录（含刚恢复的旧淘汰者）。
                # 真正质量校验失败的记录不会进入，去重只在合格记录间判定。
                candidates = [
                    rec for rec in parsed
                    if rec.get("status") == "校验通过"
                    and (not is_batch_window or str(rec.get("来源批次") or "") == scope)
                ]
                if not candidates:
                    continue

                # 先写入「运行中」占位明细：并发刷新时据此把该范围识别为仍在
                # 变化的候选，不产出可消费统计。
                for rec in candidates:
                    details.append({
                        "id": _next_id(DEDUP_DETAIL_TABLE),
                        "run_id": run_id,
                        "run_status": RUN_RUNNING,
                        "dedup_scope": scope,
                        "批次编号": scope if is_batch_window else "",
                        "去重键字段": ",".join(dedup_key_fields),
                        "dedup_key": _dedup_key(rec, dedup_key_fields),
                        "parsed_entity_id": rec["id"],
                        "来源批次": rec.get("来源批次", ""),
                        "保留策略": retain_strategy,
                        "判定结果": "",
                        "胜出记录ID": "",
                        "restored": int(rec["id"]) in prev_loser_ids,
                        "strategy_name": strategy_entry["策略名称"],
                        "created_at": _now_iso(),
                    })

                # 同键分组，组内按标准化明细 id 稳定排序，保证可重复判定。
                groups: dict[str, list[dict[str, Any]]] = {}
                for rec in candidates:
                    groups.setdefault(_dedup_key(rec, dedup_key_fields), []).append(rec)
                for members in groups.values():
                    members.sort(key=lambda r: int(r.get("id", 0)))

                scope_duplicates = 0
                scope_retained = 0
                run_details = [d for d in details if d.get("run_id") == run_id
                               and d.get("dedup_scope") == scope]
                detail_by_entity = {int(d["parsed_entity_id"]): d for d in run_details}

                for key, members in groups.items():
                    if retain_strategy == "keep_last":
                        winner = members[-1]
                    else:
                        winner = members[0]
                    for rec in members:
                        detail = detail_by_entity[int(rec["id"])]
                        if int(rec["id"]) == int(winner["id"]):
                            rec["status"] = "校验通过"
                            rec["校验状态"] = "校验通过"
                            rec["abnormal"] = False
                            rec["pending"] = False
                            rec["dedup_id"] = ""
                            detail["判定结果"] = VERDICT_RETAIN
                            detail["胜出记录ID"] = str(winner["id"])
                            scope_retained += 1
                        else:
                            rec["status"] = "校验失败"
                            rec["校验状态"] = "去重淘汰"
                            rec["abnormal"] = True
                            rec["dedup_id"] = str(winner["id"])
                            detail["判定结果"] = VERDICT_ELIMINATED
                            detail["胜出记录ID"] = str(winner["id"])
                            scope_duplicates += 1

                # 本轮完成，占位明细转为已提交；聚合只读取已完成 run。
                for detail in run_details:
                    detail["run_status"] = RUN_COMPLETED
                    detail["committed_at"] = _now_iso()

                per_scope[scope] = {
                    "candidates": len(candidates),
                    "retained": scope_retained,
                    "eliminated": scope_duplicates,
                    "unique_keys": len(groups),
                }
                total_duplicates += scope_duplicates
                total_retained += scope_retained

            return {
                "ok": True,
                "message": "去重完成",
                "strategy": strategy_entry["策略名称"],
                "dedup_key_fields": dedup_key_fields,
                "window": window,
                "retain_strategy": retain_strategy,
                "run_id": run_id,
                "duplicates_found": total_duplicates,
                "unique_keys": total_retained,
                "retained": total_retained,
                "restored": total_restored,
                "scopes": per_scope,
            }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    def validate_and_dedup_batch(
        self,
        batch_no: str,
        dedup_key_fields: list[str],
        retain_strategy: str = "keep_first",
        window: str = "batch",
    ) -> dict[str, Any]:
        """质量校验与按批去重在同一把锁内连续完成。

        新批次的迟到日志常在「去重结束时刚通过质量校验」，把两步合成一个临界区，
        并发刷新时外部只能看到「待判定候选」或「完整已提交 run」两种状态，
        不会读到校验已过、判定未落的中间态。
        """
        with _PIPELINE_LOCK:
            validation = self.validate_entities(batch_no)
            dedup = self.run_dedup(
                dedup_key_fields, window, retain_strategy, batch_no
            )
            return {"ok": True, "validation": validation, "dedup": dedup}

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """基于去重明细中的胜出日志刷新聚合视图。

        - 聚合只统计去重明细里「已完成」run 判定为保留的记录，每批计数与该批
          去重后应计条数一致；未经过去重的历史批次回退为直接读取校验通过明细。
        - 去重仍在运行中的批次只作为「候选中」暴露，不计入可消费统计。
        - 每次刷新生成不可变快照版本；刷新与去重并发时，已发布的统计只包含
          同一批的完整判定结果，不会混用两次判定。
        """
        with _PIPELINE_LOCK:
            parsed = store.rows("parsed_entity")
            entities_by_id = {int(rec["id"]): rec for rec in parsed}
            details = store.rows(DEDUP_DETAIL_TABLE)
            running = _running_scopes()

            # 已提交去重 run 覆盖的批次（global 窗口统一记在 global 范围）。
            scoped_batches = {
                str(d.get("批次编号") or "")
                for d in details
                if d.get("run_status") == RUN_COMPLETED and d.get("批次编号")
            }
            global_runs = [
                d for d in details
                if d.get("run_status") == RUN_COMPLETED
                and d.get("dedup_scope") == GLOBAL_DEDUP_SCOPE
            ]

            def _winner_ids_for_batch(scope_batch: str) -> set[int]:
                """取某批次在最新已完成 run 中的胜出记录 id。"""
                run_id = _latest_completed_run(scope_batch)
                if run_id is None:
                    return set()
                return {
                    int(d["parsed_entity_id"])
                    for d in _dedup_runs_for_scope(scope_batch)
                    if d.get("run_id") == run_id and d.get("判定结果") == VERDICT_RETAIN
                }

            # 枚举参与本次刷新的批次。
            all_batches = sorted({
                str(rec.get("来源批次") or "")
                for rec in parsed if rec.get("来源批次")
            })
            target_batches = [batch_no] if batch_no else all_batches

            results: dict[str, dict[str, Any]] = {}
            candidate_batches: list[str] = []
            stable_batches: list[str] = []
            consumed_runs: dict[str, str] = {}
            consumed_count = 0

            for scope_batch in target_batches:
                if scope_batch in running:
                    # 去重尚未提交：该批仍是变化中的候选，不进入可消费统计。
                    candidate_batches.append(scope_batch)
                    continue

                if scope_batch in scoped_batches:
                    # 按批去重：只读取最新已完成 run 的胜出明细。
                    winner_ids = _winner_ids_for_batch(scope_batch)
                    run_id = _latest_completed_run(scope_batch)
                    if run_id:
                        consumed_runs[scope_batch] = run_id
                    source_entities = [
                        entities_by_id[eid] for eid in winner_ids
                        if eid in entities_by_id
                    ]
                    stable_batches.append(scope_batch)
                elif global_runs:
                    # 全局窗口去重：从全局 run 的胜出者中筛本批记录。
                    run_id = max({d["run_id"] for d in global_runs})
                    winner_ids = {
                        int(d["parsed_entity_id"])
                        for d in global_runs
                        if d.get("run_id") == run_id and d.get("判定结果") == VERDICT_RETAIN
                    }
                    consumed_runs[scope_batch] = run_id
                    source_entities = [
                        entities_by_id[eid] for eid in winner_ids
                        if eid in entities_by_id
                        and str(entities_by_id[eid].get("来源批次") or "") == scope_batch
                    ]
                    stable_batches.append(scope_batch)
                else:
                    # 历史批次未经过去重：沿用批次取数逻辑直接消费校验通过明细。
                    source_entities = [
                        rec for rec in parsed
                        if str(rec.get("来源批次") or "") == scope_batch
                        and rec.get("status") == "校验通过"
                    ]

                for entity in source_entities:
                    fields_data = _load_fields(entity)
                    dim_val = str(fields_data.get(dim_field, "unknown"))
                    metric_val = fields_data.get(metric_field, "0")
                    bucket = results.setdefault(dim_val, {"count": 0, "sum": 0.0, "values": []})
                    bucket["count"] += 1
                    try:
                        bucket["sum"] += float(metric_val) if metric_val else 0.0
                    except (ValueError, TypeError):
                        pass
                    bucket["values"].append(metric_val)
                    consumed_count += 1

            is_stable = not candidate_batches
            snapshot = copy.deepcopy(results)
            aggs = store.rows("aggregation_view")
            version = sum(1 for a in aggs if a.get("视图名称") == view_name) + 1
            agg_entry = {
                "id": _next_id("aggregation_view"),
                "视图名称": view_name,
                "聚合维度": dim_field,
                "指标定义": f"{agg_func}({metric_field})",
                "数据源范围": "去重明细" if (scoped_batches or global_runs) else "标准化明细",
                "刷新间隔": "manual",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
                "results": snapshot,
                "refreshed_at": _now_iso(),
                "source_records": consumed_count,
                # 并发与版本控制所需元数据。
                "snapshot_version": version,
                "view_status": VIEW_CONSUMABLE if is_stable else VIEW_CANDIDATE,
                "batch_no": batch_no or "",
                "stable_batches": json.dumps(stable_batches, ensure_ascii=False),
                "candidate_batches": json.dumps(candidate_batches, ensure_ascii=False),
                "consumed_runs": json.dumps(consumed_runs, ensure_ascii=False),
                "immutable": True,
            }
            aggs.append(agg_entry)

            return {
                "ok": True,
                "message": (
                    f"聚合视图 {view_name} 已刷新"
                    if is_stable
                    else f"聚合视图 {view_name} 含候选批次，候选未计入可消费统计"
                ),
                "view": agg_entry,
                "dimensions": len(results),
                "view_status": agg_entry["view_status"],
                "snapshot_version": version,
                "consumed_count": consumed_count,
                "stable_batches": stable_batches,
                "candidate_batches": candidate_batches,
                "consumed_runs": consumed_runs,
            }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重（按当前批次取数，不与历史批次互相淘汰）
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first", batch_no)
        steps["dedup"] = dedup_result

        # S5: 聚合（只从本批去重明细的胜出记录取数）
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count", batch_no)
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()