"""按批去重与聚合快照的业务行为测试。

覆盖题述场景：
1. 去重窗口选 batch 时，两个采集批次的同键日志各自判定、各留一条，互不淘汰；
2. 去重按批生成明细，聚合视图只从明细中的胜出记录取数并计算该批计数；
3. 保留策略改变重跑时，被置换的记录可恢复、历史明细仍可查；
4. 新批次校验/去重与聚合刷新并发交叠时，上一批已发布统计与明细不被改动，
   运行中的批次只作为「候选中」暴露，不混入可消费统计。
"""
from __future__ import annotations

import json
import threading

import pytest

from app.services.pipeline import (
    DEDUP_DETAIL_TABLE,
    RUN_COMPLETED,
    RUN_RUNNING,
    VERDICT_ELIMINATED,
    VERDICT_RETAIN,
    VIEW_CANDIDATE,
    VIEW_CONSUMABLE,
)
from app.store import store


def _prepare_source(pipeline, source_id: str) -> None:
    pipeline.register_source(source_id, "http", "default")
    pipeline.validate_source(source_id)


def _ingest_parse_validate(pipeline, source_id: str, logs: list[dict]) -> str:
    """启动批次并走完解析、质量校验，返回批次编号。

    解析成功会把批次置为「已完成」，从而允许同源启动下一个批次。
    """
    batch = pipeline.start_ingest_batch(source_id, "2026Q1", logs)
    assert batch["ok"], batch["message"]
    batch_no = batch["batch"]["批次编号"]
    pipeline.parse_batch(batch_no)
    pipeline.validate_entities(batch_no)
    return batch_no


def _details_for_scope(scope: str) -> list[dict]:
    return [d for d in store.rows(DEDUP_DETAIL_TABLE) if d.get("dedup_scope") == scope]


def _winners_of_latest_run(scope: str) -> set[int]:
    rows = _details_for_scope(scope)
    completed_runs = {d["run_id"] for d in rows if d["run_status"] == RUN_COMPLETED}
    latest = max(completed_runs)
    return {
        int(d["parsed_entity_id"])
        for d in rows
        if d["run_id"] == latest and d["判定结果"] == VERDICT_RETAIN
    }


@pytest.fixture
def two_batches_with_same_key(pipeline):
    """两个批次各放两条同键（level 相同）日志。"""
    _prepare_source(pipeline, "batch-dedup-src")
    batch_a = _ingest_parse_validate(pipeline, "batch-dedup-src", [
        {"body": "t1|INFO|a-first"},
        {"body": "t2|INFO|a-second"},
    ])
    batch_b = _ingest_parse_validate(pipeline, "batch-dedup-src", [
        {"body": "t3|INFO|b-first"},
        {"body": "t4|INFO|b-second"},
    ])
    return batch_a, batch_b


class TestBatchScopedDedup:
    """按批去重：跨批次同键不互相淘汰。"""

    def test_two_batches_same_key_each_keeps_one(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        result = pipeline.run_dedup(["level"], "batch", "keep_first")

        assert result["ok"] is True
        # 两个目标批次各淘汰一条、各保留一条（种子历史批次不在核对范围内）。
        assert result["scopes"][batch_a]["eliminated"] == 1
        assert result["scopes"][batch_b]["eliminated"] == 1
        assert result["scopes"][batch_a]["retained"] == 1
        assert result["scopes"][batch_b]["retained"] == 1
        assert {batch_a, batch_b}.issubset(result["scopes"])

        parsed = store.rows("parsed_entity")
        for scope in (batch_a, batch_b):
            batch_records = [p for p in parsed if p.get("来源批次") == scope]
            passed = [p for p in batch_records if p["status"] == "校验通过"]
            failed = [p for p in batch_records if p["status"] == "校验失败"]
            assert len(passed) == 1
            assert len(failed) == 1
            # 本批保留者必须来自本批，不能被另一批的同键记录淘汰。
            assert passed[0]["来源批次"] == scope

    def test_dedup_details_generated_per_batch(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        result = pipeline.run_dedup(["level"], "batch", "keep_first")
        run_id = result["run_id"]

        for scope in (batch_a, batch_b):
            rows = _details_for_scope(scope)
            # 同一轮 run 给每个范围各生成两条明细。
            assert len(rows) == 2
            assert {d["run_id"] for d in rows} == {run_id}
            assert {d["判定结果"] for d in rows} == {VERDICT_RETAIN, VERDICT_ELIMINATED}
            assert all(d["run_status"] == RUN_COMPLETED for d in rows)
            retain = next(d for d in rows if d["判定结果"] == VERDICT_RETAIN)
            loser = next(d for d in rows if d["判定结果"] == VERDICT_ELIMINATED)
            # 淘汰明细指向本批胜出者。
            assert str(loser["胜出记录ID"]) == str(retain["parsed_entity_id"])

    def test_keep_first_picks_first_record_of_each_batch(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        pipeline.run_dedup(["level"], "batch", "keep_first")

        parsed = store.rows("parsed_entity")
        for scope in (batch_a, batch_b):
            ids = sorted(int(p["id"]) for p in parsed if p.get("来源批次") == scope)
            assert _winners_of_latest_run(scope) == {ids[0]}

    def test_explicit_historical_batch_number_supported(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key

        # 显式按历史批次编号运行，只处理该批。
        result = pipeline.run_dedup(["level"], "batch", "keep_first", batch_a)
        assert result["ok"] is True
        assert set(result["scopes"]) == {batch_a}
        details = store.rows(DEDUP_DETAIL_TABLE)
        assert {d["dedup_scope"] for d in details} == {batch_a}

        # 第二批次未被第一轮去重触碰，随后单独运行仍各留一条。
        result_b = pipeline.run_dedup(["level"], "batch", "keep_first", batch_b)
        assert result_b["ok"] is True
        assert _winners_of_latest_run(batch_a)
        assert _winners_of_latest_run(batch_b)
        # 两批胜出者是不同记录。
        assert _winners_of_latest_run(batch_a).isdisjoint(_winners_of_latest_run(batch_b))

    def test_unknown_batch_no_rejected_without_side_effect(self, pipeline, two_batches_with_same_key):
        before = len(store.rows("dedup_engine"))
        result = pipeline.run_dedup(["level"], "batch", "keep_first", "INGE-9999")
        assert result["ok"] is False
        # 校验前置，失败时不落策略、不生明细。
        assert len(store.rows("dedup_engine")) == before
        assert store.rows(DEDUP_DETAIL_TABLE) == []

    def test_global_window_still_eliminates_across_batches(self, pipeline, two_batches_with_same_key):
        # 非 batch 窗口维持全局判定：两个目标批次的 4 条同键记录只留一条，
        # 另有 1 条种子历史明细独立成组，因此全局共 2 个胜出者。
        result = pipeline.run_dedup(["level"], "global", "keep_first")
        scope = result["scopes"]["global"]
        assert scope["eliminated"] == 3
        assert scope["retained"] == 2


class TestAggregationReadsDedupDetails:
    """聚合视图从去重明细读取胜出日志并按批计数。"""

    def test_aggregation_counts_only_winners_per_batch(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        dedup = pipeline.run_dedup(["level"], "batch", "keep_first")

        view_a = pipeline.refresh_aggregation("view-a", "level", "time", "count", batch_a)
        assert view_a["view_status"] == VIEW_CONSUMABLE
        assert view_a["candidate_batches"] == []
        assert view_a["stable_batches"] == [batch_a]
        # 本批去重后只剩 1 条胜出记录。
        assert view_a["view"]["results"]["INFO"]["count"] == 1
        assert view_a["consumed_count"] == 1
        assert view_a["consumed_runs"][batch_a] == dedup["run_id"]

        view_b = pipeline.refresh_aggregation("view-b", "level", "time", "count", batch_b)
        assert view_b["view"]["results"]["INFO"]["count"] == 1

        # 不带批次的全量刷新：两个目标批次各贡献一个胜出者（INFO 共 2 条），
        # 种子历史明细在独立批次按其维度单独成桶。
        view_all = pipeline.refresh_aggregation("view-all", "level", "time", "count")
        assert view_all["view"]["results"]["INFO"]["count"] == 2
        # consumed_count 只统计两个目标批次时应为 2。
        target_only = pipeline.refresh_aggregation(
            "view-target", "level", "time", "count", batch_a
        )
        target_only_b = pipeline.refresh_aggregation(
            "view-target", "level", "time", "count", batch_b
        )
        assert target_only["consumed_count"] == 1
        assert target_only_b["consumed_count"] == 1
        assert batch_a in view_all["stable_batches"]
        assert batch_b in view_all["stable_batches"]

    def test_aggregation_matches_detail_retained_count(self, pipeline, two_batches_with_same_key):
        batch_a, _ = two_batches_with_same_key
        pipeline.run_dedup(["level"], "batch", "keep_first")
        view = pipeline.refresh_aggregation("view-x", "level", "time", "count", batch_a)

        retained = sum(
            1 for d in _details_for_scope(batch_a)
            if d["run_status"] == RUN_COMPLETED and d["判定结果"] == VERDICT_RETAIN
        )
        total_count = sum(bucket["count"] for bucket in view["view"]["results"].values())
        assert total_count == retained

    def test_published_view_snapshot_is_immutable(self, pipeline, two_batches_with_same_key):
        batch_a, _ = two_batches_with_same_key
        pipeline.run_dedup(["level"], "batch", "keep_first")
        first = pipeline.refresh_aggregation("view-snap", "level", "time", "count", batch_a)
        snapshot = json.loads(json.dumps(first["view"]["results"]))
        first_run = dict(first["consumed_runs"])

        # 再次刷新只追加新版本，已发布快照对象不被改写。
        second = pipeline.refresh_aggregation("view-snap", "level", "time", "count", batch_a)
        assert second["snapshot_version"] == first["snapshot_version"] + 1
        assert first["view"]["results"] == snapshot
        assert first["view"]["consumed_runs"] == json.dumps(first_run, ensure_ascii=False)


class TestRetainStrategyChange:
    """保留策略改变时被置换记录可恢复、历史明细可查。"""

    def test_switch_keep_first_to_keep_last_restores_losers(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        first = pipeline.run_dedup(["level"], "batch", "keep_first")
        first_winners = {scope: set(_winners_of_latest_run(scope))
                         for scope in (batch_a, batch_b)}

        second = pipeline.run_dedup(["level"], "batch", "keep_last")
        # 上一轮两个淘汰者都被恢复后重新参与判定。
        assert second["restored"] == 2
        assert second["duplicates_found"] == 2

        parsed = store.rows("parsed_entity")
        for scope in (batch_a, batch_b):
            ids = sorted(int(p["id"]) for p in parsed if p.get("来源批次") == scope)
            # 保留者从首批首条切换为本批末条。
            assert _winners_of_latest_run(scope) == {ids[-1]}
            assert _winners_of_latest_run(scope) != first_winners[scope]
            # 旧淘汰者恢复后又按新规则重新判定，当前只剩新淘汰者异常。
            statuses = [p["status"] for p in parsed if p.get("来源批次") == scope]
            assert statuses.count("校验通过") == 1
            assert statuses.count("校验失败") == 1

    def test_history_run_details_remain_queryable(self, pipeline, two_batches_with_same_key):
        batch_a, _ = two_batches_with_same_key
        first = pipeline.run_dedup(["level"], "batch", "keep_first")
        second = pipeline.run_dedup(["level"], "batch", "keep_last")

        rows = _details_for_scope(batch_a)
        run_ids = {d["run_id"] for d in rows}
        assert run_ids == {first["run_id"], second["run_id"]}
        # 第一轮明细原样保留，仍能查回当时被置换的记录。
        first_rows = [d for d in rows if d["run_id"] == first["run_id"]]
        assert {d["判定结果"] for d in first_rows} == {VERDICT_RETAIN, VERDICT_ELIMINATED}
        # 聚合只认最新已完成 run 的胜出者。
        view = pipeline.refresh_aggregation("view-h", "level", "time", "count", batch_a)
        assert view["consumed_runs"][batch_a] == second["run_id"]
        assert view["view"]["results"]["INFO"]["count"] == 1


class TestRefreshDedupConcurrency:
    """刷新与去重并发：已发布统计不混用两次判定，候选与可消费可区分。"""

    def test_running_scope_is_exposed_as_candidate(self, pipeline, two_batches_with_same_key):
        batch_a, batch_b = two_batches_with_same_key
        pipeline.run_dedup(["level"], "batch", "keep_first")

        # 模拟新批次去重进行中：写入运行中占位明细。
        new_batch = _ingest_parse_validate(pipeline, "batch-dedup-src", [
            {"body": "t5|INFO|c-first"},
            {"body": "t6|INFO|c-second"},
        ])
        running_row = {
            "id": 900001,
            "run_id": "RUN-RUNNING",
            "run_status": RUN_RUNNING,
            "dedup_scope": new_batch,
            "批次编号": new_batch,
            "去重键字段": "level",
            "dedup_key": "INFO",
            "parsed_entity_id": 999999,
            "来源批次": new_batch,
            "保留策略": "keep_first",
            "判定结果": "",
            "胜出记录ID": "",
            "strategy_name": "dedup-running",
            "created_at": "2026-11-21T00:00:00",
        }
        store.rows(DEDUP_DETAIL_TABLE).append(running_row)

        view = pipeline.refresh_aggregation("view-c", "level", "time", "count")
        assert view["view_status"] == VIEW_CANDIDATE
        assert new_batch in view["candidate_batches"]
        assert batch_a in view["stable_batches"]
        # 候选批次不计入可消费统计，两个已完成批次的 INFO 胜出者照常计数。
        assert view["view"]["results"]["INFO"]["count"] == 2
        target_stable = view["consumed_count"]
        # 种子历史批次独立计数，不影响目标批次核对。
        assert target_stable >= 2

        # 运行结束并正式去重后，刷新变为可消费。
        store.rows(DEDUP_DETAIL_TABLE).remove(running_row)
        pipeline.run_dedup(["level"], "batch", "keep_first", new_batch)
        settled = pipeline.refresh_aggregation("view-c", "level", "time", "count")
        assert settled["view_status"] == VIEW_CONSUMABLE
        assert settled["candidate_batches"] == []
        # 新批去重后加入 1 条 INFO 胜出者。
        assert settled["view"]["results"]["INFO"]["count"] == 3

    def test_concurrent_validate_dedup_and_refresh_keeps_published_stats(
        self, pipeline, two_batches_with_same_key
    ):
        batch_a, batch_b = two_batches_with_same_key
        dedup = pipeline.run_dedup(["level"], "batch", "keep_first")
        published = pipeline.refresh_aggregation(
            "view-pub", "level", "time", "count", batch_a
        )
        published_results = json.loads(json.dumps(published["view"]["results"]))
        published_run = published["consumed_runs"][batch_a]
        details_a_before = sorted(
            (d["run_id"], int(d["parsed_entity_id"]), d["判定结果"])
            for d in _details_for_scope(batch_a)
        )

        # 新批次：迟到日志刚解析完，校验与去重要和刷新交叠执行。
        new_batch = _ingest_parse_validate(pipeline, "batch-dedup-src", [
            {"body": "t7|WARN|c-first"},
            {"body": "t8|WARN|c-second"},
        ])
        # 把新批明细改回待校验，模拟「去重结束时刚通过质量校验」的交叠窗口。
        for rec in store.rows("parsed_entity"):
            if rec.get("来源批次") == new_batch:
                rec["status"] = "待校验"

        barrier = threading.Barrier(3)
        errors: list[Exception] = []

        def validate_and_dedup() -> None:
            try:
                barrier.wait()
                # 校验与去重原子完成：刷新线程看不到中间态。
                pipeline.validate_and_dedup_batch(new_batch, ["level"], "keep_first")
            except Exception as exc:  # pragma: no cover - 并发失败时留痕
                errors.append(exc)

        def refresh_old_batch() -> None:
            try:
                barrier.wait()
                for _ in range(5):
                    pipeline.refresh_aggregation(
                        "view-pub", "level", "time", "count", batch_a
                    )
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def refresh_all() -> None:
            try:
                barrier.wait()
                for _ in range(5):
                    pipeline.refresh_aggregation("view-all", "level", "time", "count")
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [
            threading.Thread(target=validate_and_dedup),
            threading.Thread(target=refresh_old_batch),
            threading.Thread(target=refresh_all),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

        # 没有遗留运行中的判定。
        assert all(d["run_status"] == RUN_COMPLETED
                   for d in store.rows(DEDUP_DETAIL_TABLE))

        # 上一批已发布统计与明细都没有被改动。
        assert published["view"]["results"] == published_results
        assert published["view"]["view_status"] == VIEW_CONSUMABLE
        latest_a = pipeline.refresh_aggregation(
            "view-pub", "level", "time", "count", batch_a
        )
        assert latest_a["consumed_runs"][batch_a] == published_run
        assert latest_a["view"]["results"] == published_results
        details_a_after = sorted(
            (d["run_id"], int(d["parsed_entity_id"]), d["判定结果"])
            for d in _details_for_scope(batch_a)
        )
        assert details_a_after == details_a_before

        # 新批次独立判定、各留一条，且聚合按同一批完整结果计数。
        assert _winners_of_latest_run(new_batch)
        new_view = pipeline.refresh_aggregation(
            "view-new", "level", "time", "count", new_batch
        )
        assert new_view["view_status"] == VIEW_CONSUMABLE
        assert new_view["view"]["results"]["WARN"]["count"] == 1

        # 每个已发布视图引用的 run 都是完整已提交 run，计数与明细保留数一致。
        for view_row in store.rows("aggregation_view"):
            consumed = json.loads(view_row.get("consumed_runs", "{}"))
            for scope, run_id in consumed.items():
                scope_rows = [
                    d for d in _details_for_scope(scope)
                    if d["run_id"] == run_id
                ]
                assert scope_rows
                assert all(d["run_status"] == RUN_COMPLETED for d in scope_rows)
