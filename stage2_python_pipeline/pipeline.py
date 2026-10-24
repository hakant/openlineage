#!/usr/bin/env python
"""Stage 2: a tiny pandas pipeline that emits OpenLineage events with openlineage-python.

Pipeline (one parent run "daily_pipeline" with three child jobs):

    data/source/orders_export.csv
        --extract_orders-->      data/raw/orders.csv
        --clean_orders-->        data/clean/orders.csv
        --build_daily_revenue--> data/marts/daily_revenue.csv

Run it from the repository root (so openlineage.yml is found):

    uv run python stage2_python_pipeline/pipeline.py                      # happy path
    uv run python stage2_python_pipeline/pipeline.py --fail-at clean_orders
    uv run python stage2_python_pipeline/pipeline.py --schema-change      # new column -> new dataset version
    uv run python stage2_python_pipeline/pipeline.py --transport console  # print events, no Marquez needed
    uv run python stage2_python_pipeline/pipeline.py --transport file     # append events to events.jsonl
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    column_lineage_dataset,
    data_quality_metrics_input_dataset,
    documentation_dataset,
    error_message_run,
    job_type_job,
    output_statistics_output_dataset,
    parent_run,
    schema_dataset,
)
from openlineage.client.uuid import generate_new_uuid

PRODUCER = "https://github.com/example/openlineage-marquez-poc/stage2"
JOB_NAMESPACE = "poc_python_pipeline"
DATASET_NAMESPACE = "file://openlineage-poc"
DATA_DIR = Path(__file__).parent / "data"

SOURCE = "data/source/orders_export.csv"
RAW = "data/raw/orders.csv"
CLEAN = "data/clean/orders.csv"
MARTS = "data/marts/daily_revenue.csv"


def path_of(dataset_name: str) -> Path:
    return DATA_DIR.parent / dataset_name


# --------------------------------------------------------------------------- #
# Facet builders: turn a DataFrame into OpenLineage facets
# --------------------------------------------------------------------------- #
def sql_type(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "VARCHAR"


def schema_facet(df: pd.DataFrame) -> schema_dataset.SchemaDatasetFacet:
    return schema_dataset.SchemaDatasetFacet(
        fields=[
            schema_dataset.SchemaDatasetFacetFields(name=str(col), type=sql_type(dtype))
            for col, dtype in df.dtypes.items()
        ]
    )


def data_quality_facet(df: pd.DataFrame) -> data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet:
    metrics = {}
    for col in df.columns:
        series = df[col]
        kwargs = {
            "nullCount": int(series.isna().sum()),
            "distinctCount": int(series.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(series.dtype) and series.notna().any():
            kwargs.update(
                count=float(series.count()),
                sum=float(series.sum()),
                min=float(series.min()),
                max=float(series.max()),
            )
        metrics[str(col)] = data_quality_metrics_input_dataset.ColumnMetrics(**kwargs)
    return data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(
        rowCount=len(df), columnMetrics=metrics
    )


def column_lineage_facet(
    mapping: dict[str, list[tuple[str, str, str, str]]],
) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """mapping: output column -> [(input dataset name, input column, type, subtype)]"""
    fields = {}
    for out_col, sources in mapping.items():
        fields[out_col] = column_lineage_dataset.Fields(
            inputFields=[
                column_lineage_dataset.InputField(
                    namespace=DATASET_NAMESPACE,
                    name=ds_name,
                    field=in_col,
                    transformations=[column_lineage_dataset.Transformation(type=ttype, subtype=subtype)],
                )
                for ds_name, in_col, ttype, subtype in sources
            ]
        )
    return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)


def identity_lineage(df: pd.DataFrame, src: str, renamed: dict[str, str] | None = None):
    """Columns copied 1:1 from `src`."""
    renamed = renamed or {}
    return {c: [(src, renamed.get(c, c), "DIRECT", "IDENTITY")] for c in df.columns}


# --------------------------------------------------------------------------- #
# Event emission helpers
# --------------------------------------------------------------------------- #
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class JobRun:
    """Context manager: START on enter, COMPLETE/FAIL on exit.

    Datasets are declared by name up front (START event) and enriched with facets once the
    data has actually been read/written (COMPLETE event). A FAIL event carries an
    ErrorMessageRunFacet with the traceback.
    """

    client: OpenLineageClient
    name: str
    inputs: list[str]
    outputs: list[str]
    parent: parent_run.ParentRunFacet | None = None
    description: str | None = None
    run_id: str = field(default_factory=lambda: str(generate_new_uuid()))
    _input_facets: dict[str, InputDataset] = field(default_factory=dict)
    _output_facets: dict[str, OutputDataset] = field(default_factory=dict)

    # -- called by the job body once data has been read / written ----------- #
    def record_input(self, name: str, df: pd.DataFrame) -> None:
        self._input_facets[name] = InputDataset(
            namespace=DATASET_NAMESPACE,
            name=name,
            facets={"schema": schema_facet(df)},
            inputFacets={"dataQualityMetrics": data_quality_facet(df)},
        )

    def record_output(
        self,
        name: str,
        df: pd.DataFrame,
        lineage: dict[str, list[tuple[str, str, str, str]]],
        description: str,
    ) -> None:
        self._output_facets[name] = OutputDataset(
            namespace=DATASET_NAMESPACE,
            name=name,
            facets={
                "schema": schema_facet(df),
                "documentation": documentation_dataset.DocumentationDatasetFacet(description=description),
                "columnLineage": column_lineage_facet(lineage),
            },
            outputFacets={
                "outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                    rowCount=len(df), size=path_of(name).stat().st_size, fileCount=1
                )
            },
        )

    # -- event plumbing ----------------------------------------------------- #
    def _emit(self, state: RunState, run_facets: dict | None = None, enriched: bool = False) -> None:
        facets = dict(run_facets or {})
        if self.parent:
            facets["parent"] = self.parent
        inputs = [
            (self._input_facets.get(n) if enriched and n in self._input_facets else InputDataset(DATASET_NAMESPACE, n))
            for n in self.inputs
        ]
        # Marquez creates a new dataset version for every event that lists an output, so declaring
        # bare outputs on START would add an empty version per run. COMPLETE reports the outputs with
        # their facets. FAIL still lists the outputs (bare) so that the job keeps its edges in the
        # lineage graph - otherwise everything downstream of a failed job would look disconnected.
        outputs = (
            [
                (self._output_facets.get(n) or OutputDataset(DATASET_NAMESPACE, n))
                for n in self.outputs
            ]
            if state in (RunState.COMPLETE, RunState.FAIL)
            else []
        )
        self.client.emit(
            RunEvent(
                eventType=state,
                eventTime=now_iso(),
                run=Run(runId=self.run_id, facets=facets),
                job=Job(
                    namespace=JOB_NAMESPACE,
                    name=self.name,
                    facets={
                        "jobType": job_type_job.JobTypeJobFacet(
                            processingType="BATCH", integration="PYTHON", jobType="JOB"
                        )
                    },
                ),
                producer=PRODUCER,
                inputs=inputs,
                outputs=outputs,
            )
        )

    def __enter__(self) -> "JobRun":
        print(f"[{self.name}] START  run_id={self.run_id}")
        self._emit(RunState.START)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self._emit(RunState.COMPLETE, enriched=True)
            print(f"[{self.name}] COMPLETE")
            return False
        error_facet = error_message_run.ErrorMessageRunFacet(
            message=f"{exc_type.__name__}: {exc}",
            programmingLanguage="PYTHON",
            stackTrace="".join(traceback.format_exception(exc_type, exc, tb)),
        )
        self._emit(RunState.FAIL, run_facets={"errorMessage": error_facet})
        print(f"[{self.name}] FAIL  {exc_type.__name__}: {exc}")
        return False  # re-raise


# --------------------------------------------------------------------------- #
# The "business" part: synthetic source data + three jobs
# --------------------------------------------------------------------------- #
def generate_source(rows: int, schema_change: bool, seed: int) -> None:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2026-10-01")
    df = pd.DataFrame(
        {
            "order_id": rng.integers(1, int(rows * 0.95), size=rows),  # duplicates on purpose
            "customer_id": rng.integers(1, 60, size=rows),
            "order_ts": (start + pd.to_timedelta(rng.integers(0, 14 * 24 * 3600, size=rows), unit="s")).astype(str),
            "amount_cents": rng.integers(500, 20000, size=rows).astype("float64"),
        }
    )
    df.loc[rng.choice(rows, size=max(1, rows // 20), replace=False), "amount_cents"] = np.nan  # dirty rows
    if schema_change:
        df["coupon_code"] = rng.choice(["SAVE10", "WELCOME", ""], size=rows)
    out = path_of(SOURCE)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def maybe_fail(args, job_name: str) -> None:
    if args.fail_at == job_name:
        raise ValueError(f"injected failure in {job_name} (--fail-at)")


def extract_orders(client, args, parent) -> None:
    with JobRun(client, "extract_orders", [SOURCE], [RAW], parent) as run:
        df = pd.read_csv(path_of(SOURCE))
        run.record_input(SOURCE, df)
        maybe_fail(args, "extract_orders")
        df["extracted_at"] = now_iso()
        out = path_of(RAW)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        lineage = identity_lineage(df.drop(columns="extracted_at"), SOURCE)
        lineage["extracted_at"] = []  # generated, no input column
        run.record_output(RAW, df, lineage, "Raw copy of the orders export plus extraction timestamp.")


def clean_orders(client, args, parent) -> None:
    with JobRun(client, "clean_orders", [RAW], [CLEAN], parent) as run:
        df = pd.read_csv(path_of(RAW))
        run.record_input(RAW, df)
        maybe_fail(args, "clean_orders")
        df = df.dropna(subset=["amount_cents"]).drop_duplicates(subset="order_id")
        df["order_ts"] = pd.to_datetime(df["order_ts"])
        df["order_date"] = df["order_ts"].dt.date.astype(str)
        df["amount"] = df["amount_cents"] / 100.0
        keep = ["order_id", "customer_id", "order_ts", "order_date", "amount"]
        if "coupon_code" in df.columns:
            keep.append("coupon_code")
        df = df[keep]
        out = path_of(CLEAN)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        lineage = {
            "order_id": [(RAW, "order_id", "DIRECT", "IDENTITY")],
            "customer_id": [(RAW, "customer_id", "DIRECT", "IDENTITY")],
            "order_ts": [(RAW, "order_ts", "DIRECT", "TRANSFORMATION")],
            "order_date": [(RAW, "order_ts", "DIRECT", "TRANSFORMATION")],
            "amount": [(RAW, "amount_cents", "DIRECT", "TRANSFORMATION")],
        }
        if "coupon_code" in df.columns:
            lineage["coupon_code"] = [(RAW, "coupon_code", "DIRECT", "IDENTITY")]
        run.record_output(CLEAN, df, lineage, "Orders without null amounts or duplicate order ids, typed and in currency units.")


def build_daily_revenue(client, args, parent) -> None:
    with JobRun(client, "build_daily_revenue", [CLEAN], [MARTS], parent) as run:
        df = pd.read_csv(path_of(CLEAN))
        run.record_input(CLEAN, df)
        maybe_fail(args, "build_daily_revenue")
        mart = (
            df.groupby("order_date")
            .agg(revenue=("amount", "sum"), orders=("order_id", "count"), avg_order_value=("amount", "mean"))
            .round(2)
            .reset_index()
        )
        out = path_of(MARTS)
        out.parent.mkdir(parents=True, exist_ok=True)
        mart.to_csv(out, index=False)
        lineage = {
            "order_date": [(CLEAN, "order_date", "DIRECT", "IDENTITY")],
            "revenue": [(CLEAN, "amount", "DIRECT", "AGGREGATION")],
            "orders": [(CLEAN, "order_id", "DIRECT", "AGGREGATION")],
            "avg_order_value": [(CLEAN, "amount", "DIRECT", "AGGREGATION")],
        }
        run.record_output(MARTS, mart, lineage, "Revenue, order count and average order value per day.")


JOBS = {
    "extract_orders": extract_orders,
    "clean_orders": clean_orders,
    "build_daily_revenue": build_daily_revenue,
}


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def make_client(transport: str) -> OpenLineageClient:
    if transport == "config":
        return OpenLineageClient()  # reads ./openlineage.yml
    if transport == "console":
        # the console transport logs each event as JSON at INFO level
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
        logging.getLogger("openlineage.client.transport.console").setLevel(logging.INFO)
        return OpenLineageClient(config={"transport": {"type": "console"}})
    if transport == "file":
        return OpenLineageClient(
            config={"transport": {"type": "file", "log_file_path": "events.jsonl", "append": True}}
        )
    raise ValueError(transport)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fail-at", choices=list(JOBS), help="raise an error inside this job")
    ap.add_argument("--schema-change", action="store_true", help="add a coupon_code column to the source data")
    ap.add_argument("--transport", choices=["config", "console", "file"], default="config")
    ap.add_argument("--rows", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    client = make_client(args.transport)
    generate_source(args.rows, args.schema_change, args.seed)

    # The parent run represents one execution of the whole pipeline.
    pipeline_run_id = str(generate_new_uuid())
    pipeline_job = Job(namespace=JOB_NAMESPACE, name="daily_pipeline")

    def emit_pipeline(state: RunState, facets: dict | None = None) -> None:
        client.emit(
            RunEvent(
                eventType=state,
                eventTime=now_iso(),
                run=Run(runId=pipeline_run_id, facets=facets or {}),
                job=pipeline_job,
                producer=PRODUCER,
            )
        )

    parent = parent_run.ParentRunFacet(
        run=parent_run.Run(runId=pipeline_run_id),
        job=parent_run.Job(namespace=JOB_NAMESPACE, name="daily_pipeline"),
        root=parent_run.Root(
            run=parent_run.RootRun(runId=pipeline_run_id),
            job=parent_run.RootJob(namespace=JOB_NAMESPACE, name="daily_pipeline"),
        ),
    )
    print(f"[daily_pipeline] START  run_id={pipeline_run_id}")
    emit_pipeline(RunState.START)
    try:
        for fn in JOBS.values():
            fn(client, args, parent)
            time.sleep(1)  # make durations visible in the UI
    except Exception as exc:  # noqa: BLE001
        emit_pipeline(
            RunState.FAIL,
            {
                "errorMessage": error_message_run.ErrorMessageRunFacet(
                    message=f"{type(exc).__name__}: {exc}", programmingLanguage="PYTHON"
                )
            },
        )
        print("[daily_pipeline] FAIL")
        client.close()
        return 1
    emit_pipeline(RunState.COMPLETE)
    print("[daily_pipeline] COMPLETE")
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
