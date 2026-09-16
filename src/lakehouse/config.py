"""Month arithmetic and the lake layout: the one place that knows where every file lives."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_LAKE_ROOT = "data/lake"


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"month must be 1-12, got {self.month}")

    @classmethod
    def parse(cls, text: str) -> Month:
        match = re.fullmatch(r"(\d{4})-(\d{2})", text)
        if not match:
            raise ValueError(f"expected YYYY-MM, got {text!r}")
        return cls(int(match[1]), int(match[2]))

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def partition_id(self) -> str:
        """BigQuery partition decorator id for MONTH partitioning (yyyymm)."""
        return f"{self.year:04d}{self.month:02d}"

    def next(self) -> Month:
        return Month(self.year + self.month // 12, self.month % 12 + 1)


def lake_root() -> str:
    """Local directory for development, gs://<bucket> for the real lake."""
    return os.environ.get("LAKE_ROOT", DEFAULT_LAKE_ROOT).rstrip("/")


def source_url(month: Month) -> str:
    return f"{TLC_BASE_URL}/yellow_tripdata_{month}.parquet"


def _month_dir(root: str, zone: str, month: Month) -> str:
    return f"{root}/{zone}/yellow/year={month.year:04d}/month={month.month:02d}"


def raw_file_uri(root: str, month: Month) -> str:
    return f"{_month_dir(root, 'raw', month)}/yellow_tripdata_{month}.parquet"


def raw_manifest_uri(root: str, month: Month) -> str:
    return f"{_month_dir(root, 'raw', month)}/_manifest.json"


def curated_dir_uri(root: str, month: Month) -> str:
    return _month_dir(root, "curated", month)


def quarantine_dir_uri(root: str, month: Month) -> str:
    return _month_dir(root, "quarantine", month)


def dq_report_uri(root: str, month: Month) -> str:
    return f"{_month_dir(root, 'reports', month)}/dq_report.json"
