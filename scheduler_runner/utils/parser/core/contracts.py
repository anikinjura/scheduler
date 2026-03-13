from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ParserJob:
    """Явное задание для parser."""

    report_type: str
    pvz_id: str
    execution_date: str
    extra_params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReportDefinition:
    """Статическое описание отчета и parser-конфига."""

    report_type: str
    config: dict[str, Any]


@dataclass(frozen=True)
class ParserRuntimeContext:
    """Runtime-параметры выполнения parser."""

    headless: bool = True
    timeout_sec: int = 180
    continue_on_job_error: bool = True
    save_to_file: bool = False
    output_format: str = "json"


@dataclass
class ParserJobResult:
    """Нормализованный результат выполнения одного parser job."""

    report_type: str
    pvz_id: str
    execution_date: str
    success: bool
    data: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_success(
        cls,
        *,
        report_type: str,
        pvz_id: str,
        execution_date: str,
        data: dict[str, Any],
    ) -> "ParserJobResult":
        return cls(
            report_type=report_type,
            pvz_id=pvz_id,
            execution_date=execution_date,
            success=True,
            data=data,
        )

    @classmethod
    def from_error(
        cls,
        *,
        report_type: str,
        pvz_id: str,
        execution_date: str,
        error_message: str,
        error_code: str | None = None,
    ) -> "ParserJobResult":
        return cls(
            report_type=report_type,
            pvz_id=pvz_id,
            execution_date=execution_date,
            success=False,
            error_code=error_code,
            error_message=error_message,
        )
