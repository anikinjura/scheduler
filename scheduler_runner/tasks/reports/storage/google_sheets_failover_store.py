"""
google_sheets_failover_store.py

Google Sheets implementation of FailoverStateStore protocol.
This is a thin adapter layer that delegates to the existing
Google Sheets implementation functions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime

from .failover_state_protocol import FailoverStateStore
from .google_sheets_store import (
    create_failover_state_logger,
    get_failover_state,
    get_failover_state_rows_by_keys,
    list_failover_state_rows,
    list_candidate_failover_rows_fast,
    upsert_failover_state,
    upsert_failover_state_records,
    mark_failover_state,
    is_claim_active,
    try_claim_failover,
)


class GoogleSheetsFailoverStore(FailoverStateStore):
    """Google Sheets implementation of FailoverStateStore protocol.

    Uses batch_get for read optimization and
    batch_update + batch_append for write optimization.
    """

    def __init__(self):
        self._logger = create_failover_state_logger()

    def get_store_type(self) -> str:
        return "google_sheets"

    def get_row(self, execution_date: str, target_object_name: str):
        return get_failover_state(
            execution_date=execution_date,
            target_object_name=target_object_name,
            logger=self._logger,
        )

    def get_rows_by_keys(self, keys):
        return get_failover_state_rows_by_keys(keys=keys, logger=self._logger)

    def list_rows(self, statuses=None, target_object_name=None):
        return list_failover_state_rows(statuses=statuses, target_object_name=target_object_name, logger=self._logger)

    def list_candidate_rows(self, statuses=None):
        return list_candidate_failover_rows_fast(statuses=statuses, logger=self._logger)

    def upsert_record(self, record):
        return upsert_failover_state(record=record, logger=self._logger)

    def upsert_records(self, records):
        return upsert_failover_state_records(records=records, logger=self._logger)

    def mark_state(self, execution_date, target_object_name, owner_object_name, status, source_run_id="", last_error="", claimed_by="", claim_expires_at="", attempt_no=0):
        return mark_failover_state(
            execution_date=execution_date,
            target_object_name=target_object_name,
            owner_object_name=owner_object_name,
            status=status,
            claimed_by=claimed_by,
            ttl_minutes=0,
            source_run_id=source_run_id,
            last_error=last_error,
            logger=self._logger,
        )

    def is_claim_active(self, state_row, now=None):
        return is_claim_active(state_row=state_row, now=now)

    def try_claim(self, execution_date, target_object_name, owner_object_name, claimer_pvz, ttl_minutes, source_run_id=""):
        return try_claim_failover(
            execution_date=execution_date,
            target_object_name=target_object_name,
            owner_object_name=owner_object_name,
            claimer_pvz=claimer_pvz,
            ttl_minutes=ttl_minutes,
            source_run_id=source_run_id,
            logger=self._logger,
        )

