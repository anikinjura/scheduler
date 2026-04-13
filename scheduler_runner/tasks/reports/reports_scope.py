"""
reports_scope.py

Scope-resolution, live accessibility, degrade multi -> single, pre-check доступности ПВЗ.

Извлечено из reports_processor.py (Phase 2.1 — medium-risk extraction).
"""
from config.base_config import PVZ_ID
from .config.scripts.reports_processor_config import BACKFILL_CONFIG
from .reports_utils import normalize_pvz_id
from scheduler_runner.utils.logging import configure_logger
from scheduler_runner.utils.parser import invoke_available_pvz_discovery


def resolve_pvz_ids(raw_pvz_ids=None):
    """Нормализует и дедуплицирует запрошенные PVZ."""
    if not raw_pvz_ids:
        return [PVZ_ID]

    resolved_pvz_ids = []
    seen_pvz_ids = set()
    for pvz_id in raw_pvz_ids:
        normalized_pvz_id = str(pvz_id or "").strip()
        if not normalized_pvz_id or normalized_pvz_id in seen_pvz_ids:
            continue
        resolved_pvz_ids.append(normalized_pvz_id)
        seen_pvz_ids.add(normalized_pvz_id)

    return resolved_pvz_ids or [PVZ_ID]


def discover_available_pvz_scope(configured_pvz_id=PVZ_ID, logger=None, parser_logger=None):
    """Discovery доступных ПВЗ через Ozon dropdown.

    Возвращает:
        dict с ключами: success, configured_pvz_id, available_pvz,
        normalized_available_pvz, discovery_result
    """
    from scheduler_runner.utils.parser import create_parser_logger

    logger = logger or configure_logger(user="reports_domain", task_name="Processor")
    parser_logger = parser_logger or create_parser_logger()

    discovery_result = invoke_available_pvz_discovery(
        pvz_id=configured_pvz_id,
        logger=parser_logger,
    )
    available_pvz = []
    normalized_available_pvz = set()

    if discovery_result.get("success", False):
        for pvz_id in discovery_result.get("available_pvz", []) or []:
            normalized_pvz_id = normalize_pvz_id(pvz_id)
            if not normalized_pvz_id or normalized_pvz_id in normalized_available_pvz:
                continue
            normalized_available_pvz.add(normalized_pvz_id)
            available_pvz.append(pvz_id)
        if normalize_pvz_id(configured_pvz_id) not in normalized_available_pvz:
            available_pvz.append(configured_pvz_id)
            normalized_available_pvz.add(normalize_pvz_id(configured_pvz_id))
        logger.info(
            f"Discovery доступных ПВЗ завершен: configured_pvz_id={configured_pvz_id}, "
            f"available_pvz={available_pvz}"
        )
    else:
        logger.warning(
            "Discovery доступных ПВЗ завершился ошибкой, fallback только на собственный PVZ: "
            f"{configured_pvz_id}; error={discovery_result.get('error', 'unknown_error')}"
        )
        available_pvz = [configured_pvz_id]
        normalized_available_pvz = {normalize_pvz_id(configured_pvz_id)}

    return {
        "success": discovery_result.get("success", False),
        "configured_pvz_id": configured_pvz_id,
        "available_pvz": available_pvz,
        "normalized_available_pvz": normalized_available_pvz,
        "discovery_result": discovery_result,
    }


def resolve_accessible_pvz_ids(raw_pvz_ids=None, configured_pvz_id=PVZ_ID, logger=None, parser_logger=None):
    """Определяет, какие PVZ доступны в текущей сессии.

    Если запрошены только свой PVZ — возвращает без discovery.
    Если запрошены коллеги — запускает discovery_available_pvz_scope
    и фильтрует по live-доступности.
    """
    requested_pvz_ids = resolve_pvz_ids(raw_pvz_ids)
    normalized_configured_pvz_id = normalize_pvz_id(configured_pvz_id)
    requested_colleague_pvz_ids = [
        pvz_id for pvz_id in requested_pvz_ids if normalize_pvz_id(pvz_id) != normalized_configured_pvz_id
    ]

    if not requested_colleague_pvz_ids:
        return {
            "accessible_pvz_ids": requested_pvz_ids,
            "skipped_pvz_ids": [],
            "discovery_scope": None,
        }

    discovery_scope = discover_available_pvz_scope(
        configured_pvz_id=configured_pvz_id,
        logger=logger,
        parser_logger=parser_logger,
    )
    normalized_available_pvz = discovery_scope.get("normalized_available_pvz", set())
    accessible_pvz_ids = []
    skipped_pvz_ids = []

    for pvz_id in requested_pvz_ids:
        if normalize_pvz_id(pvz_id) in normalized_available_pvz:
            accessible_pvz_ids.append(pvz_id)
        else:
            skipped_pvz_ids.append(pvz_id)

    if logger and skipped_pvz_ids:
        logger.warning(
            f"Недоступные для текущей учетной записи PVZ исключены из backfill: {skipped_pvz_ids}"
        )

    return {
        "accessible_pvz_ids": accessible_pvz_ids,
        "skipped_pvz_ids": skipped_pvz_ids,
        "discovery_scope": discovery_scope,
    }


def should_run_automatic_failover_coordination(
    *,
    enabled,
    raw_pvz_ids=None,
    resolved_pvz_ids=None,
    current_pvz_id=None,
    configured_pvz_id=PVZ_ID,
):
    """Определяет, нужен ли автоматический failover coordination pass.

    Автоматический failover coordination запускается только если:
    - enabled=True
    - raw_pvz_ids НЕ указаны (не ручной multi-PVZ backfill)
    - resolved_pvz_ids НЕ multi-PVZ (один PVZ или конфигурационный)
    - current_pvz_id совпадает с configured_pvz_id
    """
    if not enabled:
        return False
    if raw_pvz_ids:
        return False
    if resolved_pvz_ids is not None and len(resolved_pvz_ids) > 1:
        return False
    if current_pvz_id is not None and normalize_pvz_id(current_pvz_id) != normalize_pvz_id(configured_pvz_id):
        return False
    return True


# ──────────────────────────────────────────────
# Parser-adapter helpers (для backfill orchestration)
# ──────────────────────────────────────────────

def build_parser_definition():
    """Строит parser definition для reports backfill.

    Вынесено сюда из reports_processor.py, чтобы reports_scope мог
    самостоятельно создавать parser jobs без импорта orchestration-логики.
    """
    from scheduler_runner.utils.parser import build_parser_definition as _build_parser_definition
    return _build_parser_definition()


def build_jobs_from_missing_dates_by_pvz(missing_dates_by_pvz, definition=None, extra_params_by_pvz=None):
    """Строит parser jobs из missing_dates_by_pvz для multi-PVZ backfill."""
    from scheduler_runner.utils.parser import build_jobs_for_pvz

    parser_definition = definition or build_parser_definition()
    jobs = []
    for pvz_id, execution_dates in (missing_dates_by_pvz or {}).items():
        jobs.extend(
            build_jobs_for_pvz(
                pvz_id=pvz_id,
                execution_dates=execution_dates,
                definition=parser_definition,
                extra_params=(extra_params_by_pvz or {}).get(pvz_id),
            )
        )
    return jobs


def group_jobs_by_pvz(jobs):
    """Группирует parser jobs по pvz_id."""
    grouped_jobs = {}
    for job in jobs or []:
        grouped_jobs.setdefault(job.pvz_id, []).append(job)
    return grouped_jobs

