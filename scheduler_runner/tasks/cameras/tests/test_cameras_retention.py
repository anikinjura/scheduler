import pytest

from scheduler_runner.tasks.cameras.config.cameras_retention import (
    RETENTION_DEFAULTS_DAYS,
    RETENTION_OVERRIDES_DAYS,
    get_retention_days,
)


def test_get_retention_days_returns_default_for_unknown_pvz():
    assert get_retention_days("UNKNOWN_PVZ", "local") == RETENTION_DEFAULTS_DAYS["local"]
    assert get_retention_days("UNKNOWN_PVZ", "network") == RETENTION_DEFAULTS_DAYS["network"]


def test_get_retention_days_uses_object_override(monkeypatch):
    monkeypatch.setitem(RETENTION_OVERRIDES_DAYS, "TEST_PVZ", {"local": 30, "network": 365})

    assert get_retention_days("TEST_PVZ", "local") == 30
    assert get_retention_days("TEST_PVZ", "network") == 365


def test_get_retention_days_invalid_scenario():
    with pytest.raises(ValueError):
        get_retention_days("ANY", "invalid")

