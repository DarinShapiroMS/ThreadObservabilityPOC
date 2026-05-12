"""Unit tests for :mod:`thread_observability.pipeline.ha_availability`.

The remote fetch path (HTTP to Supervisor) is exercised via the
``_score_device`` and ``_build_device_to_entities`` helpers so we don't
need a live HA instance — those two functions encapsulate every decision
that matters for the user-facing ``available`` column.
"""

from __future__ import annotations

from thread_observability.pipeline import ha_availability


def test_build_device_to_entities_skips_disabled_and_hidden() -> None:
    entries = [
        {"entity_id": "light.kitchen", "device_id": "d1"},
        {"entity_id": "switch.kitchen", "device_id": "d1", "disabled_by": "user"},
        {"entity_id": "sensor.kitchen_battery", "device_id": "d1", "hidden_by": "user"},
        {"entity_id": "binary_sensor.door", "device_id": "d2"},
        # No device_id → ignored.
        {"entity_id": "sensor.orphan", "device_id": None},
        # No entity_id → ignored.
        {"entity_id": None, "device_id": "d3"},
    ]
    out = ha_availability._build_device_to_entities(entries)
    assert out == {
        "d1": [("light", "light.kitchen")],
        "d2": [("binary_sensor", "binary_sensor.door")],
    }


def test_score_device_primary_domain_wins() -> None:
    entities = [("light", "light.k"), ("sensor", "sensor.k_battery")]
    # Light reachable → device reachable, sensor unavailability ignored.
    assert ha_availability._score_device(
        entities,
        {"light.k": "on", "sensor.k_battery": "unavailable"},
    ) is True


def test_score_device_all_primaries_unavailable_returns_false() -> None:
    entities = [("light", "light.k"), ("switch", "switch.k")]
    assert ha_availability._score_device(
        entities,
        {"light.k": "unavailable", "switch.k": "unknown"},
    ) is False


def test_score_device_falls_back_to_diagnostic_domain() -> None:
    # No primary-domain entities → use fallback domains (binary_sensor etc).
    entities = [("binary_sensor", "binary_sensor.door"), ("sensor", "sensor.batt")]
    assert ha_availability._score_device(
        entities,
        {"binary_sensor.door": "off", "sensor.batt": "unavailable"},
    ) is True


def test_score_device_returns_none_when_no_scoreable_entities() -> None:
    # Only excluded domains (update, button) → unknown availability.
    entities = [("update", "update.fw"), ("button", "button.identify")]
    assert ha_availability._score_device(entities, {"update.fw": "on"}) is None


def test_score_device_returns_none_when_pool_states_missing() -> None:
    # Primary entities exist but no state info for any of them.
    entities = [("light", "light.k")]
    assert ha_availability._score_device(entities, {}) is None


def test_score_device_treats_blank_state_as_unreachable() -> None:
    entities = [("light", "light.k")]
    assert ha_availability._score_device(entities, {"light.k": ""}) is False
