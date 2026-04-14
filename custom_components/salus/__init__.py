"""Support for Salus iT600 gateway and devices."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from homeassistant import config_entries, core
from homeassistant.const import CONF_HOST, CONF_TOKEN
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.config_validation import config_entry_only_config_schema
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .config_flow import CONF_FLOW_TYPE, CONF_USER
from .const import CONF_POLL_FAILURE_THRESHOLD, DEFAULT_POLL_FAILURE_THRESHOLD, DOMAIN
from .exceptions import (
    IT600AuthenticationError,
    IT600ConnectionError,
    IT600UnsupportedFirmwareError,
)
from .gateway import IT600Gateway

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = config_entry_only_config_schema(DOMAIN)

GATEWAY_PLATFORMS = [
    "climate",
    "binary_sensor",
    "switch",
    "cover",
    "sensor",
    "lock",
]


async def async_setup(hass: core.HomeAssistant, config: dict) -> bool:
    """Set up the Salus iT600 component."""
    return True


async def async_setup_entry(
    hass: core.HomeAssistant, entry: config_entries.ConfigEntry
) -> bool:
    """Set up components from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    if entry.data.get(CONF_FLOW_TYPE) == CONF_USER:
        if not await async_setup_gateway_entry(hass, entry):
            return False
    else:
        _LOGGER.debug(
            "Skipping entry %s (flow type %s)",
            entry.entry_id,
            entry.data.get(CONF_FLOW_TYPE),
        )

    return True


async def async_setup_gateway_entry(
    hass: core.HomeAssistant, entry: config_entries.ConfigEntry
) -> bool:
    """Set up the Gateway component from a config entry."""
    host = entry.data[CONF_HOST]
    euid = entry.data[CONF_TOKEN]

    gateway = IT600Gateway(host=host, euid=euid)

    try:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await gateway.connect()
                await gateway.poll_status()
                break
            except Exception as exc:
                _LOGGER.debug(
                    "Connection attempt %d/%d failed: %s",
                    attempt,
                    max_attempts,
                    exc,
                )
                if attempt == max_attempts:
                    raise
                await asyncio.sleep(3)
    except IT600ConnectionError:
        _LOGGER.error(
            "Connection error: check if you have specified gateway's HOST correctly."
        )
        await gateway.close()
        return False
    except IT600AuthenticationError:
        _LOGGER.error(
            "Authentication error: check if you have specified "
            "gateway's EUID correctly."
        )
        await gateway.close()
        return False
    except IT600UnsupportedFirmwareError:
        _LOGGER.error(
            "Gateway firmware uses an unsupported encryption protocol. "
            "Enable debug logging for custom_components.salus and open an issue at "
            "https://github.com/leonardpitzu/homeassistant_salus/issues"
        )
        await gateway.close()
        return False

    _LOGGER.debug("Successfully connected to Salus gateway at %s", host)

    # ── Shared coordinator ──────────────────────────────────────────
    # Tolerate up to N consecutive poll failures before marking entities
    # unavailable.  This prevents brief gateway hiccups (common when a
    # thermostat is changing state) from flipping every entity to
    # "unavailable" for a single 30-second cycle.
    max_failures = entry.options.get(
        CONF_POLL_FAILURE_THRESHOLD, DEFAULT_POLL_FAILURE_THRESHOLD
    )
    consecutive_failures = 0

    async def _async_update_data() -> bool:
        nonlocal consecutive_failures
        try:
            async with asyncio.timeout(30):
                await gateway.poll_status()
            consecutive_failures = 0
            return True
        except Exception:
            consecutive_failures += 1
            if max_failures == 0 or consecutive_failures >= max_failures:
                _LOGGER.debug(
                    "Poll failed (%d consecutive) — marking entities unavailable",
                    consecutive_failures,
                )
                raise
            _LOGGER.debug(
                "Poll failed (%d/%d before unavailable)",
                consecutive_failures,
                max_failures,
            )
            # Swallow the error — keep last known good state.
            return True

    coordinator = DataUpdateCoordinator(
        hass,
        _LOGGER,
        config_entry=entry,
        name="salus",
        update_method=_async_update_data,
        update_interval=timedelta(seconds=30),
    )

    # The initial poll already happened above; seed the coordinator so
    # that ``coordinator.last_update_success`` is True and entities see
    # themselves as available immediately.
    coordinator.async_set_updated_data(True)

    hass.data[DOMAIN][entry.entry_id] = {
        "gateway": gateway,
        "coordinator": coordinator,
    }

    gateway_info = gateway.get_gateway_device()
    if gateway_info is not None:
        device_registry = dr.async_get(hass)
        device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,
            connections={(dr.CONNECTION_NETWORK_MAC, gateway_info.unique_id)},
            identifiers={(DOMAIN, gateway_info.unique_id)},
            manufacturer=gateway_info.manufacturer,
            name=gateway_info.name,
            model=gateway_info.model,
            sw_version=gateway_info.sw_version,
        )
    else:
        _LOGGER.debug("Gateway device info unavailable — skipping device registry")

    await hass.config_entries.async_forward_entry_setups(entry, GATEWAY_PLATFORMS)

    return True


async def async_unload_entry(
    hass: core.HomeAssistant,
    config_entry: config_entries.ConfigEntry,
) -> bool:
    """Unload a config entry."""
    _LOGGER.debug("Unloading Salus config entry %s", config_entry.entry_id)
    unload_ok = await hass.config_entries.async_unload_platforms(
        config_entry, GATEWAY_PLATFORMS
    )

    if unload_ok:
        data = hass.data[DOMAIN].pop(config_entry.entry_id, None)
        if data is not None:
            gateway = data if isinstance(data, IT600Gateway) else data.get("gateway")
            if gateway is not None:
                await gateway.close()
                _LOGGER.debug("Gateway session closed for %s", config_entry.entry_id)
            else:
                _LOGGER.debug(
                    "No gateway found in entry data during unload — "
                    "session may not have been closed"
                )
        else:
            _LOGGER.debug(
                "No data found for entry %s during unload", config_entry.entry_id
            )
    else:
        _LOGGER.debug(
            "Platform unload failed for entry %s", config_entry.entry_id
        )

    return unload_ok
