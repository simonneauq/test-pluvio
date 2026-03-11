from __future__ import annotations

from lgv_pluvio.config import AppConfig
from lgv_pluvio.connectors.hubeau import HubeauConnector
from lgv_pluvio.connectors.hydroportail import HydroPortailConnector
from lgv_pluvio.connectors.meteofrance import MeteoFranceConnector
from lgv_pluvio.connectors.open_meteo import OpenMeteoConnector
from lgv_pluvio.connectors.sandre import SandreConnector
from lgv_pluvio.connectors.vigicrues import VigicruesConnector


def build_connectors(config: AppConfig) -> dict[str, object]:
    connectors: dict[str, object] = {}
    providers = config.providers

    if providers["open_meteo"]["enabled"]:
        connectors["open_meteo"] = OpenMeteoConnector(providers["open_meteo"])
    if providers["meteofrance"]["enabled"]:
        connectors["meteofrance"] = MeteoFranceConnector(
            providers["meteofrance"],
            api_key=config.meteofrance_api_key,
        )
    if providers["hubeau"]["enabled"]:
        connectors["hubeau"] = HubeauConnector(providers["hubeau"])
    if providers["vigicrues"]["enabled"]:
        connectors["vigicrues"] = VigicruesConnector(providers["vigicrues"])
    if providers["hydroportail"]["enabled"]:
        connectors["hydroportail"] = HydroPortailConnector(providers["hydroportail"])
    if providers["sandre"]["enabled"]:
        connectors["sandre"] = SandreConnector(providers["sandre"])

    return connectors
