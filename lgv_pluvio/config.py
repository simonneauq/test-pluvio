from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

try:
    import streamlit as st
except ImportError:  # pragma: no cover
    st = None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _secret(name: str) -> str:
    if st is None:
        return ""
    try:
        value = st.secrets.get(name, "")
    except Exception:
        return ""
    return str(value).strip()


@dataclass(slots=True)
class AppConfig:
    raw: dict[str, Any]

    @property
    def title(self) -> str:
        return self.raw["app"]["title"]

    @property
    def timezone(self) -> str:
        return self.raw["app"]["timezone"]

    @property
    def history_years(self) -> int:
        return int(self.raw["app"]["history_years"])

    @property
    def sample_if_empty(self) -> bool:
        return bool(self.raw["app"]["sample_if_empty"])

    @property
    def data_root(self) -> Path:
        return Path(self.raw["paths"]["data_root"])

    @property
    def db_path(self) -> Path:
        return Path(self.raw["paths"]["db_path"])

    @property
    def communes_xlsx_path(self) -> Path:
        return Path(self.raw["paths"]["communes_xlsx_path"])

    @property
    def lrs_axes_path(self) -> Path:
        return Path(self.raw["paths"]["lrs_axes_path"])

    @property
    def lrs_pk_path(self) -> Path:
        return Path(self.raw["paths"]["lrs_pk_path"])

    @property
    def providers(self) -> dict[str, Any]:
        return self.raw["providers"]

    @property
    def matching(self) -> dict[str, Any]:
        return self.raw["matching"]

    @property
    def thresholds(self) -> dict[str, Any]:
        return self.raw["thresholds"]

    @property
    def meteofrance_api_key(self) -> str:
        return os.getenv("METEOFRANCE_API_KEY", "").strip() or _secret("METEOFRANCE_API_KEY")


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    raw["paths"]["data_root"] = os.getenv("DATA_ROOT", raw["paths"]["data_root"]) or _secret("DATA_ROOT") or raw["paths"]["data_root"]
    raw["paths"]["db_path"] = str(Path(raw["paths"]["data_root"]) / "lgv_pluviometrie.duckdb")
    raw["paths"]["communes_xlsx_path"] = os.getenv(
        "COMMUNES_XLSX_PATH",
        raw["paths"]["communes_xlsx_path"],
    ) or _secret("COMMUNES_XLSX_PATH")
    raw["paths"]["lrs_axes_path"] = os.getenv("LRS_AXES_PATH", raw["paths"]["lrs_axes_path"]) or _secret("LRS_AXES_PATH")
    raw["paths"]["lrs_pk_path"] = os.getenv("LRS_PK_PATH", raw["paths"]["lrs_pk_path"]) or _secret("LRS_PK_PATH")

    raw["providers"]["open_meteo"]["enabled"] = _env_bool(
        "OPEN_METEO_ENABLED",
        raw["providers"]["open_meteo"]["enabled"],
    )
    raw["providers"]["meteofrance"]["enabled"] = _env_bool(
        "METEOFRANCE_ENABLED",
        raw["providers"]["meteofrance"]["enabled"],
    )
    raw["providers"]["hubeau"]["enabled"] = _env_bool(
        "HUBEAU_ENABLED",
        raw["providers"]["hubeau"]["enabled"],
    )
    raw["providers"]["vigicrues"]["enabled"] = _env_bool(
        "VIGICRUES_ENABLED",
        raw["providers"]["vigicrues"]["enabled"],
    )
    raw["providers"]["hydroportail"]["enabled"] = _env_bool(
        "HYDROPORTAIL_ENABLED",
        raw["providers"]["hydroportail"]["enabled"],
    )
    raw["providers"]["sandre"]["enabled"] = _env_bool(
        "SANDRE_ENABLED",
        raw["providers"]["sandre"]["enabled"],
    )

    return AppConfig(raw=raw)
