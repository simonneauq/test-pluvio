from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd
import requests


class BaseConnector(ABC):
    provider_name: str = "base"

    def __init__(self, config: dict[str, Any], api_key: str | None = None) -> None:
        self.config = config
        self.api_key = api_key or ""
        self.session = requests.Session()

    def _get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = self.session.get(
            url,
            params=params,
            headers=headers,
            timeout=self.config.get("timeout_seconds", 30),
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            return response.json()
        return response.text

    @abstractmethod
    def fetch_metadata(self, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_observations(
        self,
        start: datetime,
        end: datetime,
        area_spec: pd.DataFrame,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_forecasts(
        self,
        start: datetime,
        end: datetime,
        area_spec: pd.DataFrame,
    ) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_alerts(
        self,
        start: datetime,
        end: datetime,
        area_spec: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError


def empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)
