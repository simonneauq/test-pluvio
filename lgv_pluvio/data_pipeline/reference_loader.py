from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from lgv_pluvio.config import AppConfig

try:
    import fiona
    import geopandas as gpd
except ImportError:  # pragma: no cover
    fiona = None
    gpd = None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip().lower() for column in result.columns]
    return result


def load_communes_reference(path: Path) -> pd.DataFrame:
    communes = pd.read_excel(path, sheet_name="communes_pk_lgv")
    communes = _normalize_columns(communes)
    communes = communes.rename(
        columns={
            "nom_region": "region",
            "code_dept": "departement_code",
            "nom_dept": "departement",
            "insee": "insee",
            "code_posta": "code_postal",
            "nom_commun": "commune_name",
            "pkd": "pk_start",
            "pkf": "pk_end",
        }
    )
    communes["pk_start"] = communes["pk_start"].astype(str).str.replace(",", ".", regex=False).astype(float)
    communes["pk_end"] = communes["pk_end"].astype(str).str.replace(",", ".", regex=False).astype(float)
    communes["insee"] = communes["insee"].astype(str).str.zfill(5)
    return communes


def _candidate_layers(path: Path) -> list[str]:
    if fiona is None:
        return []
    try:
        return list(fiona.listlayers(path))
    except Exception:
        return []


def _pick_gpkg_layer(path: Path, preferred_tokens: list[str]):
    if gpd is None:
        raise RuntimeError("geopandas et fiona sont requis pour lire les geopackage.")

    layers = _candidate_layers(path)
    if not layers:
        return gpd.read_file(path)

    for token in preferred_tokens:
        for layer in layers:
            if token.lower() in layer.lower():
                return gpd.read_file(path, layer=layer)
    return gpd.read_file(path, layer=layers[0])


def _find_first_column(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        for column in columns:
            if candidate in column.lower():
                return column
    return None


def load_lgv_segments(path: Path):
    gdf = _pick_gpkg_layer(path, ["pk", "lrs"])
    gdf.columns = [str(column).strip().lower() for column in gdf.columns]
    pk_start_col = _find_first_column(list(gdf.columns), ["pkd", "pk_debut", "pk_start", "pkini", "pk"])
    pk_end_col = _find_first_column(list(gdf.columns), ["pkf", "pk_fin", "pk_end", "pkfin"])
    line_col = _find_first_column(list(gdf.columns), ["voie", "line", "axe", "code"])
    name_col = _find_first_column(list(gdf.columns), ["libelle", "nom", "name"])

    frame = gdf.copy()
    frame["pk_start"] = pd.to_numeric(frame[pk_start_col], errors="coerce") if pk_start_col else range(len(frame))
    frame["pk_end"] = pd.to_numeric(frame[pk_end_col], errors="coerce") if pk_end_col else frame["pk_start"]
    frame["line_code"] = frame[line_col].astype(str) if line_col else "LGV"
    frame["segment_name"] = frame[name_col].astype(str) if name_col else "Segment " + frame.index.astype(str)
    frame["segment_id"] = (
        frame["line_code"].astype(str)
        + "::"
        + frame["pk_start"].round(3).astype(str)
        + "::"
        + frame["pk_end"].round(3).astype(str)
    )
    return frame[["segment_id", "segment_name", "pk_start", "pk_end", "line_code", "geometry"]]


def build_segment_reference(segments_gdf, communes_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for segment in segments_gdf.itertuples(index=False):
        overlaps = communes_df[
            (communes_df["pk_start"] <= segment.pk_end)
            & (communes_df["pk_end"] >= segment.pk_start)
        ]
        if overlaps.empty:
            overlaps = pd.DataFrame([{}])

        for commune in overlaps.to_dict(orient="records"):
            rows.append(
                {
                    "segment_id": segment.segment_id,
                    "segment_name": segment.segment_name,
                    "pk_start": segment.pk_start,
                    "pk_end": segment.pk_end,
                    "line_code": segment.line_code,
                    "commune_insee": commune.get("insee"),
                    "commune_name": commune.get("commune_name"),
                    "departement_code": commune.get("departement_code"),
                    "departement": commune.get("departement"),
                    "region": commune.get("region"),
                    "geometry_wkt": segment.geometry.wkt if segment.geometry is not None else None,
                }
            )
    return pd.DataFrame(rows)


def sample_segment_points(segments_gdf) -> pd.DataFrame:
    rows = []
    for segment in segments_gdf.itertuples(index=False):
        geometry = segment.geometry
        if geometry is None:
            continue
        point = geometry.interpolate(0.5, normalized=True) if geometry.geom_type in {"LineString", "MultiLineString"} else geometry.representative_point()
        rows.append(
            {
                "segment_id": segment.segment_id,
                "segment_name": segment.segment_name,
                "pk_start": segment.pk_start,
                "pk_end": segment.pk_end,
                "latitude": point.y,
                "longitude": point.x,
            }
        )
    return pd.DataFrame(rows)


def load_reference_bundle(config: AppConfig) -> dict[str, pd.DataFrame]:
    if not config.communes_xlsx_path or not Path(config.communes_xlsx_path).exists():
        raise FileNotFoundError("Fichier communes introuvable. Charge le fichier Excel dans l'application ou configure COMMUNES_XLSX_PATH.")
    if not config.lrs_pk_path or not Path(config.lrs_pk_path).exists():
        raise FileNotFoundError("GeoPackage PK introuvable. Charge LRS_PK.gpkg dans l'application ou configure LRS_PK_PATH.")
    communes = load_communes_reference(config.communes_xlsx_path)
    segments_gdf = load_lgv_segments(config.lrs_pk_path)
    segment_ref = build_segment_reference(segments_gdf, communes)
    sample_points = sample_segment_points(segments_gdf)
    return {"communes": communes, "segments": segment_ref, "sample_points": sample_points}
