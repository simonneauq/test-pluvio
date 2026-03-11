from __future__ import annotations

from lgv_pluvio.sample_data import build_sample_bundle


def test_sample_bundle_contains_core_tables() -> None:
    bundle = build_sample_bundle()
    assert "segments" in bundle
    assert "segment_daily_rollup" in bundle
    assert not bundle["segments"].empty
