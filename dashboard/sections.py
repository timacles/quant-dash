"""Section registry, dataclasses, and column classification sets."""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_SECTION_LIMIT = 10


@dataclass(frozen=True)
class SectionConfig:
    key: str
    title: str
    description: str
    type: str
    source: str


@dataclass(frozen=True)
class SectionDisplayConfig:
    columns: tuple[str, ...]
    column_labels: dict[str, str]


@dataclass(frozen=True)
class ResolvedSectionConfig:
    key: str
    title: str
    description: str
    type: str
    source: str
    columns: tuple[str, ...]
    column_labels: dict[str, str]


SECTIONS: tuple[SectionConfig, ...] = (
    SectionConfig(
        key="momentum_longs",
        title="Momentum Longs",
        description="Upside momentum leaders ranked by the existing cross-sectional composite score.",
        type="table",
        source="mv_etf_report_momentum_longs",
    ),
    SectionConfig(
        key="momentum_shorts",
        title="Momentum Shorts",
        description="Downside momentum leaders ranked by the existing cross-sectional composite score.",
        type="table",
        source="mv_etf_report_momentum_shorts",
    ),
    SectionConfig(
        key="oversold_mean_reversion",
        title="Oversold Mean Reversion",
        description="Long-reversion candidates with the strongest downside stretch and reversal setup.",
        type="table",
        source="mv_etf_report_oversold_mean_reversion",
    ),
    SectionConfig(
        key="overbought_mean_reversion",
        title="Overbought Mean Reversion",
        description="Short-reversion candidates with the most extended upside stretch.",
        type="table",
        source="mv_etf_report_overbought_mean_reversion",
    ),
    SectionConfig(
        key="range_compression",
        title="Range Compression",
        description="Tightening setups ranked by compression and accumulation features.",
        type="table",
        source="mv_etf_report_range_compression",
    ),
)

PERCENT_COLUMNS: frozenset[str] = frozenset({
    "ret_1d", "ret_3d", "ret_5d", "ret_10d", "rs_5", "rs_10",
    "chg_1d", "chg_5d", "chg_10d", "chg_20d",
    "vs_dma_20", "vs_dma_50", "vs_dma_200",
})
SIGNED_COLUMNS: frozenset[str] = frozenset({
    "ret_1d", "ret_3d", "ret_5d", "ret_10d", "rs_5", "rs_10",
    "zscore_close_20", "atr_stretch_20",
    "chg_1d", "chg_5d", "chg_10d", "chg_20d",
    "vs_dma_20", "vs_dma_50", "vs_dma_200",
})
DECIMAL_2_COLUMNS: frozenset[str] = frozenset({"rvol_20", "composite_score", "wk_rvol"})
DECIMAL_3_COLUMNS: frozenset[str] = frozenset({
    "zscore_close_20",
    "atr_stretch_20",
    "close_location_20",
    "volume_ratio_5_20",
    "range_compression_5_20",
    "range_compression_5_60",
    "atr_compression_5_20",
})
