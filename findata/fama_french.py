"""
findata.fama_french
-------------------
Daily Fama-French factor returns from the Kenneth R. French Data Library,
across all regions Ken French publishes:

  US                 — F-F_Research_Data_*_daily        (the original)
  Developed          — Developed_{3,5}_Factors_Daily
  DevelopedExUS      — Developed_ex_US_{3,5}_Factors_Daily
  Europe             — Europe_{3,5}_Factors_Daily
  Japan              — Japan_{3,5}_Factors_Daily
  AsiaPacificExJapan — Asia_Pacific_ex_Japan_{3,5}_Factors_Daily
  NorthAmerica       — North_America_{3,5}_Factors_Daily
  EmergingMarkets    — Emerging_{3,5}_Factors_Daily

Indian equities (.NS suffix) get the closest-fit regional factor set
via factor_loadings.py — Asia Pacific ex Japan (India is its largest
constituent) — rather than US factors with a heavy disclaimer.
"""

from __future__ import annotations

from typing import Optional


# Public region-name → pandas-datareader dataset-name maps. Both the
# 3-factor and 5-factor datasets follow the same naming convention
# across regions (only the US dataset has the legacy F-F_Research…
# prefix). Ken French only publishes a 3-factor 5-factor pair for these
# 8 regions; momentum / industry / etc. would need separate datasets.
REGIONS_3F: dict[str, str] = {
    "US":                 "F-F_Research_Data_Factors_daily",
    "Developed":          "Developed_3_Factors_Daily",
    "DevelopedExUS":      "Developed_ex_US_3_Factors_Daily",
    "Europe":             "Europe_3_Factors_Daily",
    "Japan":              "Japan_3_Factors_Daily",
    "AsiaPacificExJapan": "Asia_Pacific_ex_Japan_3_Factors_Daily",
    "NorthAmerica":       "North_America_3_Factors_Daily",
    "EmergingMarkets":    "Emerging_3_Factors_Daily",
}

REGIONS_5F: dict[str, str] = {
    "US":                 "F-F_Research_Data_5_Factors_2x3_daily",
    "Developed":          "Developed_5_Factors_Daily",
    "DevelopedExUS":      "Developed_ex_US_5_Factors_Daily",
    "Europe":             "Europe_5_Factors_Daily",
    "Japan":              "Japan_5_Factors_Daily",
    "AsiaPacificExJapan": "Asia_Pacific_ex_Japan_5_Factors_Daily",
    "NorthAmerica":       "North_America_5_Factors_Daily",
    "EmergingMarkets":    "Emerging_5_Factors_Daily",
}

# Tolerated aliases on the region name so callers can pass natural forms.
_REGION_ALIASES: dict[str, str] = {
    "us": "US", "u.s.": "US", "usa": "US", "united states": "US",
    "developed": "Developed",
    "developedexus": "DevelopedExUS", "developed_ex_us": "DevelopedExUS",
    "europe": "Europe",
    "japan": "Japan",
    "asia_pacific_ex_japan": "AsiaPacificExJapan",
    "asia pacific ex japan": "AsiaPacificExJapan",
    "asiapacificexjapan": "AsiaPacificExJapan",
    "asia": "AsiaPacificExJapan",
    "northamerica": "NorthAmerica", "north_america": "NorthAmerica",
    "emerging": "EmergingMarkets",
    "emerging_markets": "EmergingMarkets",
    "emergingmarkets": "EmergingMarkets",
    "em": "EmergingMarkets",
}


def _normalise_region(region: str) -> str:
    """Resolve a region string to a canonical key from REGIONS_3F/5F.

    Accepts the canonical form (``"AsiaPacificExJapan"``) or any of the
    spelled-out aliases (``"asia pacific ex japan"`` /
    ``"asia_pacific_ex_japan"``). Raises ValueError on unknown values.
    """
    if region in REGIONS_3F:
        return region
    key = (region or "").strip().lower().replace("-", "_")
    canonical = _REGION_ALIASES.get(key)
    if canonical is None:
        raise ValueError(
            f"Unknown region {region!r}. Supported: "
            f"{sorted(REGIONS_3F.keys())}"
        )
    return canonical


def list_regions() -> list[str]:
    """Return the canonical list of supported region keys."""
    return list(REGIONS_3F.keys())


def get_fama_french_factors(
    factor_model: str = "3",
    region: str = "US",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    as_decimal: bool = True,
) -> "pd.DataFrame":
    """
    Fetch daily Fama-French factor returns (3- or 5-factor models) for
    any of the 8 regions Ken French publishes.

    Parameters
    ----------
    factor_model : str, optional
        ``"3"`` for the 3-factor set or ``"5"`` for the 5-factor set.
    region : str, optional, default ``"US"``
        One of: ``US, Developed, DevelopedExUS, Europe, Japan,
        AsiaPacificExJapan, NorthAmerica, EmergingMarkets``. Aliases
        like ``"emerging"`` and ``"asia pacific ex japan"`` are accepted.
    start_date : str or None, optional
        Inclusive start date, ``"YYYY-MM-DD"``. ``None`` returns all history.
    end_date : str or None, optional
        Inclusive end date, ``"YYYY-MM-DD"``. ``None`` returns all history.
    as_decimal : bool, optional
        If ``True`` (default), divide values by 100 to convert percentage
        returns to decimals.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows with columns:
        3-factor: ``Mkt-RF``, ``SMB``, ``HML``, ``RF``.
        5-factor: ``Mkt-RF``, ``SMB``, ``HML``, ``RMW``, ``CMA``, ``RF``.

    Raises
    ------
    ValueError
        If ``factor_model`` is not ``"3"`` or ``"5"``, ``region`` is
        unknown, or date inputs are invalid.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> # US 5-factor since 2020
    >>> df = get_fama_french_factors(factor_model="5", region="US",
    ...                              start_date="2020-01-01")
    >>> # Asia Pacific ex Japan 3-factor (closest fit for Indian equities)
    >>> df = get_fama_french_factors(region="AsiaPacificExJapan")
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        from pandas_datareader import data as pdr_data
    except ImportError as exc:
        raise ImportError(
            "pandas-datareader is required. Install: pip install pandas-datareader"
        ) from exc

    model_key = str(factor_model).strip()
    if model_key == "3":
        region_map = REGIONS_3F
    elif model_key == "5":
        region_map = REGIONS_5F
    else:
        raise ValueError(
            f"factor_model must be '3' or '5'. Received: {factor_model!r}"
        )

    canonical_region = _normalise_region(region)
    dataset_name = region_map[canonical_region]

    start_dt = None
    end_dt = None

    if start_date is not None:
        try:
            start_dt = pd.to_datetime(start_date)
        except Exception as exc:
            raise ValueError("start_date must be a valid date string.") from exc

    if end_date is not None:
        try:
            end_dt = pd.to_datetime(end_date)
        except Exception as exc:
            raise ValueError("end_date must be a valid date string.") from exc

    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date.")

    try:
        raw = pdr_data.DataReader(dataset_name, "famafrench")
    except Exception as exc:
        raise ValueError(
            f"Failed to download Fama-French data from the Ken French library "
            f"(model={model_key}, region={canonical_region}, dataset={dataset_name})."
        ) from exc

    if not isinstance(raw, dict) or 0 not in raw:
        raise ValueError(
            "Unexpected data format returned by pandas-datareader for famafrench."
        )

    df = raw[0].copy()
    if df.empty:
        raise ValueError(
            f"No data returned for the requested factor set "
            f"(model={model_key}, region={canonical_region})."
        )

    # The US dataset is YYYYMMDD-formatted; the regional ones can come
    # back as PeriodIndex (monthly) for some endpoints. The daily
    # regional datasets are date-typed already in newer pandas-datareader,
    # but we coerce defensively.
    try:
        df.index = pd.to_datetime(df.index)
    except Exception as exc:
        raise ValueError("Failed to parse dates in Fama-French data.") from exc

    df.index.name = "date"

    if start_dt is not None:
        df = df.loc[df.index >= start_dt]

    if end_dt is not None:
        df = df.loc[df.index <= end_dt]

    if as_decimal:
        df = df / 100.0

    return df
