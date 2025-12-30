from __future__ import annotations

import dataclasses
import datetime as dt
import math
from typing import Dict, Iterable, List, Tuple


Category = str


@dataclasses.dataclass(frozen=True)
class SeasonDef:
    name: str
    start_month: int
    start_day: int


COUNTRY_SEASONS: dict[str, list[SeasonDef]] = {
    # Approximate national patterns.
    "rw": [SeasonDef("A", 2, 10), SeasonDef("B", 9, 10)],
    "ke": [SeasonDef("Long", 3, 15), SeasonDef("Short", 10, 10)],
    "ug": [SeasonDef("1", 3, 15), SeasonDef("2", 9, 10)],
}


@dataclasses.dataclass(frozen=True)
class CategoryCurve:
    lag_days: int
    amplitude: float
    ramp_days: int
    peak_days: int
    decay_days: int


CATEGORY_CURVES: dict[Category, CategoryCurve] = {
    "Seeds": CategoryCurve(lag_days=0, amplitude=2.2, ramp_days=14, peak_days=28, decay_days=14),
    "Fertilizer": CategoryCurve(lag_days=18, amplitude=1.6, ramp_days=14, peak_days=28, decay_days=14),
    "Pesticides": CategoryCurve(lag_days=35, amplitude=1.2, ramp_days=10, peak_days=28, decay_days=10),
    "Tools": CategoryCurve(lag_days=7, amplitude=0.35, ramp_days=10, peak_days=35, decay_days=10),
    "Spare Parts": CategoryCurve(lag_days=7, amplitude=0.30, ramp_days=10, peak_days=35, decay_days=10),
    "Packaging": CategoryCurve(lag_days=75, amplitude=0.9, ramp_days=14, peak_days=28, decay_days=14),
}


def _piecewise_pulse(days_since_start: int, curve: CategoryCurve) -> float:
    if days_since_start < 0:
        return 0.0
    if days_since_start <= curve.ramp_days:
        return max(0.0, days_since_start / max(curve.ramp_days, 1))
    if days_since_start <= curve.ramp_days + curve.peak_days:
        return 1.0
    if days_since_start <= curve.ramp_days + curve.peak_days + curve.decay_days:
        t = days_since_start - (curve.ramp_days + curve.peak_days)
        return max(0.0, 1.0 - t / max(curve.decay_days, 1))
    return 0.0


def _season_start_for_year(season: SeasonDef, year: int) -> dt.date:
    return dt.date(year, season.start_month, season.start_day)


def seasonal_multiplier(country_code: str, category: Category, day: dt.date) -> float:
    cc = country_code.lower()
    seasons = COUNTRY_SEASONS[cc]
    curve = CATEGORY_CURVES[category]

    pulses = 0.0
    for s in seasons:
        start = _season_start_for_year(s, day.year)
        # Also consider the previous year's season for dates early in the year.
        for season_start in (start, _season_start_for_year(s, day.year - 1)):
            d = (day - season_start).days - curve.lag_days
            pulses += _piecewise_pulse(d, curve)

    # Base 1.0 plus seasonal lift.
    return 1.0 + curve.amplitude * min(pulses, 1.25)


def weekday_multiplier(kind: str, day: dt.date) -> float:
    """Operational patterns by movement kind.

    kind: inbound|outbound|internal|damage
    """
    wd = day.weekday()  # Mon=0 ... Sun=6

    match (kind, wd):
        case ("outbound", 6):
            return 0.15
        case ("outbound", 5):
            return 0.65
        case ("outbound", _):
            return 1.0
        case ("inbound", 5 | 6):
            return 0.25
        case ("inbound", _):
            return 1.0
        case ("internal", 1 | 2 | 3):
            return 1.2
        case ("internal", 6):
            return 0.4
        case ("internal", _):
            return 0.9
        case ("damage", _):
            return 1.0
        case _:
            raise ValueError(f"Unknown kind={kind}")


def bounded_normal(mean: float, stdev: float, *, rng) -> float:
    # Lightly bounded random normal for realism without outliers dominating.
    val = rng.gauss(mean, stdev)
    return max(0.0, min(val, mean + 4 * stdev))


def demand_intensity(country_code: str, category: Category, day: dt.date, *, rng) -> float:
    base = seasonal_multiplier(country_code, category, day) * weekday_multiplier("outbound", day)
    # Add small noise so two weeks don't look identical.
    return base * (0.9 + 0.2 * rng.random())
