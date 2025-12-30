from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


_LOCAL_DATA_DIR = Path(__file__).resolve().parent / "data"
_ROOT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
# Prefer data/ next to this module; fall back to repo-level data/
DATA_DIR = _LOCAL_DATA_DIR if _LOCAL_DATA_DIR.exists() else _ROOT_DATA_DIR


_NON_CODE = re.compile(r"[^A-Z0-9_]+")


def slugify(value: str, *, upper: bool = True) -> str:
    value = value.strip()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[-/]+", "_", value)
    value = value.upper() if upper else value.lower()
    value = _NON_CODE.sub("", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def readable(value: str) -> str:
    value = value.strip()
    value = re.sub(r"\s+", " ", value)
    return value


@dataclass(frozen=True)
class WarehouseGeo:
    country_code: str
    warehouse_name: str
    warehouse_slug: str
    base_unit_names: list[str]


def _normalize_warehouse_name(value: str) -> str:
    value = readable(value)
    # If data sources are all-caps (e.g., Kenya), keep readable Title Case.
    if value.isupper():
        return value.title()
    return value


def _read_geo_json(path: Path) -> dict[str, list[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    raw: dict[str, list[str]] = {}

    if isinstance(payload, dict):
        items = payload.items()
        for name, units in items:
            warehouse = _normalize_warehouse_name(str(name))
            if units is None:
                units = []
            if not isinstance(units, list):
                raise ValueError(f"Invalid geo JSON value for {warehouse!r} in {path}: expected list")
            base_units: list[str] = []
            for unit in units:
                unit_value = str(unit).strip()
                if unit_value:
                    base_units.append(_normalize_warehouse_name(unit_value))
            raw[warehouse] = base_units

    elif isinstance(payload, list):
        for idx, item in enumerate(payload):
            if not isinstance(item, dict):
                raise ValueError(f"Invalid geo JSON item at index {idx} in {path}: expected object")
            if "name" not in item:
                raise ValueError(f"Invalid geo JSON item at index {idx} in {path}: missing 'name'")
            warehouse = _normalize_warehouse_name(str(item["name"]))
            units = item.get("base_units", [])
            if units is None:
                units = []
            if not isinstance(units, list):
                raise ValueError(
                    f"Invalid geo JSON base_units for {warehouse!r} at index {idx} in {path}: expected list"
                )
            base_units: list[str] = []
            for unit in units:
                unit_value = str(unit).strip()
                if unit_value:
                    base_units.append(_normalize_warehouse_name(unit_value))
            raw[warehouse] = base_units

    else:
        raise ValueError(f"Invalid geo JSON in {path}: expected object or array")

    if not raw:
        raise ValueError(f"No geo records parsed from {path}")
    return raw


def _read_geo_txt(path: Path) -> dict[str, list[str]]:
    raw: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        head, tail = line.split(":", 1)
        warehouse = _normalize_warehouse_name(head)
        # Expect format like: District: [Sector1, Sector2]
        tail = tail.strip()
        if not tail.startswith("[") or not tail.endswith("]"):
            raise ValueError(f"Invalid geo line (missing brackets) in {path}: {line}")
        inner = tail[1:-1].strip()
        if not inner:
            base_units = []
        else:
            base_units = [_normalize_warehouse_name(v.strip()) for v in inner.split(",") if v.strip()]
        raw[warehouse] = base_units
    if not raw:
        raise ValueError(f"No geo records parsed from {path}")
    return raw


def _read_geo_file(country_code: str) -> dict[str, list[str]]:
    cc = country_code.lower()
    json_path = DATA_DIR / f"geo_data_{cc}.json"
    txt_path = DATA_DIR / f"geo_data_{cc}.txt"

    if json_path.exists():
        return _read_geo_json(json_path)
    if txt_path.exists():
        return _read_geo_txt(txt_path)

    raise FileNotFoundError(f"Missing geo data file: {json_path}")

# reads geo data files - ie: geo_data_rw.json warehouse and base units and caches results
@lru_cache(maxsize=8)
def _country_geo(country_code: str) -> dict[str, dict[str, list[str]]]:
    """Returns {warehouse_slug: {warehouse_name, base_units}}."""
    cc = country_code.lower()
    data = _read_geo_file(cc)
    out: dict[str, dict[str, list[str]]] = {}
    for wh_name, units in data.items():
        wh_slug = slugify(wh_name)
        out[wh_slug] = {"name": wh_name, "base_units": list(units)}
    return out


# returns list of warehouse names for a given country code
def warehouses_for_country(country_code: str) -> list[str]:
    cc = country_code.lower()
    geo = _country_geo(cc)
    # Preserve file order if possible.
    return [v["name"] for _, v in geo.items()]


def base_unit_label(country_code: str) -> str:
    cc = country_code.lower()
    if cc == "rw":
        return "Sector"
    if cc == "ke":
        return "Sub-county"
    if cc == "ug":
        return "County"
    raise ValueError(f"Unsupported country_code={country_code}")


def generate_base_units(country_code: str, warehouse_name: str, *, count: int) -> list[str]:
    cc = country_code.lower()
    geo = _country_geo(cc)
    wh_slug = slugify(_normalize_warehouse_name(warehouse_name))
    if wh_slug not in geo or not geo[wh_slug]["base_units"]:
        label = base_unit_label(country_code)
        return [f"{label} {i:03d}" for i in range(1, count + 1)]
    units = list(geo[wh_slug]["base_units"])
    return units[: min(count, len(units))]


def geo_plan(
    country_code: str,
    *,
    scale: str,
    full_geo: bool,
    max_warehouses: int | None = None,
    base_units_per_wh: int | None = None,
) -> list[WarehouseGeo]:
    scale = scale.lower()
    wh_names = warehouses_for_country(country_code)

    if max_warehouses is None:
        if scale == "small":
            max_warehouses = 5
        elif scale == "medium":
            max_warehouses = 10
        elif scale == "large":
            max_warehouses = len(wh_names)
        else:
            raise ValueError("scale must be small|medium|large")

    wh_names = wh_names[: max_warehouses]

    if base_units_per_wh is None:
        # Default behavior:
        # - without --full-geo: take a subset per warehouse
        # - with --full-geo: use all base units from the data files
        if not full_geo:
            base_units_per_wh = 20 if scale != "small" else 10

    plan: list[WarehouseGeo] = []
    geo = _country_geo(country_code)
    for wh in wh_names:
        wh_name = _normalize_warehouse_name(wh)
        wh_slug = slugify(wh_name)
        full_base_units_list = list(geo.get(wh_slug, {}).get("base_units", []))
        if not full_base_units_list:
            fallback_n = base_units_per_wh if base_units_per_wh is not None else (20 if scale != "small" else 10)
            full_base_units_list = generate_base_units(country_code, wh_name, count=fallback_n)
        if full_geo:
            base_units = full_base_units_list if base_units_per_wh is None else full_base_units_list[: min(base_units_per_wh, len(full_base_units_list))]
        else:
            n = base_units_per_wh or 0
            if n <= 0:
                n = 20 if scale != "small" else 10
            base_units = full_base_units_list[: min(n, len(full_base_units_list))]
        plan.append(
            WarehouseGeo(
                country_code=country_code.lower(),
                warehouse_name=wh_name,
                warehouse_slug=wh_slug,
                base_unit_names=base_units,
            )
        )
    return plan
