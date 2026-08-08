from __future__ import annotations

import json

import pandas as pd
from django.http import JsonResponse
from tethys_sdk.routing import controller

from ..app import App
from ..flood_alert_service import run_flood_alert_job
from ..flood_alert_storage import BASIN_GEOJSON, storage
from ..flood_alert_utils import build_run_id
from ..jobs import get_job_manager


STATES = [
    ("ALABAMA", "Alabama"),
    ("ARIZONA", "Arizona"),
    ("ARKANSAS", "Arkansas"),
    ("CALIFORNIA", "California"),
    ("COLORADO", "Colorado"),
    ("CONNECTICUT", "Connecticut"),
    ("DELAWARE", "Delaware"),
    ("DISTRICT_OF_COLUMBIA", "District of Columbia"),
    ("FLORIDA", "Florida"),
    ("GEORGIA", "Georgia"),
    ("IDAHO", "Idaho"),
    ("ILLINOIS", "Illinois"),
    ("INDIANA", "Indiana"),
    ("IOWA", "Iowa"),
    ("KANSAS", "Kansas"),
    ("KENTUCKY", "Kentucky"),
    ("LOUISIANA", "Louisiana"),
    ("MAINE", "Maine"),
    ("MARYLAND", "Maryland"),
    ("MASSACHUSETTS", "Massachusetts"),
    ("MICHIGAN", "Michigan"),
    ("MINNESOTA", "Minnesota"),
    ("MISSISSIPPI", "Mississippi"),
    ("MISSOURI", "Missouri"),
    ("MONTANA", "Montana"),
    ("NEBRASKA", "Nebraska"),
    ("NEVADA", "Nevada"),
    ("NEW_HAMPSHIRE", "New Hampshire"),
    ("NEW_JERSEY", "New Jersey"),
    ("NEW_MEXICO", "New Mexico"),
    ("NEW_YORK", "New York"),
    ("NORTH_CAROLINA", "North Carolina"),
    ("NORTH_DAKOTA", "North Dakota"),
    ("OHIO", "Ohio"),
    ("OKLAHOMA", "Oklahoma"),
    ("OREGON", "Oregon"),
    ("PENNSYLVANIA", "Pennsylvania"),
    ("RHODE_ISLAND", "Rhode Island"),
    ("SOUTH_CAROLINA", "South Carolina"),
    ("SOUTH_DAKOTA", "South Dakota"),
    ("TENNESSEE", "Tennessee"),
    ("TEXAS", "Texas"),
    ("UTAH", "Utah"),
    ("VERMONT", "Vermont"),
    ("VIRGINIA", "Virginia"),
    ("WASHINGTON", "Washington"),
    ("WEST_VIRGINIA", "West Virginia"),
    ("WISCONSIN", "Wisconsin"),
    ("WYOMING", "Wyoming"),
]


ALERT_COLORS = {
    "NORMAL": "#3BA55D",
    "WATCH": "#F1C40F",
    "WARNING": "#E67E22",
    "SEVERE": "#E74C3C",
}


def _normalize_datetime_from_form(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return value
    if "T" in value:
        value = value.replace("T", " ")
    if len(value) == 16:
        value = value + ":00"
    return value


def _json_safe(v):
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        return v.item()
    return v


def _geometry_bbox_area(geometry: dict | None) -> float:
    if not geometry:
        return 0.0

    coords = geometry.get("coordinates")
    if not coords:
        return 0.0

    xs = []
    ys = []

    def collect(obj):
        if isinstance(obj, list):
            if len(obj) >= 2 and all(isinstance(x, (int, float)) for x in obj[:2]):
                xs.append(float(obj[0]))
                ys.append(float(obj[1]))
            else:
                for item in obj:
                    collect(item)

    collect(coords)

    if not xs or not ys:
        return 0.0

    return (max(xs) - min(xs)) * (max(ys) - min(ys))


def _pixel_polygon_from_center(lon: float, lat: float, dx: float = 0.01, dy: float = 0.01) -> dict:
    half_dx = dx / 2.0
    half_dy = dy / 2.0
    x0 = lon - half_dx
    x1 = lon + half_dx
    y0 = lat - half_dy
    y1 = lat + half_dy

    return {
        "type": "Polygon",
        "coordinates": [[
            [x0, y0],
            [x1, y0],
            [x1, y1],
            [x0, y1],
            [x0, y0],
        ]],
    }


@controller(name="flood_alert", url="flood-alert/")
def flood_alert(request):
    return App.render(request, "flood_alert.html", {"states": STATES})

@controller(name="run_flood_alert", url="flood-alert/run/")
def run_flood_alert(request):
    """Submit a flood-alert run as a background job (dedup + status in the DB).

    Renders the processing page, which polls ``flood-alert/status/<job_id>`` and
    redirects to the results once the job succeeds. Reuses a completed run whose
    outputs already exist in shared storage.
    """
    if request.method != "POST":
        return App.render(request, "flood_alert.html", {"states": STATES})

    state = request.POST.get("state", "").upper().strip()
    if request.POST.get("run_id"):
        run_id = request.POST["run_id"]
    else:
        start_dt = _normalize_datetime_from_form(request.POST.get("start_datetime", ""))
        end_dt = _normalize_datetime_from_form(request.POST.get("end_datetime", ""))
        run_id = build_run_id(start_dt, end_dt)
    workers = int(request.POST.get("workers", "4"))

    base_context = {
        "state": state.lower(),
        "run_id": run_id,
        "workers": workers,
        "process_type": "flood_alert",
    }

    # A finished run is already in shared storage -> any replica can serve it.
    if storage.run_exists(state, run_id):
        return App.render(request, "processing.html", {
            **base_context,
            "job_status": "success",
            "message": "This run already exists. Loading results…",
        })

    job, created = get_job_manager().submit(
        state=state,
        run_id=run_id,
        key=f"{state}:{run_id}",
        params={"workers": workers},
        runner=run_flood_alert_job,
    )

    return App.render(request, "processing.html", {
        **base_context,
        "job_id": job["id"],
        "message": "Generating flood alert results…",
    })


@controller(name="flood_alert_status", url="flood-alert/status/{job_id}/")
def flood_alert_status(request, job_id):
    """Current state of one background flood-alert job (polled by the UI)."""
    job = get_job_manager().get(job_id)
    if job is None:
        return JsonResponse({"status": "error", "message": "No such job"}, status=404)
    return JsonResponse({
        "status": "success",
        "job": {
            "job_id": job["id"],
            "state": job["state"],
            "run_id": job["run_id"],
            "job_status": job["status"],
            "message": job["message"],
        },
    })
    
        


@controller(
    name="flood_alert_results",
    url="flood-alert/results/{state}/{run_id}/",
)
def flood_alert_results(request, state, run_id):
    state = state.upper()
    context = {
        "state": state,
        "run_id": run_id,
        "basin_geojson_exists": storage.run_exists(state, run_id),
        "pixel_parquet_exists": storage.exists(state, run_id, "pixel_alerts.parquet"),
    }
    return App.render(request, "flood_alert_results.html", context)


@controller(
    name="flood_alert_basin_geojson",
    url="flood-alert/geojson/{state}/{run_id}/basins/")
def flood_alert_basin_geojson(request, state, run_id):
    state = state.upper()

    with storage.local(state, run_id, BASIN_GEOJSON) as fp:
        if fp is None:
            return JsonResponse({"error": "Missing basin GeoJSON"}, status=404)
        with open(fp, "r", encoding="utf-8") as f:
            obj = json.load(f)

    relevant_levels = {"WARNING", "SEVERE"}

    features = [
        feat
        for feat in obj.get("features", [])
        if feat.get("properties", {}).get("alert_level") in relevant_levels
    ]

    for feat in features:
        props = feat.setdefault("properties", {})
        alert_level = props.get("alert_level")
        props["fill_color"] = ALERT_COLORS.get(alert_level, "#808080")
        props["stroke_color"] = "#222222"

    features = sorted(
        features,
        key=lambda feat: _geometry_bbox_area(feat.get("geometry")),
        reverse=True,
    )

    obj["features"] = features
    obj.setdefault("metadata", {})
    obj["metadata"]["filtered_to_relevant_alerts"] = True
    obj["metadata"]["included_alert_levels"] = sorted(relevant_levels)
    obj["metadata"]["filtered_n_features"] = len(features)
    obj["metadata"]["sorted_large_to_small_for_clickability"] = True

    return JsonResponse(obj, safe=False)


@controller(
    name="flood_alert_pixel_geojson",
    url="flood-alert/geojson/{state}/{run_id}/pixels/",
)
def flood_alert_pixel_geojson(request, state, run_id):
    state = state.upper()

    site_id = request.GET.get("site_id")
    max_pixels = int(request.GET.get("max_pixels", "5000"))

    if not site_id:
        return JsonResponse({"error": "Missing required query parameter: site_id"}, status=400)

    with storage.local(state, run_id, "pixel_alerts.parquet") as parquet_fp:
        if parquet_fp is None:
            return JsonResponse({"error": "Missing pixel alerts parquet"}, status=404)
        df = pd.read_parquet(parquet_fp)

    if df.empty:
        return JsonResponse(
            {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "state": state,
                    "site_id": site_id,
                    "n_features": 0,
                    "note": "pixel_alerts.parquet is empty",
                },
            },
            safe=False,
        )

    df = df[df["site_id"].astype(str) == str(site_id)].copy()

    if df.empty:
        return JsonResponse(
            {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "state": state,
                    "site_id": site_id,
                    "n_features": 0,
                    "note": "No pixel alerts found for site_id",
                },
            },
            safe=False,
        )

    sort_cols = [c for c in ["current_pixel_accumulation", "current_pixel_value"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=False)

    if max_pixels is not None and len(df) > max_pixels:
        df = df.head(max_pixels).copy()

    property_cols = [
        "state",
        "site_id",
        "pixel_id_state",
        "pixel_id_basin",
        "row",
        "col",
        "current_pixel_value",
        "current_pixel_accumulation",
        "current_basin_accumulation",
        "historical_basin_accumulation_threshold",
        "historical_pixel_best_event_id",
        "historical_pixel_best_delta_water_stage",
        "historical_pixel_best_basin_accumulation",
        "historical_pixel_best_pixel_accumulation",
        "efficient_pixel_percentile",
        "efficient_basin_percentile",
        "efficient_weighted_percentile",
    ]

    features = []

    for _, row in df.iterrows():
        lon = float(row["lon"])
        lat = float(row["lat"])

        props = {c: _json_safe(row[c]) for c in property_cols if c in row.index}

        
        alert_level = "SEVERE"
        

        props["alert_level"] = alert_level
        props["fill_color"] = ALERT_COLORS.get(alert_level, "#808080")
        props["stroke_color"] = "#222222"

        features.append(
            {
                "type": "Feature",
                "geometry": _pixel_polygon_from_center(lon, lat),
                "properties": props,
            }
        )

    return JsonResponse(
        {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "state": state,
                "site_id": site_id,
                "n_features": len(features),
                "source": storage.key_for(state, run_id, "pixel_alerts.parquet"),
                "dynamic_from_parquet": True,
                "max_pixels": max_pixels,
            },
        },
        safe=False,
    )