from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from time import perf_counter
from typing import Callable, Optional

import pandas as pd

from .flood_alert_s3 import download_flood_alert_inputs, download_s3_prefix_jsons
from .flood_alert_storage import BASIN_GEOJSON, storage
from .flood_alert_utils import build_run_directory, build_run_id, get_times_from_run_id

from mrms_usgs_events.ews.state_rain import build_current_state_rain_npz
from mrms_usgs_events.ews.current_alerts import compute_current_alerts_for_state
from mrms_usgs_events.ews.tethys_outputs import export_basin_alerts_geojson


def flood_alert_workdir() -> Path:
    """Per-replica local scratch dir for compute + reference-data caching.

    Intermediates (downloaded reference npz, current-rain arrays) live here on
    local disk; only the final run outputs are published to shared storage.
    """
    root = Path(os.getenv("USGS_MRMS_WORKDIR", str(Path(tempfile.gettempdir()) / "usgs_mrms_work")))
    root.mkdir(parents=True, exist_ok=True)
    return root


def run_flood_alert_job(job: dict, progress: Callable[[str], None]) -> str:
    """JobManager runner: compute the run, publish outputs to shared storage.

    Returns the storage prefix of the published run (the job's ``result_key``).
    """
    state = job["state"].upper()
    run_id = job["run_id"]
    params = job.get("params") or {}
    start, end = get_times_from_run_id(run_id)
    workers = int(params.get("workers", 4))

    result = run_flood_alert_pipeline(
        base_dir=flood_alert_workdir(),
        state=state,
        start=start,
        end=end,
        workers=workers,
        progress=progress,
    )

    progress("Publishing results…")
    run_dir = Path(result["run_dir"])
    for fname in (BASIN_GEOJSON, "basin_alerts.csv", "pixel_alerts.csv"):
        fp = run_dir / fname
        if fp.exists():
            storage.publish(fp, state, run_id, fname)
    # pixel parquet drives the on-demand pixel GeoJSON endpoint
    pixel_parquet = Path(result["exports"]["pixel_alerts_parquet"])
    if pixel_parquet.exists():
        storage.publish(pixel_parquet, state, run_id, "pixel_alerts.parquet")

    return storage.run_prefix(state, run_id)


def run_flood_alert_pipeline(
    *,
    base_dir: Path,
    state: str,
    start: str,
    end: str,
    workers: int = 4,
    progress: Optional[Callable[[str], None]] = None,
) -> dict:
    state = state.upper()
    base_dir = Path(base_dir)

    def _report(msg: str) -> None:
        print(msg, flush=True)
        if progress is not None:
            progress(msg)

    run_id = build_run_id(start, end)
    run_dir = build_run_directory(base_dir, state, start, end)

    print("=" * 100, flush=True)
    print("TETHYS FLOOD ALERT PIPELINE", flush=True)
    print("=" * 100, flush=True)
    print(f"state   : {state}", flush=True)
    print(f"start   : {start}", flush=True)
    print(f"end     : {end}", flush=True)
    print(f"workers : {workers}", flush=True)
    print(f"base_dir: {base_dir}", flush=True)
    print(f"run_id  : {run_id}", flush=True)
    print(f"run_dir : {run_dir}", flush=True)

    total_t0 = perf_counter()

    _report("Downloading reference data…")
    t0 = perf_counter()
    inputs = download_flood_alert_inputs(
        base_dir=base_dir,
        state=state,
        workers=workers,
    )
    print(f"[TIME] download inputs: {perf_counter() - t0:.2f} sec", flush=True)

    _report("Building current rainfall…")
    t1 = perf_counter()
    current_rain_npz = base_dir / "current_rain" / f"{state}_{run_id}_current_rain.npz"

    build_current_state_rain_npz(
        state=state,
        state_mask_fp=inputs["state_mask_fp"],
        out_npz=current_rain_npz,
        base_dir=base_dir,
        start=start,
        end=end,
        workers=workers,
    )
    print(f"[TIME] build current rain: {perf_counter() - t1:.2f} sec", flush=True)

    _report("Computing flood alerts…")
    t2 = perf_counter()

    efficient_event_reference_fp = (
    base_dir
    / "hydro_history"
    / "state_efficient_event_reference"
    / f"{state}_efficient_event_reference.npz"

    )

    alerts_result = compute_current_alerts_for_state(
        state=state,
        current_rain_npz=current_rain_npz,
        state_basin_index_npz=inputs["state_basin_index_fp"],
        pixel_event_index_npz=inputs["pixel_event_index_fp"],
        efficient_event_reference_npz=efficient_event_reference_fp,
        out_dir=base_dir / "ews_alerts",
        max_pixels_per_basin_output=250,
        workers=workers,
    )


    print(f"[TIME] compute alerts: {perf_counter() - t2:.2f} sec", flush=True)

    _report("Exporting basin alerts…")
    t3 = perf_counter()

    alerts_dir = base_dir / "ews_alerts" / state
    basin_alerts_parquet = alerts_dir / "basin_alerts.parquet"
    pixel_alerts_parquet = alerts_dir / "pixel_alerts.parquet"

    basin_geojson = run_dir / "basin_alerts.geojson"
    basin_csv = run_dir / "basin_alerts.csv"
    pixel_csv = run_dir / "pixel_alerts.csv"

    relevant_levels = ["SEVERE", "WARNING"]

    # Fetch geometry for ONLY the alerted basins, not the whole state's hundreds.
    # compute_current_alerts already wrote basin_alerts.parquet; the export below
    # uses geometry solely for the SEVERE/WARNING rows.
    basin_df = pd.read_parquet(basin_alerts_parquet)
    if {"alert_level", "site_id"}.issubset(basin_df.columns):
        alerted = set(
            basin_df.loc[basin_df["alert_level"].isin(relevant_levels), "site_id"].astype(str)
        )
        if alerted:
            _report(f"Fetching {len(alerted)} alerted basin geometries…")
            download_s3_prefix_jsons(
                s3_prefix=f"basins_json/{state}/",
                local_dir=base_dir / "basins_json" / state,
                workers=workers,
                only_stems=alerted,
            )

    export_basin_alerts_geojson(
        state=state,
        base_dir=base_dir,
        basin_alerts_parquet=basin_alerts_parquet,
        out_geojson=basin_geojson,
        relevant_levels=relevant_levels,
        max_features=300,
    )

    # Keep lightweight copies for the run folder.
    # Pixel GeoJSON is intentionally NOT generated here.
    if (alerts_dir / "basin_alerts.csv").exists():
        shutil.copy2(alerts_dir / "basin_alerts.csv", basin_csv)

    if (alerts_dir / "pixel_alerts.csv").exists():
        shutil.copy2(alerts_dir / "pixel_alerts.csv", pixel_csv)

    print(f"[TIME] export basin geojson only: {perf_counter() - t3:.2f} sec", flush=True)
    print(f"[TIME] total pipeline: {perf_counter() - total_t0:.2f} sec", flush=True)

    return {
        "state": state,
        "start": start,
        "end": end,
        "workers": workers,
        "base_dir": str(base_dir),
        "run_id": run_id,
        "run_dir": str(run_dir),
        "current_rain_npz": str(current_rain_npz),
        "inputs": {k: str(v) for k, v in inputs.items()},
        "alerts": {k: str(v) for k, v in alerts_result.items()},
        "exports": {
            "basin_geojson": str(basin_geojson),
            "basin_csv": str(basin_csv),
            "pixel_alerts_parquet": str(pixel_alerts_parquet),
            "pixel_csv": str(pixel_csv),
            "out_dir": str(run_dir),
        },
    }