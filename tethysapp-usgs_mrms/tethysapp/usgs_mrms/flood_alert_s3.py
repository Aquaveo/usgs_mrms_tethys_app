from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .s3_config import bucket_name, full_key, get_bucket


def download_s3_file_if_missing(*, s3_key: str, local_fp: Path) -> Path:
    local_fp = Path(local_fp)
    local_fp.parent.mkdir(parents=True, exist_ok=True)

    if local_fp.exists() and local_fp.stat().st_size > 0:
        print(f"[SKIP] exists: {local_fp}", flush=True)
        return local_fp

    bucket = get_bucket()
    key = full_key(s3_key)
    print(f"[DOWNLOAD] s3://{bucket_name()}/{key} -> {local_fp}", flush=True)
    bucket.download_file(key, str(local_fp))
    return local_fp


def _download_one_json(obj_key: str, local_fp: Path) -> Path:
    local_fp = Path(local_fp)
    local_fp.parent.mkdir(parents=True, exist_ok=True)

    if local_fp.exists() and local_fp.stat().st_size > 0:
        return local_fp

    bucket = get_bucket()
    bucket.download_file(obj_key, str(local_fp))
    return local_fp


def download_s3_prefix_jsons(
    *,
    s3_prefix: str,
    local_dir: Path,
    workers: int = 4,
    only_stems: set[str] | None = None,
) -> list[Path]:
    # only_stems limits the download to objects whose filename stem is in the set
    # (e.g. just the alerted basins), instead of the whole prefix.
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    bucket = get_bucket()
    prefix = full_key(s3_prefix)
    objects = [obj for obj in bucket.objects.filter(Prefix=prefix) if obj.key.endswith(".json")]

    if only_stems is not None:
        want = {str(s) for s in only_stems}
        objects = [obj for obj in objects if Path(obj.key).stem in want]
    elif not objects:
        raise FileNotFoundError(f"No JSON files found in s3://{bucket_name()}/{prefix}")

    tasks = []
    downloaded: list[Path] = []

    for obj in objects:
        local_fp = local_dir / Path(obj.key).name

        if local_fp.exists() and local_fp.stat().st_size > 0:
            downloaded.append(local_fp)
        else:
            tasks.append((obj.key, local_fp))

    print(
        f"[BASIN JSON] prefix=s3://{bucket_name()}/{prefix} "
        f"total={len(objects)} existing={len(downloaded)} to_download={len(tasks)} workers={workers}",
        flush=True,
    )

    if not tasks:
        return downloaded

    workers = max(1, min(int(workers), len(tasks)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_download_one_json, obj_key, local_fp): (obj_key, local_fp)
            for obj_key, local_fp in tasks
        }

        for n, future in enumerate(as_completed(futures), start=1):
            obj_key, local_fp = futures[future]
            try:
                fp = future.result()
                downloaded.append(fp)

                if n % 25 == 0 or n == len(tasks):
                    print(f"[BASIN JSON] downloaded {n}/{len(tasks)}", flush=True)

            except Exception as e:
                raise RuntimeError(f"Failed downloading s3://{bucket_name()}/{obj_key} -> {local_fp}: {e}") from e

    return downloaded


def download_flood_alert_inputs(
    *,
    base_dir: Path,
    state: str,
    workers: int = 4,
) -> dict[str, Path]:
    base_dir = Path(base_dir)
    state = state.upper()

    state_mask_fp = download_s3_file_if_missing(
        s3_key=f"state_masks/{state}_mrms_mask.npz",
        local_fp=base_dir / "state_masks" / f"{state}_mrms_mask.npz",
    )

    state_basin_index_fp = download_s3_file_if_missing(
        s3_key=f"state_basin_index/{state}_state_basin_index.npz",
        local_fp=base_dir / "state_basin_index" / f"{state}_state_basin_index.npz",
    )

    hydro_history_s3_prefix = os.getenv(
        "HYDRO_HISTORY_S3_PREFIX",
        "experiments/hydro_history_3mm_all_stage",
    ).strip("/")

    pixel_event_index_fp = download_s3_file_if_missing(
        s3_key=f"{hydro_history_s3_prefix}/state_pixel_event_index/{state}_pixel_event_index.npz",
        local_fp=base_dir / "hydro_history" / "state_pixel_event_index" / f"{state}_pixel_event_index.npz",
    )

    efficient_event_reference_fp = download_s3_file_if_missing(
        s3_key=f"{hydro_history_s3_prefix}/state_efficient_event_reference/{state}_efficient_event_reference.npz",
        local_fp=base_dir / "hydro_history" / "state_efficient_event_reference" / f"{state}_efficient_event_reference.npz",
    )

    # Basin geometries are fetched on demand for only the alerted basins (see
    # flood_alert_service) rather than bulk-downloaded here — a run uses a handful,
    # but the full set is hundreds of files / hundreds of MiB.
    basin_json_dir = base_dir / "basins_json" / state

    return {
        "state_mask_fp": state_mask_fp,
        "state_basin_index_fp": state_basin_index_fp,
        "pixel_event_index_fp": pixel_event_index_fp,
        "efficient_event_reference_fp": efficient_event_reference_fp,
        "basin_json_dir": basin_json_dir,
    }