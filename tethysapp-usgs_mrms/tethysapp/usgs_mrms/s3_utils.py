import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from .s3_config import full_key, get_bucket

MAX_WORKERS = 4


def _download_one_file(obj_key, local_path):
    bucket = get_bucket()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    bucket.download_file(obj_key, local_path)


def _download_files_parallel(download_jobs, max_workers=MAX_WORKERS):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_download_one_file, obj_key, local_path)
            for obj_key, local_path in download_jobs
        ]

        for future in as_completed(futures):
            future.result()


def download_basin_geojson_files(state_name, destination_path):
    bucket = get_bucket()
    prefix = full_key(f"basins_json/{state_name}")
    dest = f"{destination_path}/basin_json_downloaded_files"
    state_dest = f"{dest}/{state_name}"

    os.makedirs(dest, exist_ok=True)

    if os.path.exists(state_dest):
        return

    objects = [
        obj for obj in bucket.objects.filter(Prefix=prefix)
        if obj.key.endswith(".json")
    ]

    if not objects:
        raise FileNotFoundError(
            f"No basin JSON files found in S3 for state {state_name} with prefix {prefix}"
        )

    if len(os.listdir(dest)) >= 5:
        oldest_dir = min(
            os.listdir(dest),
            key=lambda f: os.path.getctime(os.path.join(dest, f)),
        )
        shutil.rmtree(os.path.join(dest, oldest_dir))

    os.makedirs(state_dest, exist_ok=True)

    download_jobs = [
        (
            obj.key,
            os.path.join(state_dest, os.path.basename(obj.key)),
        )
        for obj in objects
    ]

    _download_files_parallel(download_jobs)


def download_zarr_file(state_name, gage_id, destination_path):
    first_folder = gage_id[:2]
    second_folder = gage_id[:4]
    bucket = get_bucket()

    zarr_prefix = full_key(
        f"rain_zarr/{state_name}/{first_folder}/{second_folder}/{gage_id}.zarr"
    )

    dest = f"{destination_path}/zarr_files"
    local_zarr_path = os.path.join(dest, f"{gage_id}.zarr")

    os.makedirs(dest, exist_ok=True)

    if os.path.exists(local_zarr_path):
        return

    existing_files = os.listdir(dest)

    if len(existing_files) >= 5:
        oldest_file = min(
            existing_files,
            key=lambda f: os.path.getctime(os.path.join(dest, f)),
        )
        shutil.rmtree(os.path.join(dest, oldest_file))

    objects = list(bucket.objects.filter(Prefix=zarr_prefix))

    if len(objects) == 0:
        raise FileNotFoundError(
            f"No Zarr files found in S3 for {gage_id} with prefix {zarr_prefix}"
        )

    download_jobs = []

    for obj in objects:
        relative_path = os.path.relpath(obj.key, os.path.dirname(zarr_prefix))
        local_file_path = os.path.join(dest, relative_path)
        download_jobs.append((obj.key, local_file_path))

    _download_files_parallel(download_jobs)
