"""Storage of flood-alert run outputs through Django's default file storage.

Outputs are published to ``default_storage`` (S3 when the portal configures it,
local filesystem otherwise), so every portal replica serves the same run — the
pod that computed a run is not necessarily the pod that serves its GeoJSON.
Mirrors fimserve_viewer's results storage.
"""

from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

from django.core.files import File
from django.core.files.storage import default_storage
from django.http import FileResponse

# The main map overlay; its presence marks a run as complete/servable.
BASIN_GEOJSON = "basin_alerts.geojson"


class FloodAlertStorage:
    """Publishes and retrieves flood-alert run files via default storage."""

    prefix = "usgs_mrms/flood_alert_runs"

    def run_prefix(self, state: str, run_id: str) -> str:
        return f"{self.prefix}/{state.upper()}/{run_id}"

    def key_for(self, state: str, run_id: str, filename: str) -> str:
        return f"{self.run_prefix(state, run_id)}/{filename}"

    def publish(self, local_path: Path, state: str, run_id: str, filename: Optional[str] = None) -> str:
        """Publish one local file under the run prefix; return its storage key."""
        local_path = Path(local_path)
        key = self.key_for(state, run_id, filename or local_path.name)
        if default_storage.exists(key):
            default_storage.delete(key)
        with local_path.open("rb") as handle:
            default_storage.save(key, File(handle))
        return key

    def exists(self, state: str, run_id: str, filename: str) -> bool:
        return default_storage.exists(self.key_for(state, run_id, filename))

    def run_exists(self, state: str, run_id: str) -> bool:
        """True when the completed run's basin GeoJSON is present."""
        return self.exists(state, run_id, BASIN_GEOJSON)

    def response(self, state: str, run_id: str, filename: str, content_type: str) -> Optional[FileResponse]:
        """Stream one stored run file, or None when it is absent."""
        key = self.key_for(state, run_id, filename)
        if not default_storage.exists(key):
            return None
        return FileResponse(default_storage.open(key, "rb"), content_type=content_type)

    @contextmanager
    def local(self, state: str, run_id: str, filename: str):
        """Yield a temporary local copy of a stored run file (or None if absent)."""
        key = self.key_for(state, run_id, filename)
        if not default_storage.exists(key):
            yield None
            return
        suffix = Path(filename).suffix or ""
        with default_storage.open(key, "rb") as source:
            with NamedTemporaryFile(suffix=suffix, delete=False) as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(chunk)
                path = Path(target.name)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)


storage = FloodAlertStorage()
