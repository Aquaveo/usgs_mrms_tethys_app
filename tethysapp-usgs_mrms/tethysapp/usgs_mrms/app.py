from tethys_sdk.app_settings import PersistentStoreDatabaseSetting
from tethys_sdk.base import TethysAppBase


class App(TethysAppBase):
    """
    Tethys app class for USGS-MRMS Flood Explorer.
    """
    name = 'USGS-MRMS Flood Explorer (8616 US basins)'
    description = ''
    package = 'usgs_mrms'  # WARNING: Do not change this value
    index = 'home'
    icon = f'{package}/images/icon.gif'
    root_url = 'usgs-mrms'
    color = '#c23616'
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def persistent_store_settings(self):
        """Database of background flood-alert job records, shared across replicas."""
        return (
            PersistentStoreDatabaseSetting(
                name='jobs_db',
                description='Background flood-alert (EWS) job records shared across portal '
                            'replicas. Assign a database service in the portal admin.',
                initializer='usgs_mrms.model.init_jobs_db',
                required=True,
            ),
        )
