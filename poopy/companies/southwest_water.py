"""Module for Southwest Water API interaction."""

from datetime import timedelta

import pandas as pd

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany
from poopy.utils import latlong_to_osgb


class SouthWestWater(WaterCompany):
    """
    Create an object to interact with the South West Water EDM API.

    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services-eu1.arcgis.com/OMdMOtfhATJPcHe3/arcgis/rest/services/"
    CURRENT_API_RESOURCE = "NEH_outlets_PROD/FeatureServer/0/query"
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 1000  # Max num of outputs that can be requested from the API at once

    D8_FILE_URL = "https://zenodo.org/records/14238014/files/southwest_d8.nc?download=1"
    D8_FILE_HASH = "md5:1df4df2f3d7afac19c1d8f9dcf794882"

    def __init__(self, client_id="", client_secret=""):
        """Initialise a South West Water object."""
        # No auth required for this API so no need to pass in client_id and client_secret
        print("\033[36m" + "Initialising South West Water object..." + "\033[0m")
        self._name = "SouthWest Water"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_monitor_history(self, monitor: Monitor) -> list[Event]:
        """Not available for South West Water API."""
        print(
            "\033[36m"
            + "This function is not available for the South West Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """Not available for South West Water API."""
        print(
            "\033[36m"
            + "This function is not available for the South West Water API."
            + "\033[0m"
        )
        pass
        return

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the South West Water active API response to a Monitor object.

        See `_fetch_current_status_df`
        """
        current_time = (
            self._timestamp
        )  # Get the current time which corresponds to when the API was called (so it is same for all monitors)

        x, y = latlong_to_osgb(row["latitude"], row["longitude"])

        # South West Water does not always provide a site name or even ID! Losers!
        if pd.isna(row["ID"]):
            name = "Unknown"
        else:
            name = row["ID"]

        # if monitor currently discharging we set last_48h to be True.
        if row["status"] == 1:
            last_48h = True

        elif pd.notnull(row["latestEventEnd"]):
            # if monitor has different status (i.e., offline or not discharging) but has discharged in the last 48 hours we also set last_48h to be True.
            last_48h = (
                current_time - pd.to_datetime(row["latestEventEnd"], unit="ms")
            ) <= timedelta(hours=48)
        else:
            # This is normally the case when the monitor has never discharged
            last_48h = None

        # Parse row["ReceivingWaterCourse"] to a string, including when it is None
        if pd.isna(row["receivingWaterCourse"]):
            receiving_watercourse = "Unknown"
        else:
            receiving_watercourse = row["receivingWaterCourse"]

        return Monitor(
            site_name=name,  # South West Water does not provide a site name so we use the ID
            permit_number="Unknown",
            x_coord=x,
            y_coord=y,
            receiving_watercourse=receiving_watercourse,
            water_company=self,
            discharge_in_last_48h=last_48h,
        )

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the South West Water active API response to an Event object.

        See `_fetch_current_status_df`
        """
        if row["status"] == 1:
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["statusStart"], unit="ms"),
            )
        elif row["status"] == 0:
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["statusStart"], unit="ms"),
            )
        elif row["status"] == -1:
            event = Offline(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["statusStart"], unit="ms"),
            )
        else:
            # Raise an exception if the status is not -1, 0 or 1 (should not happen!)
            raise Exception(
                "Unknown status type " + row["status"] + " for monitor " + row["ID"]
            )
        return event
