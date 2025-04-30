"""Module for Severnt Trent Water API interaction."""

from datetime import timedelta

import pandas as pd

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany
from poopy.utils import latlong_to_osgb


class SevernTrentWater(WaterCompany):
    """
    Create an object to interact with the SevernTrent Water EDM API.

    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services1.arcgis.com/NO7lTIlnxRMMG9Gw/arcgis/rest/services/"
    CURRENT_API_RESOURCE = (
        "Severn_Trent_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    )
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 2000  # Max num of outputs that can be requested from the API at once

    D8_FILE_URL = (
        "https://zenodo.org/records/14238014/files/severntrent_d8.nc?download=1"
    )
    D8_FILE_HASH = "md5:6259a6b1b411a972b68067c1092bd0bb"

    def __init__(self, client_id="", client_secret=""):
        """Initialise a Severn Trent Water object."""
        # No auth required for this API so no need to pass in client_id and client_secret
        print("\033[36m" + "Initialising SevernTrent Water object..." + "\033[0m")
        self._name = "SevernTrent Water"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_monitor_history(self, monitor: Monitor) -> list[Event]:
        """Not available for SevernTrent Water API."""
        print(
            "\033[36m"
            + "This function is not available for the SevernTrent Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """Not available for SevernTrent Water API."""
        print(
            "\033[36m"
            + "This function is not available for the SevernTrent Water API."
            + "\033[0m"
        )
        pass
        return

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the SevernTrent Water active API response to a Monitor object.

        See `_fetch_current_status_df`
        """
        current_time = (
            self._timestamp
        )  # Get the current time which corresponds to when the API was called (so it is same for all monitors)

        x, y = latlong_to_osgb(row["Latitude"], row["Longitude"])

        # if monitor currently discharging we set last_48h to be True.
        if row["Status"] == 1:
            last_48h = True

        elif pd.notnull(row["LatestEventEnd"]):
            # if monitor has different status (i.e., offline or not discharging) but has discharged in the last 48 hours we also set last_48h to be True.
            last_48h = (
                current_time - pd.to_datetime(row["LatestEventEnd"], unit="ms")
            ) <= timedelta(hours=48)
        else:
            # This is normally the case when the monitor has never discharged. But for UU the information in the data-stream is not clear,
            # specifically the "LatestEventEnd" field is not always populated sensibly (nor is the "StatusStart" field). Shrug!
            last_48h = None

        # Parse row["ReceivingWaterCourse"] to a string, including when it is None
        if pd.isna(row["ReceivingWaterCourse"]):
            receiving_watercourse = "Unknown"
        else:
            receiving_watercourse = row["ReceivingWaterCourse"]

        return Monitor(
            site_name=row["Id"],  # SevernTrent Water does not provide a site name
            permit_number="Unknown",
            x_coord=x,
            y_coord=y,
            receiving_watercourse=receiving_watercourse,
            water_company=self,
            discharge_in_last_48h=last_48h,
        )

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the SevernTrent Water active API response to an Event object.

        See `_fetch_current_status_df`
        """
        # SevernTrent provide a good use of the "StatusStart" field to determine the start time of the event. Hooray!
        # This makes our life easier (but does mean that we should check that it matches up with LatestEventEnd and LatestEventStart!_
        if row["Status"] == 1:
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusStart"], unit="ms"),
            )
        elif row["Status"] == 0:
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusStart"], unit="ms"),
            )
        elif row["Status"] == -1:
            event = Offline(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusStart"], unit="ms"),
            )
        else:
            # Raise an exception if the status is not -1, 0 or 1 (should not happen!)
            raise Exception(
                "Unknown status type " + row["Status"] + " for monitor " + row["Id"]
            )
        return event
