"""Module for Anglian Water API interaction."""

from datetime import timedelta

import pandas as pd

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany
from poopy.utils import latlong_to_osgb


class AnglianWater(WaterCompany):
    """
    Create an object to interact with the AnglianWater EDM API.

    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services3.arcgis.com/VCOY1atHWVcDlvlJ/arcgis/rest/services/"
    CURRENT_API_RESOURCE = "stream_service_outfall_locations_view/FeatureServer/0/query"
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 1000  # Max num of outputs that can be requested from the API at once
    D8_FILE_URL = "https://zenodo.org/records/14238014/files/anglian_d8.nc?download=1"
    D8_FILE_HASH = "md5:a053da23a0305b36856f38f4a5e59e10"

    def __init__(self, client_id="", client_secret=""):
        """Initialise an Anglian Water object."""
        # No auth required for this API so no need to pass in client_id and client_secret
        print("\033[36m" + "Initialising Anglian Water object..." + "\033[0m")
        self._name = "AnglianWater"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_monitor_history(self, monitor: Monitor) -> list[Event]:
        """Not available for Anglian Water API."""
        print(
            "\033[36m"
            + "This function is not available for the Anglian Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """Not available for Anglian Water API."""
        print(
            "\033[36m"
            + "This function is not available for the Anglian Water API."
            + "\033[0m"
        )
        pass
        return

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Anglian Water active API response to a Monitor object.

        See `_fetch_current_status_df`
        """
        current_time = (
            self._timestamp
        )  # Get the current time which corresponds to when the API was called (so it is same for all monitors)

        x, y = latlong_to_osgb(row["Latitude"], row["Longitude"])

        # if row["latestEventEnd"] is not nan, convert it to datetime, else set it to None
        if not pd.isna(row["LatestEventEnd"]):
            last_event_end = pd.to_datetime(row["LatestEventEnd"], unit="ms")
        else:
            last_event_end = None

        # if monitor currently discharging we set last_48h to be True.
        if row["Status"] == 1:
            last_48h = True

        # if monitor not currently discharging or if it is offline, we check the last_event_end time
        elif row["Status"] == 0 or row["Status"] == -1:
            # If last_event_end is within the last 48 hours, set last_48h to be True
            if (
                last_event_end is not None
                and last_event_end > current_time - timedelta(days=2)
            ):
                last_48h = True
            # If last_event_end is more than 48 hours ago, or undefined, set last_48h to be False
            else:
                last_48h = False
        else:
            # Raise an exception if the status is not 0 or 1
            raise Exception(
                f"Status is not 0 or 1 for monitor {row['Id']}. Status is {row['Status']}"
            )

        # Parse row["ReceivingWaterCourse"] to a string, including when it is None
        if pd.isna(row["ReceivingWaterCourse"]):
            receiving_watercourse = "Unknown"
        else:
            receiving_watercourse = row["ReceivingWaterCourse"]

        return Monitor(
            site_name=row["Id"],  # Anglian Water does not provide a site name
            permit_number="Unknown",
            x_coord=x,
            y_coord=y,
            receiving_watercourse=receiving_watercourse,
            water_company=self,
            discharge_in_last_48h=last_48h,
        )

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the Anglian Water active API response to an Event object.

        See `_fetch_current_status_df`
        """
        if row["Status"] == 1:
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["LatestEventStart"], unit="ms"),
            )
        elif row["Status"] == 0:
            # If event_end is NaT update event_end to be None. This is normally because the monitor is yet
            # to have a discharge event. So, cannot sensibly record the start time of the no discharge event.
            # if row["latestEventEnd"] is not nan, convert it to datetime, else set it to None
            if not pd.isna(row["LatestEventEnd"]):
                last_event_end = pd.to_datetime(row["LatestEventEnd"], unit="ms")
            else:
                last_event_end = None
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                # Assume the the period of no discharge is from the end of the last event to the current time
                start_time=last_event_end,
            )
        elif row["Status"] == -1:
            event = Offline(
                monitor=monitor,
                ongoing=True,
                start_time=None,  # Offline events may not have a reliable start time in the Anglian Water API
            )
        else:
            # Raise an exception if the status is not 0 or 1 (should not happen!)
            raise Exception(
                "Unknown status type " + row["Status"] + " for monitor " + row["Id"]
            )
        return event
