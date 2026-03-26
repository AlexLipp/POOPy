"""Module for Scottish Water API interaction."""

from datetime import datetime, timedelta

import pandas as pd
import requests

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany


class ScottishWater(WaterCompany):
    """
    Create an object to interact with the Scottish Water EDM API.

    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.

    Status codes from the API:
        13 - Overflowing
        14 - Recent Overflow
        15 - No Overflows
        16 - No Data Available
    """

    API_ROOT = "https://api.scottishwater.co.uk/overflow-event-monitoring/v1"
    CURRENT_API_RESOURCE = "/near-real-time"
    HISTORICAL_API_RESOURCE = ""
    D8_FILE_URL = "PLACEHOLDER"  # TODO: Update with Zenodo URL once uploaded
    D8_FILE_HASH = "PLACEHOLDER"  # TODO: Update with MD5 hash once uploaded

    STATUS_OVERFLOWING = 13
    STATUS_RECENT_OVERFLOW = 14
    STATUS_NO_OVERFLOW = 15
    STATUS_NO_DATA = 16

    def __init__(self, client_id="", client_secret=""):
        """Initialise a Scottish Water object."""
        print("\033[36m" + "Initialising Scottish Water object..." + "\033[0m")
        self._name = "Scottish Water"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_monitor_history(self, monitor: Monitor) -> list[Event]:
        """Not available for Scottish Water API."""
        print(
            "\033[36m"
            + "This function is not available for the Scottish Water API."
            + "\033[0m"
        )
        return

    def set_all_histories(self) -> None:
        """Not available for Scottish Water API."""
        print(
            "\033[36m"
            + "This function is not available for the Scottish Water API."
            + "\033[0m"
        )
        return

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """Get the current status of the monitors by calling the Scottish Water API."""
        print(
            "\033[36m"
            + f"Requesting current status data from {self.name} API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.CURRENT_API_RESOURCE
        print("\033[36m" + "\tRequesting from " + url + "\033[0m")
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(
                f"\tRequest failed with status code {response.status_code}, "
                f"and error message: {response.json()}"
            )

        data = response.json()
        results = data.get("results", [])

        if not results:
            return pd.DataFrame()

        # Handle both array and single-object responses
        if isinstance(results, dict):
            results = [results]

        return pd.DataFrame(results)

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime | None:
        """
        Parse an ISO 8601 UTC datetime string to a naive datetime.

        Returns None if the string is empty or null.
        """
        if not dt_str or pd.isna(dt_str):
            return None
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(
            tzinfo=None
        )

    def _row_to_monitor(self, row: pd.Series) -> Monitor:
        """
        Convert a row of the Scottish Water active API response to a Monitor object.

        See `_fetch_current_status_df`.
        """
        current_time = self._timestamp
        x = row["DISCHARGE_OVERFLOW_LOCATION_X"]
        y = row["DISCHARGE_OVERFLOW_LOCATION_Y"]
        status_id = row["OVERFLOW_STATUS_ID"]

        if status_id == self.STATUS_OVERFLOWING:
            last_48h = True
        elif status_id == self.STATUS_RECENT_OVERFLOW:
            last_48h = True
        elif status_id == self.STATUS_NO_OVERFLOW:
            end_time = self._parse_datetime(row.get("OVERFLOW_END_DATETIME", ""))
            if end_time is not None:
                last_48h = (current_time - end_time) <= timedelta(hours=48)
            else:
                last_48h = False
        else:
            # STATUS_NO_DATA (16) - no reliable discharge information
            last_48h = None

        receiving_watercourse = row.get("RECEIVING_WATER", "Unknown")
        if pd.isna(receiving_watercourse) or not receiving_watercourse:
            receiving_watercourse = "Unknown"

        permit_number = row.get("LICENCE_NUMBER", "Unknown")
        if pd.isna(permit_number) or not permit_number:
            permit_number = "Unknown"

        return Monitor(
            site_name=row["ASSET_NAME"],
            permit_number=permit_number,
            x_coord=x,
            y_coord=y,
            receiving_watercourse=receiving_watercourse,
            water_company=self,
            discharge_in_last_48h=last_48h,
        )

    def _row_to_event(self, row: pd.Series, monitor: Monitor) -> Event:
        """
        Convert a row of the Scottish Water active API response to an Event object.

        See `_fetch_current_status_df`.
        """
        status_id = row["OVERFLOW_STATUS_ID"]

        if status_id == self.STATUS_OVERFLOWING:
            start_time = self._parse_datetime(row.get("OVERFLOW_START_DATETIME", ""))
            return Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=start_time,
            )
        elif status_id == self.STATUS_RECENT_OVERFLOW:
            # The overflow ended recently; the NoDischarge event started at the overflow end
            start_time = self._parse_datetime(row.get("OVERFLOW_END_DATETIME", ""))
            return NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=start_time,
            )
        elif status_id == self.STATUS_NO_OVERFLOW:
            # No overflow; the NoDischarge event started when the last overflow ended
            start_time = self._parse_datetime(row.get("OVERFLOW_END_DATETIME", ""))
            return NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=start_time,
            )
        else:
            # STATUS_NO_DATA (16) - treat as offline
            return Offline(
                monitor=monitor,
                ongoing=True,
                start_time=None,
            )
