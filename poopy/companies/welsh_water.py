"""Module for Welsh Water API interaction."""

import warnings
from datetime import timedelta

import pandas as pd
import requests

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany


class WelshWater(WaterCompany):
    """
    Create an object to interact with the WelshWater EDM API.

    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services3.arcgis.com/KLNF7YxtENPLYVey/arcgis/rest/services"
    CURRENT_API_RESOURCE = (
        "/Spill_Prod/FeatureServer/0/query?where=1=1&outFields=*&f=json"
    )
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 2000  # Max num of outputs that can be requested from the API at once

    D8_FILE_URL = "https://zenodo.org/records/14238014/files/welsh_d8.nc?download=1"
    D8_FILE_HASH = "md5:8c965ad0597929df3bc54bc728ed8404"

    def __init__(self, client_id="", client_secret=""):
        """Initialise a Welsh Water object."""
        # No auth required for this API so no need to pass in client_id and client_secret
        print("\033[36m" + "Initialising Welsh Water object..." + "\033[0m")
        self._name = "WelshWater"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """Get the current status of the monitors by calling the API."""
        print(
            "\033[36m"
            + "Requesting current status data from Welsh Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.CURRENT_API_RESOURCE

        params = {
            "resultRecordCount": self.API_LIMIT,
            "resultOffset": "",  # No offset required for this API
        }
        df = self._handle_current_api_response(url=url, params=params)

        return df

    def _handle_current_api_response(self, url: str, params: str) -> pd.DataFrame:
        """
        Create and handles the response from the API.

        If the response is valid, return a dataframe of the response.
        Otherwise, raise an exception. This is a helper function for the `_fetch_current_status_df` (and `_fetch_monitor_history_df` not implemented for WW) functions.
        """
        df = pd.DataFrame()

        r = requests.get(
            url,
            params=params,
        )

        print("\033[36m" + "\tRequesting from " + r.url + "\033[0m")
        # check response status and use only valid requests
        if r.status_code == 200:
            response = r.json()
            # If no items are returned, return an empty dataframe
            if "features" not in response:
                print("\033[36m" + "\tNo records to fetch" + "\033[0m")
            else:
                data = response["features"]
                for location in data:
                    location = location["attributes"]
                    df_temp = pd.json_normalize(location)
                    df = pd.concat([df, df_temp])
        else:
            raise Exception(
                f"\tRequest failed with status code {r.status_code}, and error message: {r.json()}"
            )
        df.reset_index(drop=True, inplace=True)
        # if number of rows is exactly the API limit, there may be more records to fetch so print a warning
        if df.shape[0] == self.API_LIMIT:
            warnings.warn(
                "\033[36m"
                + f"\tNumber of records fetched is equal to the API limit of {self.API_LIMIT}. There may be missing records!"
                + "\033[0m"
            )
        return df

    def _fetch_monitor_history(self, monitor: Monitor) -> list[Event]:
        """Not available for WW API."""
        # Print a helpful message to the user that this function is not available for this API
        print(
            "\033[36m"
            + "This function is not available for the Welsh Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """Not available for WW API."""
        # Print a helpful message to the user that this function is not available for this API
        print(
            "\033[36m"
            + "This function is not available for the Welsh Water API."
            + "\033[0m"
        )
        pass
        return

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Welsh Water active API response to a Monitor object.

        See `_fetch_current_status_df`
        """
        current_time = (
            self._timestamp
        )  # Get the current time which corresponds to when the API was called (so it is same for all monitors)

        # if monitor currently discharging we set last_48h to be True.
        if row["status"] == "Overflow Operating":
            last_48h = True
        elif pd.notnull(row["stop_date_time_discharge"]):
            # if monitor has different status but has discharged in the last 48 hours we also set last_48h to be True.
            last_48h = (
                current_time - pd.to_datetime(row["stop_date_time_discharge"])
            ) <= timedelta(hours=48)
        else:
            last_48h = None
        monitor = Monitor(
            site_name=row["asset_name"],
            permit_number=row["permit_number"],
            x_coord=row["discharge_x_location"],
            y_coord=row["discharge_y_location"],
            receiving_watercourse=row["Receiving_Water"],
            water_company=self,
            discharge_in_last_48h=last_48h,
        )
        return monitor

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the Welsh Water active API response to an Event object.

        See `_fetch_current_status_df`
        """
        if row["status"] == "Overflow Operating":
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["start_date_time_discharge"]),
            )
        elif row["status"] == "Overflow Not Operating":
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(
                    row["stop_date_time_discharge"]
                ),  # Assume that the start of "not discharging" is the end of the last discharge event
            )
        elif row["status"] == "Overflow Not Operating (Has in the last 24 hours)":
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["stop_date_time_discharge"]),
            )
        elif row["status"] == "Under Maintenance":
            event = Offline(
                monitor=monitor,
                ongoing=True,
                start_time=None,  # !!! The api doesn't provide a status change date for this
            )
        else:
            raise Exception(
                "Unknown status type "
                + row["status"]
                + " for monitor "
                + row["asset_name"]
            )
        return event
