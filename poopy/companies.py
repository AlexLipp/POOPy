import datetime
import requests
import warnings
from typing import Dict, List

import pandas as pd

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany


class ThamesWater(WaterCompany):
    """
    A subclass of `WaterCompany` that represents the EDM monitoring network for Thames Water.
    """

    API_ROOT = "https://prod-tw-opendata-app.uk-e1.cloudhub.io"
    CURRENT_API_RESOURCE = "/data/STE/v1/DischargeCurrentStatus"
    HISTORICAL_API_RESOURCE = "/data/STE/v1/DischargeAlerts"
    API_LIMIT = 1000  # Max num of outputs that can be requested from the API at once

    # The URL and hash of the D8 raster file on the server
    D8_FILE_URL = "PLACEHOLDER"
    D8_FILE_HASH = "PLACEHOLDER"

    def __init__(self, clientID: str, clientSecret: str):
        print("\033[36m" + "Initialising Thames Water object..." + "\033[0m")
        print("\033[36m" + "\tFetching D8 raster..." + "\033[0m")
        print(
            "\033[36m"
            + "\tThis download can be slow if it is the first time you have generated a ThamesWater object." 
            + "\n\tThe file will be cached so will be faster on subsequent uses..."
            + "\033[0m"
        )
        super().__init__(clientID, clientSecret)
        self._name = "ThamesWater"
        # self._d8_file_path = self._fetch_d8_file(
        #     url=self.D8_FILE_URL,
        #     known_hash=self.D8_FILE_HASH,
        # )
        self._d8_file_path = "thames_d8.nc"

    def set_all_histories(self) -> None:
        """
        Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        """
        self._history_timestamp = datetime.datetime.now()
        df = self._get_all_monitors_history_df()
        historical_names = df["LocationName"].unique().tolist()
        # Find which monitors present in historical_names are not in active_names
        active_names = self.active_monitor_names
        inactive_names = [x for x in historical_names if x not in active_names]
        # If inactive is not empty raise a warning using the warnings module in red using ANSI escape codes
        if inactive_names:
            warnings.warn(
                f"\033[31m\n! WARNING ! The following historical monitors are no longer active: {inactive_names}\nStoring historical data for inactive monitors is not currently supported!\nIf this message has appeared it should be implemented...\033[0m "
            )
        print("\033[36m" + f"Building history for monitors..." + "\033[0m")
        for name in active_names:
            subset = df[df["LocationName"] == name]
            monitor = self.active_monitors[name]
            monitor._history = self._events_df_to_events_list(subset, monitor)

    def _get_current_status_df(self) -> pd.DataFrame:
        """
        Get the current status of the monitors by calling the API.
        """
        print(
            "\033[36m"
            + "Requesting current status data from Thames Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.CURRENT_API_RESOURCE
        params = {
            "limit": self.API_LIMIT,
            "offset": 0,
        }
        df = self._handle_api_response(url=url, params=params)

        return df

    def _get_all_monitors_history_df(self) -> pd.DataFrame:
        """
        Get the historical status of all monitors by calling the API.
        """
        print(
            "\033[36m"
            + f"Requesting historical data for all monitors from Thames Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.HISTORICAL_API_RESOURCE
        params = {
            "limit": self.API_LIMIT,
            "offset": 0,
        }
        df = self._handle_api_response(url=url, params=params)
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_monitor_events_df(self, monitor: Monitor) -> pd.DataFrame:
        """
        Get the historical status of a particular monitor by calling the API.
        """
        print(
            "\033[36m"
            + f"Requesting historical data for {monitor.site_name} from Thames Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.HISTORICAL_API_RESOURCE
        params = {
            "limit": self.API_LIMIT,
            "offset": 0,
            "col_1": "LocationName",
            "operand_1": "eq",
            "value_1": monitor.site_name,
        }
        df = self._handle_api_response(url=url, params=params)
        return df

    def _handle_api_response(self, url: str, params: str) -> pd.DataFrame:
        """
        Creates and handles the response from the API. If the response is valid, return a dataframe of the response.
        Otherwise, raise an exception. This is a helper function for the `_get_current_status_df` and `_get_monitor_history_df` functions.
        Loops through the API calls until all the records are fetched.
        """
        df = pd.DataFrame()
        while True:
            r = requests.get(
                url,
                headers={
                    "client_id": self.clientID,
                    "client_secret": self.clientSecret,
                },
                params=params,
            )

            print("\033[36m" + "\tRequesting from " + r.url + "\033[0m")
            # check response status and use only valid requests
            if r.status_code == 200:
                response = r.json()
                # If no items are returned, return an empty dataframe
                if "items" not in response:
                    print("\033[36m" + "\tNo more records to fetch" + "\033[0m")
                    break
                else:
                    df_temp = pd.json_normalize(response["items"])
            else:
                raise Exception(
                    "\tRequest failed with status code {0}, and error message: {1}".format(
                        r.status_code, r.json()
                    )
                )
            df = pd.concat([df, df_temp])
            params["offset"] += params["limit"]  # Increment offset for the next request
        df.reset_index(drop=True, inplace=True)
        return df

    def _fetch_active_monitors(self) -> Dict[str, Monitor]:
        """
        Returns a dictionary of Monitor objects representing the active monitors.
        """
        df = self._get_current_status_df()
        monitors = {}
        for _, row in df.iterrows():
            monitor = self._row_to_monitor(row=row)
            event = self._row_to_event(row=row, monitor=monitor)
            monitor.current_event = event
            monitors[monitor.site_name] = monitor
        return monitors

    def _events_df_to_events_list(
        self, df: pd.DataFrame, monitor: Monitor
    ) -> List[Event]:
        def _warn(reason: str) -> None:
            """Automatically raises a warning with the correct message"""
            warnings.warn(
                f"\033[91m! WARNING ! Alert stream for monitor {monitor.site_name} contains an invalid entry! \nReason: {reason}. Skipping that entry...\033[0m"
            )

        print("\033[36m" + f"\tBuilding history for {monitor.site_name}..." + "\033[0m")
        history = []
        history.append(monitor.current_event)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            # If the dataframe is empty, there are no events to create
            return []

        if df["LocationName"].unique().size > 1:
            raise Exception(
                "The dataframe contains events for multiple monitors, beyond the one specified!"
            )
        if df["LocationName"].unique()[0] != monitor.site_name:
            raise Exception(
                "The dataframe contains events for a different monitor than the one specified!"
            )

        for index, row in df.iterrows():
            n_rows = len(df)
            next_index = index + 1

            if index == n_rows - 1:
                # At the last entry in the df...
                if not (
                    (row["AlertType"] == "Start")
                    or (row["AlertType"] == "Offline start")
                ):
                    # ... and it's not a start event!
                    reason = "the last recorded event is not a Start event!"
                    _warn(reason)
                    continue
                else:
                    break

            if row["AlertType"] == "Stop":
                # Found the end of an event...
                if df.iloc[next_index]["AlertType"] != "Start":
                    # ... but it's not preceded by a start event!
                    reason = f"a stop event was not preceded by Start event at {df.iloc[index]['DateTime']}"
                    _warn(reason)
                    continue
                else:
                    # ... its preceded by a start event, so we create a Discharge event!
                    stop = pd.to_datetime(row["DateTime"])
                    start = pd.to_datetime(df.iloc[next_index]["DateTime"])
                    event = Discharge(
                        monitor=monitor, ongoing=False, start_time=start, end_time=stop
                    )
                    history.append(event)

            if row["AlertType"] == "Offline stop":
                # Found the end of an offline event...
                if df.iloc[next_index]["AlertType"] != "Offline start":
                    # ... but it's not preceded by an offline start event!
                    reason = f"an offline Stop event was not preceded by Offline Start event at {df.iloc[index]['DateTime']}"
                    _warn(reason)
                    continue
                else:
                    # ... its preceded by an offline start event, so we create an Offline event!
                    stop = pd.to_datetime(row["DateTime"])
                    start = pd.to_datetime(df.iloc[index + 1]["DateTime"])
                    event = Offline(
                        monitor=monitor, ongoing=False, start_time=start, end_time=stop
                    )
                    history.append(event)

            if row["AlertType"] == "Start" or row["AlertType"] == "Offline start":
                # Found the start of an event...
                if index == n_rows - 1:
                    # ... but it's the last entry in the df, so we can't create an event! Quit the loop.
                    break
                else:
                    if (
                        df.iloc[next_index]["AlertType"] == "Start"
                        or df.iloc[next_index]["AlertType"] == "Offline start"
                    ):
                        # ... and it's followed by another start event!
                        reason = f"a Start or Offline Start event was preceded by a Start or Offline Start event at {df.iloc[index]['DateTime']}"
                        _warn(reason)
                        continue
                    else:
                        # ... and it's not followed by another start event, so we create a NoDischarge event
                        # to represent the period between the start of this event and the end of the previous event.
                        stop = pd.to_datetime(row["DateTime"])
                        start = pd.to_datetime(df.iloc[next_index]["DateTime"])
                        event = NoDischarge(
                            monitor=monitor,
                            ongoing=False,
                            start_time=start,
                            end_time=stop,
                        )
                        history.append(event)
        return history

    def _get_monitor_history(self, monitor: Monitor) -> List[Event]:
        """
        Creates a list of historical Event objects from the alert stream for a given monitor.
        This is done by iterating through the alert stream and creating an Event object for each
        start/stop event pair. If the alert stream is invalid, a warning is raised and the entry is skipped.
        If the alert stream is empty, an empty list is returned.

        Args:
            monitor (Monitor): The monitor for which to create the history

        Returns:
            List[Event]: A list of Event objects representing the historical events for the monitor
        """
        # Get the historical data for the monitor from the API
        events_df = self._get_monitor_events_df(monitor)
        return self._events_df_to_events_list(df=events_df, monitor=monitor)

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Thames Water active API response to a Monitor object. See `_get_current_status_df`
        """
        monitor = Monitor(
            site_name=row["LocationName"],
            permit_number=row["PermitNumber"],
            x_coord=row["X"],
            y_coord=row["Y"],
            receiving_watercourse=row["ReceivingWaterCourse"],
            water_company=self,
            discharge_in_last_48h=row["AlertPast48Hours"],
        )
        return monitor

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the Thames Water active API response to an Event object. See `_get_current_status_df`
        """
        if row["AlertStatus"] == "Discharging":
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusChange"]),
            )
        elif row["AlertStatus"] == "Not discharging":
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusChange"]),
            )
        elif row["AlertStatus"] == "Offline":
            event = Offline(
                monitor=monitor,
                ongoing=True,
                start_time=pd.to_datetime(row["StatusChange"]),
            )
        else:
            raise Exception(
                "Unknown status type "
                + row["AlertStatus"]
                + " for monitor"
                + row["LocationName"]
            )
        return event
