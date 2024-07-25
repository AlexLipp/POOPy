import datetime
import warnings
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Tuple, Any, Callable

import requests
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

    # Set history valid until to be half past midnight on the 1st April 2022
    HISTORY_VALID_UNTIL = datetime.datetime(2022, 4, 1, 0, 30, 0)
    # This is the date until by which the EDM monitors had been attached to the API and so the
    # point at which the record becomes valid. Note however that most records we actually
    # not attached until 1/1/2023, so its not sensible to compare records before this date.

    # A large number of monitors have exactly 1st April 2022 as their first record,
    # and a long period of offline that follows.

    # The URL and hash of the D8 raster file on the server
    D8_FILE_URL = "https://zenodo.org/records/10426423/files/thames_d8.nc?download=1"
    D8_FILE_HASH = "md5:1047a14906237cd436fd483e87c1647d"

    def __init__(self, clientID: str, clientSecret: str):
        print("\033[36m" + "Initialising Thames Water object..." + "\033[0m")
        super().__init__(clientID, clientSecret)
        self._name = "ThamesWater"
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )

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
        start = datetime.datetime.now()
        for name in active_names:
            subset = df[df["LocationName"] == name]
            monitor = self.active_monitors[name]
            monitor._history = self._events_df_to_events_list(subset, monitor)
        end = datetime.datetime.now()
        # Print how long it took without parallel, rounded to 2 decimal places:
        print(
            f"\033[36m"
            + f"Processing took {round((end - start).total_seconds(), 2)} seconds."
            + "\033[0m"
        )

    def set_all_histories_parallel(self) -> None:
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

        # Prepare arguments for parallel processing
        args_list = [
            (name, df, self.active_monitors, self._events_df_to_events_list)
            for name in active_names
        ]
        
        start = datetime.datetime.now()

        # Use a Pool to parallelize the loop
        with Pool() as pool:
            results = pool.map(process_monitor, args_list)

        # Update the monitor objects with the results in serial
        for name, history in results:
            self.active_monitors[name]._history = history

        end = datetime.datetime.now()

        # Print how long it took with parallel, rounded to 2 decimal places:
        print(
            f"\033[36m"
            + f"Parallel Processing took {round((end - start).total_seconds(), 2)} seconds."
            + "\033[0m"
        )

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
        df = self._handle_current_api_response(url=url, params=params)

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
        df = self._handle_history_api_response(url=url, params=params)
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_monitor_events_df(
        self, monitor: Monitor, verbose: bool = False
    ) -> pd.DataFrame:
        """
        Get the historical status of a particular monitor by calling the API.
        If verbose is set to True, the function will print the dataframe of the full API response to the console.
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
        df = self._handle_current_api_response(url=url, params=params, verbose=verbose)
        # Note, we use handle_current_api_response here because we want to try and fetch all records not just those up to a certain date. This
        # is because individual monitors don't have the same "start" date and so the historical fetching criterion varies. However, this is
        # not ideal because it means that if the API erroneously returns an empty dataframe in place of an error message, then the function will
        # return an empty dataframe. This is the fault of the API, not this code but it is something to be aware of, and needs to be fixed.
        return df

    def _handle_current_api_response(
        self, url: str, params: str, verbose: bool = False
    ) -> pd.DataFrame:
        """
        Creates and handles the response from the API. If the response is valid, return a dataframe of the response.
        Otherwise, raise an exception. This is a helper function for the `_get_current_status_df` and `_get_monitor_history_df` functions.
        Loops through the API calls until all the records are fetched. If verbose is set to True, the function will print the full dataframe
        to the console.
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

        # Print the full dataframe to the console if verbose is set to True
        if verbose:
            print("\033[36m" + "\tPrinting full API response..." + "\033[0m")
            with pd.option_context(
                "display.max_rows", None, "display.max_columns", None
            ):  # more options can be specified also
                print(df)

        return df

    def _handle_history_api_response(self, url: str, params: str) -> pd.DataFrame:
        """
        Creates and handles the response from the API. If the response is valid, it returns a dataframe of the response.
        Otherwise, it raises an exception. The function loops through the API calls until a record is returned that has a datetime
        that exceeds the `HISTORY_VALID_UNTIL` date. This differs from the `handle_api_response` function in that it does not try
        to fetch all records, but only those until a certain date. This allows for more elegant handling of the error whereby the
        API erroneously returns an empty dataframe in place of an error message. Note that there is one case where this function
        will behave unexpectedly: if the number of records returned is exactly an integer multiple of the API limit number of events
        (e.g., 0, 1000, 2000 etc.), then the function will return an empty dataframe. This is because the function cannot distinguish
        between this case and the case where the API genuinely returns no records. This is the fault of the API, not this code but
        it is something to be aware of, and needs to be fixed.

        See also the `handle_current_api_response` function.
        """
        print(
            "\033[36m"
            + "\tRequesting historical events since {0}...".format(
                self.HISTORY_VALID_UNTIL
            )
            + "\033[0m"
        )
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
                # If no items are returned, handle it here. Think hard on how to handle this.
                if "items" not in response:
                    # Raise an exception if the response is empty.
                    nrecords = df.shape[0]

                    # TODO: handle this exception more elegantly...
                    # ...Maybe if last record returned is really close to HISTORY_VALID_UNTIL then don't raise an exception.
                    # Cannot just return records because it gives false impression that all records have been fetched.
                    raise Exception(
                        "\n\t!ERROR! \n\tAPI returned no items for request: {0} \n\t! ABORTING !".format(
                            r.url
                        )
                        + "\n\t"
                        + "-" * 80
                        + "\n\tThis error is *probably* caused by the API erroneously returning an empty response in place of an error..."
                        + "\n\t...but it could also be caused by the API genuinely returning no records."
                        + "\n\tThis might occur if there have been *exactly* an integer multiple of the API limit number of events (e.g., 0, 1000, 2000 etc.)."
                        + "\n\tAt present there is no way to distinguish between these two cases (which is the fault of the API, not this code)."
                        + "\n\tIf you think this is the case, try using the _handle_current_api_response function instead or modifying HISTORY_VALID_UNTIL."
                        + "\n\t"
                        + "-" * 80
                        + "\n\tNumber of records fetched before error: {0}".format(
                            nrecords
                        )
                    )
                else:
                    df_temp = pd.json_normalize(response["items"])
                    # Extract the datetime of the last record fetched and cast it to a datetime object
                    last_record_datetime = pd.to_datetime(df_temp["DateTime"].iloc[-1])
                    if last_record_datetime < self.HISTORY_VALID_UNTIL:
                        print(
                            "\033[36m"
                            + "\tFound a record with datetime {0} before `valid until' date {1}.".format(
                                last_record_datetime, self.HISTORY_VALID_UNTIL
                            )
                            + "\033[0m"
                        )

                        # Check the number of rows and compare to the API limit
                        if df_temp.shape[0] < self.API_LIMIT:
                            # If the number of records is less than the API limit, then we have fetched all records
                            print(
                                "\033[36m"
                                + "\tLast request contained {0} many records, fewer than the API limit of {1}.".format(
                                    df_temp.shape[0], self.API_LIMIT
                                )
                                + "\033[0m"
                            )
                            print(
                                "\033[36m" + "\tNo more records to fetch!" + "\033[0m"
                            )
                            df = pd.concat([df, df_temp])
                            break
                        else:
                            # If the number of records is equal to the API limit, possibly more records to fetch so we continue.
                            print(
                                "\033[36m"
                                + "\tLast request contained {0} many records, equal to the API limit of {1}.".format(
                                    df_temp.shape[0], self.API_LIMIT
                                )
                                + "\033[0m"
                            )
                            print(
                                "\033[36m"
                                + "\tChecking if there are more records to fetch..."
                                + "\033[0m"
                            )
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

    def _get_monitor_history(
        self, monitor: Monitor, verbose: bool = False
    ) -> List[Event]:
        """
        Creates a list of historical Event objects from the alert stream for a given monitor.
        This is done by iterating through the alert stream and creating an Event object for each
        start/stop event pair. If the alert stream is invalid, a warning is raised and the entry is skipped.
        If the alert stream is empty, an empty list is returned. Optionally prints the full dataframe
        of the API response to the console if verbose is set to True.

        Args:
            monitor (Monitor): The monitor for which to create the history
            verbose (bool): If True, the function will print the full dataframe of the API response to the console

        Returns:
            List[Event]: A list of Event objects representing the historical events for the monitor
        """
        # Get the historical data for the monitor from the API
        events_df = self._get_monitor_events_df(monitor, verbose=verbose)
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


def process_monitor(
    args: Tuple[
        str,
        pd.DataFrame,
        Dict[str, Monitor],
        Callable[[pd.DataFrame, Monitor], List[Event]],
    ]
) -> None:
    """
    Process a single monitor in parallel. This function is used in the `set_all_histories_parallel` method of the `ThamesWater` class.

    Args:
        args: A tuple containing:
            - name: The name of the monitor.
            - df: The DataFrame containing historical data.
            - active_monitors: A dictionary of active monitors.
            - events_df_to_events_list: A function to convert DataFrame to events list.
    """
    name, df, active_monitors, events_df_to_events_list = args
    subset = df[df["LocationName"] == name]
    monitor = active_monitors[name]
    history = events_df_to_events_list(subset, monitor)
    return name, history
