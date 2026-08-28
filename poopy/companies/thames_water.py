"""Module for Thames Water API interaction."""

import random
import time
import warnings
from collections.abc import Callable
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
import requests

from poopy.poopy import Discharge, Event, Monitor, NoDischarge, Offline, WaterCompany


def _response_error_text(response: requests.Response) -> str:
    """
    Describe a failed response without itself raising.

    The API does not always return JSON on an error, so calling `.json()` directly
    in an error path can raise a decode error that masks the original failure.
    """
    try:
        return str(response.json())
    except ValueError:
        return response.text[:200] if response.text else "<empty response body>"


class ThamesWater(WaterCompany):
    """A subclass of `WaterCompany` that represents the EDM monitoring network for Thames Water."""

    API_ROOT = "https://api.thameswater.co.uk"
    CURRENT_API_RESOURCE = "/opendata/v2/discharge/status"
    HISTORICAL_API_RESOURCE = "/opendata/v2/discharge/alerts"
    API_LIMIT = 1000  # Max num of outputs that can be requested from the API at once

    # (connect, read) timeout in seconds for every API request. Without this a hung
    # connection blocks a run indefinitely, which is a common cause of stalled cron jobs.
    REQUEST_TIMEOUT = (10, 60)

    # Set history valid until to be half past midnight on the 1st April 2022
    HISTORY_VALID_UNTIL = datetime(2022, 4, 1, 0, 30, 0)
    # This is the date until by which the EDM monitors had been attached to the API and so the
    # point at which the record becomes valid. Note however that most records we actually
    # not attached until 1/1/2023, so its not sensible to compare records before this date.

    # A large number of monitors have exactly 1st April 2022 as their first record,
    # and a long period of offline that follows.

    # The URL and hash of the D8 raster file on the server
    D8_FILE_URL = "https://zenodo.org/records/19709169/files/thames_d8.tif?download=1"
    D8_FILE_HASH = "md5:c9ec32f43a0b77a8f616dd24a19828df"

    def __init__(self, client_id="", client_secret=""):
        """Initialise a Thames Water object."""
        print("\033[36m" + "Initialising Thames Water object..." + "\033[0m")
        self._name = "ThamesWater"
        super().__init__(client_id, client_secret)
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def set_all_histories(self, since: datetime | None = None) -> None:
        """
        Set the historical data for all active monitors and store it in the history attribute of each monitor.

        Args:
            since: Only fetch events back to this datetime. Defaults to
                `HISTORY_VALID_UNTIL` (i.e. the whole record). Passing a recent
                datetime dramatically reduces the number of paginated API calls,
                because the API returns records newest-first and pagination stops
                as soon as a record older than `since` is seen.

                Note that events which *straddle* `since` cannot be reconstructed:
                the alert stream pairs each stop alert with the preceding start
                alert, so a stop whose start falls outside the fetched window is
                skipped with a warning. Callers doing incremental updates should
                therefore fetch further back than the window they intend to trust.

        """
        self._history_timestamp = datetime.now()
        df = self._fetch_all_monitors_history_df(since=since)
        historical_names = df["LocationName"].unique().tolist()
        # Find which monitors present in historical_names are not in active_names
        active_names = self.active_monitor_names
        inactive_names = [x for x in historical_names if x not in active_names]
        # If inactive is not empty raise a warning using the warnings module in red using ANSI escape codes
        if inactive_names:
            warnings.warn(
                f"\033[31m\n! WARNING ! The following historical monitors are no longer active: {inactive_names}\nStoring historical data for inactive monitors is not currently supported!\nIf this message has appeared it should be implemented...\033[0m "
            )
        print("\033[36m" + "Building history for monitors..." + "\033[0m")
        for name in active_names:
            subset = df[df["LocationName"] == name]
            monitor = self.active_monitors[name]
            monitor._history = self._alerts_df_to_events_list(subset, monitor)

    def set_all_histories_parallel(self) -> None:
        """Set the historical data for all active monitors and store it in the history attribute of each monitor in parallel."""
        raise warnings.warn(
            "\033[31m\n! WARNING ! The parallel version of this function is experimental and not recommended...\033[0m "
        )
        self._history_timestamp = datetime.datetime.now()
        df = self._fetch_all_monitors_history_df()
        historical_names = df["LocationName"].unique().tolist()
        # Find which monitors present in historical_names are not in active_names
        active_names = self.active_monitor_names
        inactive_names = [x for x in historical_names if x not in active_names]
        # If inactive is not empty raise a warning using the warnings module in red using ANSI escape codes
        if inactive_names:
            warnings.warn(
                f"\033[31m\n! WARNING ! The following historical monitors are no longer active: {inactive_names}\nStoring historical data for inactive monitors is not currently supported!\nIf this message has appeared it should be implemented...\033[0m "
            )
        print("\033[36m" + "Building history for monitors..." + "\033[0m")

        # Prepare arguments for parallel processing
        args_list = [
            (name, df, self.active_monitors, self._alerts_df_to_events_list)
            for name in active_names
        ]

        # Use a Pool to parallelize the loop (this is faster)
        with Pool() as pool:
            results = pool.map(_process_monitor_history_pl, args_list)

        # Update the monitor objects with the results in serial
        for name, history in results:
            self.active_monitors[name]._history = history

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """Get the current status of the monitors by calling the API."""
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

    def _fetch_all_monitors_history_df(
        self, since: datetime | None = None
    ) -> pd.DataFrame:
        """
        Get the historical status of all monitors by calling the API.

        Args:
            since: Only fetch events back to this datetime. Defaults to
                `HISTORY_VALID_UNTIL`. See `set_all_histories`.

        """
        print(
            "\033[36m"
            + "Requesting historical data for all monitors from Thames Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.HISTORICAL_API_RESOURCE
        params = {
            "limit": self.API_LIMIT,
            "offset": 0,
        }
        df = self._handle_history_api_response(url=url, params=params, since=since)
        df.reset_index(drop=True, inplace=True)
        return df

    def _fetch_monitor_events_df(
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
            "locationName": monitor.site_name,
        }
        df = self._handle_current_api_response(url=url, params=params, verbose=verbose)
        # Note, we use handle_current_api_response here because we want to try and fetch all records not just those up to a certain date. This
        # is because individual monitors don't have the same "start" date and so the historical fetching criterion varies. However, this is
        # not ideal because it means that if the API erroneously returns an empty dataframe in place of an error message, then the function will
        # return an empty dataframe. This is the fault of the API, not this code but it is something to be aware of, and needs to be fixed.
        return df

    def _transform_api_response(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform API response so Thames v2 data remains compatible with WaterCompany class."""
        if df.empty:
            return df

        # Map of camelCase field names to PascalCase field names
        field_mapping = {
            "locationName": "LocationName",
            "permitNumber": "PermitNumber",
            "locationGridRef": "LocationGridRef",
            "x": "X",
            "y": "Y",
            "receivingWaterCourse": "ReceivingWaterCourse",
            "alertStatus": "AlertStatus",
            "statusChanged": "StatusChange",
            "alertPast48Hours": "AlertPast48Hours",
            "datetime": "DateTime",
            "alertType": "AlertType",
        }

        # Rename columns that exist in the DataFrame
        existing_columns = set(df.columns) & set(field_mapping.keys())
        rename_dict = {col: field_mapping[col] for col in existing_columns}
        return df.rename(columns=rename_dict)

    def _request_with_retries(
        self, url: str, params: dict, max_retries: int = 5, base_delay: int = 5
    ) -> requests.Response:
        """
        GET from the API, retrying rate limits and transient server errors.

        Thames routinely returns 429 "Quota has been exceeded", and a run that
        makes many sequential requests will meet one sooner or later. Retrying
        with exponential backoff and jitter turns that from a failed run into a
        slow one.

        Args:
            url: The endpoint to request.
            params: Query parameters.
            max_retries: How many attempts before giving up.
            base_delay: Seconds for the first backoff, doubling each attempt.

        Returns:
            The successful response.

        Raises:
            Exception: If the request still fails after `max_retries` attempts,
                or fails with a status code that is not worth retrying.

        """
        for attempt in range(max_retries):
            try:
                r = requests.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
                print("\033[36m" + "\tRequesting from " + r.url + "\033[0m")

                # Retry on rate-limiting (429) and on transient server errors (5xx).
                # Both are routinely recoverable, and a single unretried failure
                # part-way through pagination otherwise aborts the entire run.
                if r.status_code == 429 or r.status_code >= 500:
                    if attempt < max_retries - 1:
                        # Calculate delay with exponential backoff and jitter
                        delay = base_delay * (2**attempt) + random.uniform(0, 30)
                        print(
                            f"\033[93m\tAPI returned {r.status_code} ({_response_error_text(r)}). "
                            f"Attempt {attempt + 1}/{max_retries}. "
                            f"Waiting {delay:.1f} seconds before retrying...\033[0m"
                        )
                        time.sleep(delay)
                        continue
                    print(
                        f"\033[91m\tAPI still returning {r.status_code} after {max_retries} attempts. "
                        f"Please try again later.\033[0m"
                    )
                    raise Exception(
                        f"Request failed with status code {r.status_code} after "
                        f"{max_retries} retry attempts. Error: {_response_error_text(r)}"
                    )

                if r.status_code != 200:
                    raise Exception(
                        f"Request failed with status code {r.status_code}, and error message: {_response_error_text(r)}"
                    )

                return r

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(0, 30)
                    print(
                        f"\033[93m\tNetwork error occurred. Attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {delay:.1f} seconds before retrying...\033[0m"
                    )
                    time.sleep(delay)
                    continue
                raise Exception(
                    f"Network error after {max_retries} attempts: {str(e)}"
                ) from e

        raise Exception(f"Request to {url} failed after {max_retries} attempts.")

    def _handle_current_api_response(
        self, url: str, params: str, verbose: bool = False
    ) -> pd.DataFrame:
        """
        Create and handles the response from the API.

        If the response is valid, return a dataframe of the response.
        Otherwise, raise an exception. This is a helper function for the `_fetch_current_status_df` and `_fetch_monitor_history_df` functions.
        Loops through the API calls until all the records are fetched. If verbose is set to True, the function will print the full dataframe
        to the console.
        """
        df = pd.DataFrame()
        while True:
            r = self._request_with_retries(url=url, params=params)
            response = r.json()
            # If no items are returned, we have fetched everything
            if "items" not in response:
                print("\033[36m" + "\tNo more records to fetch" + "\033[0m")
                break
            df_temp = pd.json_normalize(response["items"])
            df_temp = self._transform_api_response(
                df_temp
            )  # Transform from v2 format for compatibility
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

    def _handle_history_api_response(
        self, url: str, params: str, since: datetime | None = None
    ) -> pd.DataFrame:
        """
        Create and handles the response from the API.

        If the response is valid, it returns a dataframe of the response.
        Otherwise, it raises an exception. The function loops through the API calls until a record is returned that has a datetime
        that exceeds the `since` date (defaulting to `HISTORY_VALID_UNTIL`). This differs from the `handle_api_response` function in that it does not try
        to fetch all records, but only those until a certain date. This allows for more elegant handling of the error whereby the
        API erroneously returns an empty dataframe in place of an error message. Note that there is one case where this function
        will behave unexpectedly: if the number of records returned is exactly an integer multiple of the API limit number of events
        (e.g., 0, 1000, 2000 etc.), then the function will return an empty dataframe. This is because the function cannot distinguish
        between this case and the case where the API genuinely returns no records. This is the fault of the API, not this code but
        it is something to be aware of, and needs to be fixed.

        See also the `handle_current_api_response` function.
        """
        fetch_back_to = since if since is not None else self.HISTORY_VALID_UNTIL
        print(
            "\033[36m"
            + f"\tRequesting historical events since {fetch_back_to}..."
            + "\033[0m"
        )
        df = pd.DataFrame()

        while True:
            r = self._request_with_retries(url=url, params=params)

            # Process the successful response
            response = r.json()
            # If no items are returned, handle it here. Think hard on how to handle this.
            if "items" not in response:
                # Raise an exception if the response is empty.
                nrecords = df.shape[0]

                # TODO: handle this exception more elegantly...
                # ...Maybe if last record returned is really close to HISTORY_VALID_UNTIL then don't raise an exception.
                # Cannot just return records because it gives false impression that all records have been fetched.
                raise Exception(
                    f"\n\t!ERROR! \n\tAPI returned no items for request: {r.url} \n\t! ABORTING !"
                    + "\n\t"
                    + "-" * 80
                    + "\n\tThis error is *probably* caused by the API erroneously returning an empty response in place of an error..."
                    + "\n\t...but it could also be caused by the API genuinely returning no records."
                    + "\n\tThis might occur if there have been *exactly* an integer multiple of the API limit number of events (e.g., 0, 1000, 2000 etc.)."
                    + "\n\tAt present there is no way to distinguish between these two cases (which is the fault of the API, not this code)."
                    + "\n\tIf you think this is the case, try using the _handle_current_api_response function instead or modifying the `since` date."
                    + "\n\t"
                    + "-" * 80
                    + f"\n\tNumber of records fetched before error: {nrecords}"
                )
            else:
                df_temp = pd.json_normalize(response["items"])
                df_temp = self._transform_api_response(
                    df_temp
                )  # Transform from v2 format for compatibility
                # Extract the datetime of the last record fetched and cast it to a datetime object
                last_record_datetime = pd.to_datetime(df_temp["DateTime"].iloc[-1])
                if last_record_datetime < fetch_back_to:
                    print(
                        "\033[36m"
                        + f"\tFound a record with datetime {last_record_datetime} before `fetch back to' date {fetch_back_to}."
                        + "\033[0m"
                    )

                    # When an explicit `since` was given we only want that window,
                    # so crossing the bound is itself the stop signal: there is no
                    # need to keep paging back towards the start of the record.
                    # (Records come back newest-first, and a page is a full 1000
                    # long right up until the very end of the record, so the
                    # short-page test below would otherwise never fire early.)
                    if since is not None:
                        print(
                            "\033[36m"
                            + "\tReached the requested window; no more records to fetch!"
                            + "\033[0m"
                        )
                        df = pd.concat([df, df_temp])
                        break

                    # Check the number of rows and compare to the API limit
                    if df_temp.shape[0] < self.API_LIMIT:
                        # If the number of records is less than the API limit, then we have fetched all records
                        print(
                            "\033[36m"
                            + f"\tLast request contained {df_temp.shape[0]} many records, fewer than the API limit of {self.API_LIMIT}."
                            + "\033[0m"
                        )
                        print("\033[36m" + "\tNo more records to fetch!" + "\033[0m")
                        df = pd.concat([df, df_temp])
                        break
                    else:
                        # If the number of records is equal to the API limit, possibly more records to fetch so we continue.
                        print(
                            "\033[36m"
                            + f"\tLast request contained {df_temp.shape[0]} many records, equal to the API limit of {self.API_LIMIT}."
                            + "\033[0m"
                        )
                        print(
                            "\033[36m"
                            + "\tChecking if there are more records to fetch..."
                            + "\033[0m"
                        )
            df = pd.concat([df, df_temp])
            params["offset"] += params["limit"]  # Increment offset for the next request
        df.reset_index(drop=True, inplace=True)
        return df

    def _fetch_monitor_history(
        self, monitor: Monitor, verbose: bool = False
    ) -> list[Event]:
        """
        Create a list of historical Event objects from the alert stream for a given monitor.

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
        events_df = self._fetch_monitor_events_df(monitor, verbose=verbose)
        return self._alerts_df_to_events_list(df=events_df, monitor=monitor)

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Thames Water active API response to a Monitor object.

        See `_fetch_current_status_df`
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
        Convert a row of the Thames Water active API response to an Event object.

        See `_fetch_current_status_df`
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


def _process_monitor_history_pl(
    args: tuple[
        str,
        pd.DataFrame,
        dict[str, Monitor],
        Callable[[pd.DataFrame, Monitor], list[Event]],
    ],
) -> tuple[str, list[Event]]:
    """
    Process a single monitor's history in parallel.

    This function is used in the `set_all_histories_parallel` method of the `ThamesWater` class.

    Args:
        args: A tuple containing:
            - name: The name of the monitor.
            - df: The DataFrame containing historical data.
            - active_monitors: A dictionary of active monitors.
            - alerts_df_to_events_list: A function to convert DataFrame to events list.

    """
    name, df, active_monitors, alerts_df_to_events_list = args
    subset = df[df["LocationName"] == name]
    monitor = active_monitors[name]
    history = alerts_df_to_events_list(subset, monitor)
    return name, history
