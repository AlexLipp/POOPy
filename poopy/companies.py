from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Callable, Dict, List, Tuple
import warnings

import pandas as pd
import requests
from osgeo import osr

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
    HISTORY_VALID_UNTIL = datetime(2022, 4, 1, 0, 30, 0)
    # This is the date until by which the EDM monitors had been attached to the API and so the
    # point at which the record becomes valid. Note however that most records we actually
    # not attached until 1/1/2023, so its not sensible to compare records before this date.

    # A large number of monitors have exactly 1st April 2022 as their first record,
    # and a long period of offline that follows.

    # The URL and hash of the D8 raster file on the server
    D8_FILE_URL = "https://zenodo.org/records/13882300/files/thames_d8.nc?download=1"
    D8_FILE_HASH = "md5:1047a14906237cd436fd483e87c1647d"

    def __init__(self, clientID: str, clientSecret: str):
        print("\033[36m" + "Initialising Thames Water object..." + "\033[0m")
        super().__init__(clientID, clientSecret)
        self._name = "ThamesWater"
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def set_all_histories(self) -> None:
        """
        Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        A faster version of this function is available in the `set_all_histories_parallel` method.
        """
        self._history_timestamp = datetime.now()
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
        print("\033[36m" + f"Building history for monitors..." + "\033[0m")
        for name in active_names:
            subset = df[df["LocationName"] == name]
            monitor = self.active_monitors[name]
            monitor._history = self._alerts_df_to_events_list(subset, monitor)

    def set_all_histories_parallel(self) -> None:
        """
        Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        Faster than the `set_all_histories` method if multiple cores are available.
        """
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
        print("\033[36m" + f"Building history for monitors..." + "\033[0m")

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

    def _fetch_all_monitors_history_df(self) -> pd.DataFrame:
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
        Otherwise, raise an exception. This is a helper function for the `_fetch_current_status_df` and `_fetch_monitor_history_df` functions.
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
        df = self._fetch_current_status_df()
        monitors = {}
        for _, row in df.iterrows():
            monitor = self._row_to_monitor(row=row)
            event = self._row_to_event(row=row, monitor=monitor)
            monitor.current_event = event
            monitors[monitor.site_name] = monitor
        return monitors

    def _fetch_monitor_history(
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
        events_df = self._fetch_monitor_events_df(monitor, verbose=verbose)
        return self._alerts_df_to_events_list(df=events_df, monitor=monitor)

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Thames Water active API response to a Monitor object. See `_fetch_current_status_df`
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
        Convert a row of the Thames Water active API response to an Event object. See `_fetch_current_status_df`
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


class WelshWater(WaterCompany):
    """
    Creates an object to interact with the WelshWater EDM API.
    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services3.arcgis.com/KLNF7YxtENPLYVey/arcgis/rest/services"
    CURRENT_API_RESOURCE = (
        "/Spill_Prod/FeatureServer/0/query?where=1=1&outFields=*&f=json"
    )
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 2000  # Max num of outputs that can be requested from the API at once

    D8_FILE_URL = "https://zenodo.org/records/13882300/files/welsh_d8.nc?download=1"
    D8_FILE_HASH = "md5:8c965ad0597929df3bc54bc728ed8404"

    def __init__(self, clientID="", clientSecret=""):
        # No auth required for this API so no need to pass in clientID and clientSecret
        print("\033[36m" + "Initialising Welsh Water object..." + "\033[0m")
        super().__init__(clientID, clientSecret)
        self._name = "WelshWater"
        self._d8_file_path = self._fetch_d8_file(
            url=self.D8_FILE_URL,
            known_hash=self.D8_FILE_HASH,
        )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """
        Get the current status of the monitors by calling the API.
        """
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
        Creates and handles the response from the API. If the response is valid, return a dataframe of the response.
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
                "\tRequest failed with status code {0}, and error message: {1}".format(
                    r.status_code, r.json()
                )
            )
        df.reset_index(drop=True, inplace=True)
        # if number of rows is exactly the API limit, there may be more records to fetch so print a warning
        if df.shape[0] == self.API_LIMIT:
            warnings.warn(
                "\033[36m"
                + "\tNumber of records fetched is equal to the API limit of {0}. There may be missing records!".format(
                    self.API_LIMIT
                )
                + "\033[0m"
            )
        return df

    def _fetch_active_monitors(self) -> Dict[str, Monitor]:
        """
        Returns a dictionary of Monitor objects representing the active monitors.
        """
        df = self._fetch_current_status_df()
        monitors = {}
        for _, row in df.iterrows():
            monitor = self._row_to_monitor(row=row)
            event = self._row_to_event(row=row, monitor=monitor)
            monitor.current_event = event
            monitors[monitor.site_name] = monitor
        return monitors

    def _fetch_monitor_history(self, monitor: Monitor) -> List[Event]:
        """
        Not available for WW API.
        """
        # Print a helpful message to the user that this function is not available for this API
        print(
            "\033[36m"
            + "This function is not available for the Welsh Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """
        Not available for WW API.
        """
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
        Convert a row of the Welsh Water active API response to a Monitor object. See `_fetch_current_status_df`
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
        Convert a row of the Welsh Water active API response to an Event object. See `_fetch_current_status_df`
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


class SouthernWater(WaterCompany):
    """
    Creates an object to interact with the SouthernWater EDM API.
    There is no auth on this endpoint required currently.
    There is only a current status endpoint, no historical endpoint available.
    """

    API_ROOT = "https://services-eu1.arcgis.com/XxS6FebPX29TRGDJ/arcgis/rest/services/"
    CURRENT_API_RESOURCE = (
        "Southern_Water_Storm_Overflow_Activity/FeatureServer/0/query"
    )
    HISTORICAL_API_RESOURCE = ""
    API_LIMIT = 1000  # Max num of outputs that can be requested from the API at once

    D8_FILE_URL = ""
    D8_FILE_HASH = ""

    def __init__(self, clientID="", clientSecret=""):
        # No auth required for this API so no need to pass in clientID and clientSecret
        print("\033[36m" + "Initialising Southern Water object..." + "\033[0m")
        super().__init__(clientID, clientSecret)
        self._name = "SouthernWater"
        # self._d8_file_path = self._fetch_d8_file(
        #     url=self.D8_FILE_URL,
        #     known_hash=self.D8_FILE_HASH,
        # )
        self._alerts_table = f"{self._name}_alerts.csv"
        self._alerts_table_update_list = f"{self._name}_alerts_update_list.dat"

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """
        Get the current status of the monitors by calling the API.
        """
        print(
            "\033[36m"
            + "Requesting current status data from Southern Water API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.CURRENT_API_RESOURCE
        params = {
            "outFields": "*",
            "where": "1=1",
            "f": "json",
            "resultOffset": 0,
            "resultRecordCount": 1000,  # Adjust the limit as needed
        }
        df = self._handle_current_api_response(url=url, params=params)

        return df

    def _handle_current_api_response(
        self, url: str, params: str, verbose: bool = False
    ) -> pd.DataFrame:
        """
        Creates and handles the response from the API. If the response is valid, return a dataframe of the response.
        Otherwise, raise an exception. This is a helper function for the `_fetch_current_status_df` and `_fetch_monitor_history_df` functions.
        Loops through the API calls until all the records are fetched. If verbose is set to True, the function will print the full dataframe
        to the console.
        """
        df = pd.DataFrame()
        while True:
            response = requests.get(url, params=params)
            print("\033[36m" + "\tRequesting from " + response.url + "\033[0m")

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                # If no features are returned, break the loop
                if "features" not in data or not data["features"]:
                    print("\033[36m" + "\tNo more records to fetch" + "\033[0m")
                    break
                else:
                    # Extract attributes from the JSON response
                    attributes = [feature["attributes"] for feature in data["features"]]
                    # Convert the attributes to a DataFrame
                    df_temp = pd.DataFrame(attributes)
                    df = pd.concat([df, df_temp], ignore_index=True)
            else:
                raise Exception(
                    "\tRequest failed with status code {0}, and error message: {1}".format(
                        response.status_code, response.json()
                    )
                )

            # Increment offset for the next request
            params["resultOffset"] += params["resultRecordCount"]

        # Print the full dataframe to the console if verbose is set to True
        if verbose:
            print("\033[36m" + "\tPrinting full API response..." + "\033[0m")
            with pd.option_context(
                "display.max_rows", None, "display.max_columns", None
            ):
                print(df)

        return df

    def _fetch_active_monitors(self) -> Dict[str, Monitor]:
        """
        Returns a dictionary of Monitor objects representing the active monitors.
        """
        df = self._fetch_current_status_df()
        monitors = {}
        for _, row in df.iterrows():
            monitor = self._row_to_monitor(row=row)
            event = self._row_to_event(row=row, monitor=monitor)
            monitor.current_event = event
            monitors[monitor.site_name] = monitor
        return monitors

    def _fetch_monitor_history(self, monitor: Monitor) -> List[Event]:
        """
        Not available for Southern API.
        """
        print(
            "\033[36m"
            + "This function is not available for the Southern Water API."
            + "\033[0m"
        )
        pass
        return

    def set_all_histories(self) -> None:
        """
        Not available for Southern API.
        """
        print(
            "\033[36m"
            + "This function is not available for the Southern Water API."
            + "\033[0m"
        )
        pass
        return

    def _row_to_monitor(self, row: pd.DataFrame) -> Monitor:
        """
        Convert a row of the Southern Water active API response to a Monitor object. See `_fetch_current_status_df`
        """
        current_time = (
            self._timestamp
        )  # Get the current time which corresponds to when the API was called (so it is same for all monitors)

        x, y = latlong_to_osgb(row["Latitude"], row["Longitude"])

        # if row["latestEventEnd"] is not nan, convert it to datetime, else set it to None
        if not pd.isna(row["LatestEventEnd"]):
            last_event_end = datetime.utcfromtimestamp(row["LatestEventEnd"] / 1000)
        else:
            last_event_end = None

        # if monitor currently discharging we set last_48h to be True.
        if row["Status"] == 1:
            last_48h = True

        # if monitor not currently discharging, we check if it has discharged in the last 48 hours
        elif row["Status"] == 0:
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

        return Monitor(
            site_name=row["Id"],  # Southern Water does not provide a site name
            permit_number="Unknown",  # Assuming that the permit number is the ID
            x_coord=x,
            y_coord=y,
            receiving_watercourse=row["ReceivingWaterCourse"],
            water_company="dummy",
            discharge_in_last_48h=last_48h,
        )

    def _row_to_event(self, row: pd.DataFrame, monitor: Monitor) -> Event:
        """
        Convert a row of the Welsh Water active API response to an Event object. See `_fetch_current_status_df`
        """
        if row["Status"] == 1:
            event = Discharge(
                monitor=monitor,
                ongoing=True,
                start_time=datetime.utcfromtimestamp(row["StatusStart"] / 1000),
            )
        elif row["Status"] == 0:
            event = NoDischarge(
                monitor=monitor,
                ongoing=True,
                start_time=datetime.utcfromtimestamp(row["StatusStart"] / 1000),
            )
        else:
            raise Exception(
                "Unknown status type " + row["Status"] + " for monitor " + row["Id"]
            )
        return event


def latlong_to_osgb(lat, lon):
    # Define the WGS84 spatial reference system
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)  # WGS84

    # Define the OSGB36 spatial reference system
    osgb36 = osr.SpatialReference()
    osgb36.ImportFromEPSG(27700)  # OSGB36

    # Create a coordinate transformation
    transform = osr.CoordinateTransformation(wgs84, osgb36)

    # Transform the coordinates
    x, y, _ = transform.TransformPoint(lat, lon)
    return x, y


def _process_monitor_history_pl(
    args: Tuple[
        str,
        pd.DataFrame,
        Dict[str, Monitor],
        Callable[[pd.DataFrame, Monitor], List[Event]],
    ]
) -> Tuple[str, List[Event]]:
    """
    Process a single monitor's history in parallel. This function is used in
    the `set_all_histories_parallel` method of the `ThamesWater` class.

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
