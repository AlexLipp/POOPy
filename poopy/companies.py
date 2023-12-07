import requests
import pandas as pd
from typing import Dict

from poopy.poopy import Monitor, WaterCompany, Discharge, NoDischarge, Offline, Event


class ThamesWater(WaterCompany):
    """
    A subclass of `WaterCompany` that represents the EDM monitoring network for Thames Water.
    """

    def __init__(self, clientID: str, clientSecret: str):
        print("\033[36m" + "Initialising Thames Water object..." + "\033[0m")
        print("\033[36m" + "\tFetching model grid..." + "\033[0m")
        print(
            "\033[36m"
            + "\tThis download can be slow if it is the first time you have generated a ThamesWater object. \n\tThe file will be cached so will be faster on subsequent uses..."
            + "\033[0m"
        )
        super().__init__(clientID, clientSecret)
        self._name = "ThamesWater"
        self._model_grid_file_path = self._fetch_model_grid_file(
            url="https://zenodo.org/records/10280997/files/tw_model_grid.zip",
            known_hash="md5:5da73b57c3b1587476594601943356e9",
        )

    def _fetch_active_monitors(self) -> Dict[str, Monitor]:
        """
        Get the current status of the monitors by calling the API.
        """
        df = self._get_current_status_df(self._clientID, self._clientSecret)
        monitors = {}
        for _, row in df.iterrows():
            monitor = self._row_to_monitor(row=row)
            event = self._row_to_event(row=row, monitor=monitor)
            monitor.current_event = event
            monitors[monitor.site_name] = monitor
        return monitors

    def _get_current_status_df(self, clientID: str, clientSecret: str) -> pd.DataFrame:
        """
        Get the current status of the monitors by calling the API.
        """

        # Change the following to indent by a tab and print it in blue print("Requesting current status data from Thames Water API...")
        print(
            "\033[36m"
            + "Requesting current status data from Thames Water API..."
            + "\033[0m"
        )
        api_root = "https://prod-tw-opendata-app.uk-e1.cloudhub.io"
        api_resource = "/data/STE/v1/DischargeCurrentStatus"
        url = api_root + api_resource
        params = ""
        # send the request
        r = requests.get(
            url,
            headers={"client_id": clientID, "client_secret": clientSecret},
            params=params,
        )
        print("\033[36m" + "\tRequesting from " + r.url + "\033[0m")
        # check response status and use only valid requests
        if r.status_code == 200:
            response = r.json()
            df = pd.json_normalize(response, "items")
        else:
            raise Exception(
                "\tRequest failed with status code {0}, and error message: {1}".format(
                    r.status_code, r.json()
                )
            )

        if df.shape[0] == 1000:
            raise Exception(
                "\tWarning: Number of outputs is at or exceeds 1000 output limit. \nOutputs may be incomplete"
            )
        return df

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
