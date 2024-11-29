import datetime
import warnings
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Tuple
import os
import requests

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pooch
from geojson import MultiLineString, Feature, FeatureCollection, Point
from matplotlib.colors import LogNorm

from poopy.d8_accumulator import D8Accumulator


class Monitor:
    """A class to represent a CSO monitor.

    Attributes:
        site_name: The name of the site.
        permit_number: The permit number of the monitor.
        x_coord: The X coordinate of the site.
        y_coord: The Y coordinate of the site.
        receiving_watercourse: The receiving watercourse of the site.
        water_company: The water company that the monitor belongs to.
        current_status: The current status of the monitor.
        discharge_in_last_48h: Whether the monitor has discharged in the last 48 hours.
        current_event: The current event at the monitor.
        history: The history of events at the monitor.

    Methods:
        print_status: Print the current status of the monitor.
        get_history: Get the historical discharge information for the monitor and store it in the history attribute.
        plot_history: Plot the history of events at the monitor. Optionally specify a start date to plot from.
        total_discharge: Returns the total discharge in minutes since the given datetime.
        total_discharge_last_6_months: Returns the total discharge in minutes in the last 6 months (183 days)
        total_discharge_last_12_months: Returns the total discharge in minutes in the last 12 months (365 days)
        total_discharge_since_start_of_year: Returns the total discharge in minutes since the start of the year
        event_at: Returns the event that was occurring at the given time for the given monitor (or None).
        recent_discharge_at: Returns whether there was a discharge event in the preceding 48 hours of a specified time.
    """

    def __init__(
        self,
        site_name: str,
        permit_number: str,
        x_coord: float,
        y_coord: float,
        receiving_watercourse: str,
        water_company: "WaterCompany",
        discharge_in_last_48h: Optional[bool] = None,
    ) -> None:
        """
        Initialize attributes to describe a CSO monitor.

        Args:
            site_name: The name of the site.
            permit_number: The permit number of the monitor.
            x_coord: The X coordinate of the site.
            y_coord: The Y coordinate of the site.
            receiving_watercourse: The receiving watercourse of the site.
            water_company: The water company that the monitor belongs to.
            discharge_in_last_48h: Whether the monitor has discharged in the last 48 hours.
        """
        self._site_name: str = site_name
        self._permit_number: str = permit_number
        self._x_coord: float = x_coord
        self._y_coord: float = y_coord
        self._receiving_watercourse: str = receiving_watercourse
        self._water_company: WaterCompany = water_company
        self._discharge_in_last_48h: bool = discharge_in_last_48h
        self._current_event: Event = None
        self._history: List[Event] = None

    @property
    def site_name(self) -> str:
        """Return the name of the site."""
        return self._site_name

    @property
    def permit_number(self) -> str:
        """Return the permit number of the monitor."""
        return self._permit_number

    @property
    def x_coord(self) -> float:
        """Return the X coordinate of the site."""
        return self._x_coord

    @property
    def y_coord(self) -> float:
        """Return the Y coordinate of the site."""
        return self._y_coord

    @property
    def receiving_watercourse(self) -> str:
        """Return the receiving watercourse of the site."""
        return self._receiving_watercourse

    @property
    def water_company(self) -> "WaterCompany":
        """Return the water company that the monitor belongs to."""
        return self._water_company

    @property
    def current_status(self) -> str:
        """Return the current status of the monitor."""
        return self._current_event.event_type

    @property
    def current_event(self) -> "Event":
        """Return the current event of the monitor.

        Raises:
            ValueError: If the current event is not set.
        """
        if self._current_event is None:
            raise ValueError("Current event is not set.")
        return self._current_event

    def get_history(self, verbose: bool = False) -> None:
        """
        Get the historical data for the monitor and store it in the history attribute.

        Args:
            verbose: Whether to print the dataframe of API responses when the history is set. Defaults to False.
        """
        self._history = self.water_company._fetch_monitor_history(self, verbose=verbose)

    @property
    def history(self) -> List["Event"]:
        """Return a list of all past events at the monitor.

        Raises:
            ValueError: If the history is not yet set.
        """
        if self._history is None:
            raise ValueError("History is not yet set!")
        return self._history

    @property
    def discharge_in_last_48h(self) -> bool:
        # Raise a warning if the discharge_in_last_48h is not set
        if self._discharge_in_last_48h is None:
            warnings.warn(
                "\033[91m"
                f"!ADVISORY! Information on discharges in last 48hrs could not be set for '{self.site_name}'. `.discharge_in_last_48h` attribute returns None."
                + "\033[0m"
            )
        return self._discharge_in_last_48h

    @current_event.setter
    def current_event(self, event: "Event") -> None:
        """Set the current event of the monitor.

        Raises:
            ValueError: If the current event is not ongoing.
        """
        if not event.ongoing:
            raise ValueError("Current Event must be ongoing.")
        else:
            self._current_event = event

    def print_status(self) -> None:
        """Print the current status of the monitor."""
        if self._current_event is None:
            print("No current event at this Monitor.")
        self._current_event.print()

    def total_discharge(self, since: datetime.datetime = None) -> float:
        """Returns the total discharge in minutes since the given datetime.
        If no datetime is given, it will return the total discharge since records began
        """
        history = self.history
        total = 0.0
        if since is None:
            since = datetime.datetime(2000, 1, 1)  # A long time ago
        for event in history:
            if event.event_type == "Discharging":
                if event.ongoing:
                    if event.start_time < since:
                        # If the start time is before the cut off date, we can take the difference between the current time and the cut off date
                        total += (datetime.datetime.now() - since).total_seconds() / 60
                    else:
                        total += event.duration
                else:
                    # If the end time is before the cut off date, we can skip this event
                    if event.end_time < since:
                        continue
                    # If the endtime is after since but start_time is before, we take the difference between the end time and since
                    elif (event.end_time > since) and (event.start_time < since):
                        total += (event.end_time - since).total_seconds() / 60
                    elif event.end_time > since:
                        total += event.duration
        return total

    def total_discharge_last_6_months(self) -> float:
        """Returns the total discharge in minutes in the last 6 months (183 days)"""
        return self.total_discharge(
            since=datetime.datetime.now() - datetime.timedelta(days=183)
        )

    def total_discharge_last_12_months(self) -> float:
        """Returns the total discharge in minutes in the last 12 months (365 days)"""
        return self.total_discharge(
            since=datetime.datetime.now() - datetime.timedelta(days=365)
        )

    def total_discharge_since_start_of_year(self) -> float:
        """Returns the total discharge in minutes since the start of the year"""
        return self.total_discharge(
            since=datetime.datetime(datetime.datetime.now().year, 1, 1)
        )

    def plot_history(self, since: datetime.datetime = None) -> None:
        """Plot the history of events at the monitor. Optionally specify a start date to plot from.
        If no start date is specified, it will plot from the first recorded Discharge or Offline event.
        If no events are recorded for that monitor, no plot will be returned and a warning will be raised.
        """
        events = self.history
        if len(events) == 0:
            warnings.warn(
                "\033[91m"
                + f"\n!ADVISORY! Monitor '{self.site_name}' has no recorded events. Returning None."
                + "\033[0m"
            )

        else:
            plt.figure(figsize=(10, 2))
            for event in events:
                start = event.start_time
                if event.ongoing:
                    end = datetime.datetime.now()
                else:
                    end = event.end_time
                if event.event_type == "Discharging":
                    color = "#8B4513"
                if event.event_type == "Offline":
                    color = "grey"
                if event.event_type == "Not Discharging":
                    continue

                # Create a figure that is wide and not very tall
                # Plot a polygon for each event that extends from the start to the end of the event
                # and from y = 0 to y = 1
                plt.fill_between([start, end], 0, 1, color=color)
                # Set the title to the name of the monitor
            # Remove all y axis ticks and labels
            plt.yticks([])
            plt.ylabel("")
            plt.ylim(0, 1)
            # Set the x axis limits to the start and end of the event list
            if since is None:
                minx, maxx = events[-1].start_time, datetime.datetime.now()
            else:
                minx, maxx = since, datetime.datetime.now()
            plt.xlim(minx, maxx)
            total_discharge = self.total_discharge(since=since)
            plt.title(
                self.site_name
                + "\n"
                + f"Total Discharge: {round(total_discharge,2)} minutes"
            )
            plt.tight_layout()
            plt.show()

    def event_at(self, time: datetime.datetime) -> Union[None, "Event"]:
        """
        Returns the event that is ongoing at the given time for the given monitor.
        If no event is found (e.g., the time was before a monitor was installed), it returns None.

        Args:
            time: The time to check for an event.

        Returns:
            The event that is ongoing at the given time for the given monitor.
        """
        out = None
        now = datetime.datetime.now()
        # Check if time is in the future and return none raising a warning in red:
        if time > now:
            warnings.warn(
                "\033[91m"
                + f"\n!WARNING! Time {time} is in the future. Returning None."
                + "\033[0m"
            )
            return out
        for event in self.history:
            start = event.start_time
            if event.ongoing:
                end = now
            else:
                end = event.end_time
            if start < time and time < end:
                # If the event is not ongoing but the time is between the start and end time, then it is the current event
                out = event
                return out
        warnings.warn(
            f"\033[31m\n! WARNING ! No event found at {time} for {self.site_name}. \nProbably the monitor was not active at that time OR has no recorded events. \033[0m"
        )
        return out

    def recent_discharge_at(self, time: datetime.datetime) -> bool:
        """
        This function checks if there was a discharge event in the preceding 48 hours of a specified time.

        Args:
            time (datetime.datetime): The time to check for a discharge event.

        Returns:
            bool: True if there was a discharge event in the preceding 48 hours of the specified time, False otherwise.

        Raises:
            ValueError: If the target time is in the future.
        """

        now = datetime.datetime.now()
        discharge_in_last_48_hours: bool = False
        # Raise a value error if the target time is in the future
        if time > now:
            raise ValueError("The target time cannot be in the future.")

        # Loop through the events in the monitor's history
        for i in range(len(self.history)):
            event = self.history[i]
            start = event.start_time
            if event.ongoing:
                end = now
            else:
                end = event.end_time
            if start < time and time < end:
                # We have found the event containing target time
                if event.event_type == "Discharging":
                    # This event itself is a discharge, so we can quit the loop
                    discharge_in_last_48_hours = True
                    return discharge_in_last_48_hours
                else:
                    # This event was not a discharge, but we check the preceding events for a recent discharge
                    while i + 2 < len(self.history):
                        prev_event = self.history[i + 1]
                        time_diff = time - prev_event.end_time
                        if time_diff > datetime.timedelta(hours=48):
                            # This event ended more than 48 hours before target time, so we can finish the search
                            return discharge_in_last_48_hours
                        elif prev_event.event_type == "Discharging":
                            # This event was a discharge and ended within 48 hours of target time so we can quit having found a recent discharge
                            discharge_in_last_48_hours = True
                            return discharge_in_last_48_hours
                        else:
                            # This event was within 48 hours but was not a discharge, so we move on to the next event to check that one too
                            i += 1
                    # Searched all the previous events and found no recent discharges so we can finish the search, returning False
                    return discharge_in_last_48_hours
        # If we reach this point, it means that there were no events found at the target time
        warnings.warn(
            f"\033[31m\n! WARNING ! No event found at {time} for {self.site_name}. \nProbably the monitor was not active at that time OR has no recorded events. \033[0m"
        )
        return discharge_in_last_48_hours

    def _history_masks(
        self, times: List[datetime.datetime]
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        online = np.zeros(len(times), dtype=bool)
        active = np.zeros(len(times), dtype=bool)
        recent = np.zeros(len(times), dtype=bool)
        """
        Returns three boolean arrays that indicate, respectively, whether the monitor was online, active,
        or recently active (within 48 hours) at each time given in the times list. The times list should be
        regularly spaced in 15 minute intervals. The arrays are returned in the same order as the times list.
        This is a hidden method that is used by the get_monitor_timeseries() method in the WaterCompany class.

        Args:
            times: A list of times to check the monitor status at.

        Returns:
            A tuple of three boolean arrays indicating whether the monitor was online, active, or recently active.

        """

        if len(self.history) == 0:
            print(f"Monitor {self.site_name} has no recorded events")
            return online, active, recent

        first_event = round_time_down_15(self.history[-1].start_time)
        # If first event is before the first time in the times list, then we need to fill the online array with 1s
        if first_event < times[0]:
            online[:] = True
        else:
            online[times.index(first_event) :] = True

        for event in self.history:
            if event.event_type == "Discharging" or event.event_type == "Offline":
                start_round = round_time_down_15(event.start_time)
                if start_round < times[0]:
                    # Quit loop if start_round is before the first time in the times list
                    break
                if event.ongoing:
                    # If the event is ongoing, then we can set the active array to True from the start_round to the end of
                    # the array
                    if event.event_type == "Discharging":
                        active[times.index(start_round) :] = True
                        recent[times.index(start_round) :] = True
                    else:
                        online[times.index(start_round) :] = False
                else:
                    # If the event is not ongoing, then we can set the active array to True from the start_round to the
                    # end_round
                    end_round = round_time_up_15(event.end_time)
                    if event.event_type == "Discharging":
                        active[times.index(start_round) : times.index(end_round)] = True
                        # Set recent to True from start_round to 48 hours after end_round
                        recent_end = end_round + datetime.timedelta(hours=48)
                        if recent_end > times[-1]:
                            # If recent_end is after the end of the array, then set recent to True from start_round to the end of the array
                            recent[times.index(start_round) :] = True
                        else:
                            recent[
                                times.index(start_round) : times.index(recent_end)
                            ] = True
                    else:
                        online[times.index(start_round) : times.index(end_round)] = (
                            False
                        )

        return online, active, recent


class Event(ABC):
    """A class to represent an event at a CSO monitor.

    Attributes:
        monitor: The monitor at which the event occurred.
        ongoing: Whether the event is ongoing.
        start_time: The start time of the event.
        end_time: The end time of the event.
        duration: The duration of the event.
        event_type: The type of event.

    Methods:
        summary: Print a summary of the event.

    """

    @abstractmethod
    def __init__(
        self,
        monitor: Monitor,
        ongoing: bool,
        start_time: datetime.datetime,
        end_time: Optional[datetime.datetime] = None,
        event_type: Optional[str] = "Unknown",
    ) -> None:
        """
        Initialize attributes to describe an event.

        Args:
            monitor: The monitor at which the event occurred.
            ongoing: Whether the event is ongoing.
            start_time: The start time of the event.
            end_time: The end time of the event. Defaults to None.
            event_type: The type of event. Defaults to "Unknown".

        Methods:
            print: Print a summary of the event.
        """
        self._monitor = monitor
        self._start_time = start_time
        self._ongoing = ongoing
        self._end_time = end_time
        self._event_type = event_type
        self._duration = self.duration
        self._validate()

    def _validate(self):
        """Validate the attributes of the event.

        Raises:
            ValueError: If the end time is before the start time.
            ValueError: If the end time is not None and the event is ongoing.
        """
        if self._ongoing and self._end_time is not None:
            raise ValueError("End time must be None if the event is ongoing.")
        if self._end_time is not None and self._end_time < self._start_time:
            raise ValueError("End time must be after the start time.")

    @property
    def duration(self) -> float:
        """Return the duration of the event in minutes."""
        if self._start_time is not None:
            if not self.ongoing:
                return (self._end_time - self._start_time).total_seconds() / 60
            else:
                return (datetime.datetime.now() - self._start_time).total_seconds() / 60
        else:
            # If the start time is None, return nan (i.e., the event has no sensible duration)
            return np.nan

    @property
    def ongoing(self) -> bool:
        """Return if the event is ongoing."""
        return self._ongoing

    @property
    def start_time(self) -> Optional[datetime.datetime]:
        """Return the start time of the event."""
        if self._start_time is None:
            warnings.warn(
                "\033[91m"
                + f"!ADVISORY! For {self.event_type} event for '{self.monitor.site_name}' (in {self.monitor.water_company.name}) start time information is not available. `start_time` attribute returns None."
                + "\033[0m"
            )
        return self._start_time

    @property
    def end_time(self) -> Union[datetime.datetime, None]:
        """Return the end time of the event."""
        # If the event is Ongoing raise a Warning that the event is ongoing and has no end time but allow program to continue
        if self._ongoing:
            warnings.warn(
                "\033[91m"
                + f"!ADVISORY! This {self.event_type} event for '{self.monitor.site_name}' (in {self.monitor.water_company.name}) is ongoing. `end_time` attribute returns None."
                + "\033[0m"
            )
        return self._end_time

    @property
    def event_type(self) -> str:
        """Return the type of event."""
        return self._event_type

    @property
    def monitor(self) -> Monitor:
        """Return the monitor at which the event occurred."""
        return self._monitor

    # Define a setter for ongoing that only allows setting to False. It then sets the end time to the current time, and calculates the duration.
    @ongoing.setter
    def ongoing(self, value: bool) -> None:
        """Set the ongoing status of the event.

        Args:
            value: The value to set the ongoing status to.

        Raises:
            ValueError: If the ongoing status is already False.
            ValueError: If the event is already not ongoing.
        """
        if value:
            raise ValueError("Ongoing status can only be set to False.")
        # Check if the discharge event is already not ongoing
        if not self._ongoing:
            raise ValueError("Event is already not ongoing.")
        else:
            self._ongoing = value
            self._end_time = datetime.datetime.now()
            self._duration = self.duration

    def print(self) -> None:
        """Print a summary of the event."""

        # Define a dictionary of colours for the event types
        event_type_colour = {
            "Discharging": "\033[31m",  # Red
            "Offline": "\033[30m",  # Black
            "Not Discharging": "\033[32m",  # Green
            "Unknown": "\033[0m",  # Default
        }

        print(
            f"""
        {event_type_colour[self.event_type]}
        --------------------------------------
        Event Type: {self.event_type}
        Site Name: {self.monitor.site_name}
        Permit Number: {self.monitor.permit_number}
        OSGB Coordinates: ({self.monitor.x_coord}, {self.monitor.y_coord})
        Receiving Watercourse: {self.monitor.receiving_watercourse}
        Start Time: {self.start_time}
        End Time: {self.end_time if not self.ongoing else "Ongoing"}
        Duration: {round(self.duration,2)} minutes\033[0m
        """
        )

    def _to_row(self) -> pd.DataFrame:
        """
        Convert a discharge event to a row in a dataframe.
        """
        row = pd.DataFrame(
            {
                "LocationName": self.monitor.site_name,
                "PermitNumber": self.monitor.permit_number,
                "X": self.monitor.x_coord,
                "Y": self.monitor.y_coord,
                "ReceivingWaterCourse": self.monitor.receiving_watercourse,
                "StartDateTime": self.start_time,
                "StopDateTime": self.end_time,
                "Duration": self.duration,
                "OngoingEvent": self.ongoing,
            },
            index=[0],
        )
        return row


class Discharge(Event):
    """A class to represent a discharge event at a CSO."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Discharging"


class Offline(Event):
    """A class to represent a CSO monitor being offline."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Offline"


class NoDischarge(Event):
    """A class to represent a CSO not discharging."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Not Discharging"


class WaterCompany(ABC):
    """
    A class that represents the EDM monitoring network for a Water Company.

    Attributes:
        name: The name of the Water Company network (set by the child class).
        timestamp: The timestamp of the last update.
        history_timestamp: The timestamp of the last historical data update (set in the `get_history` method of the child class).
        clientID: The client ID for the Water Company API (set by the child class).
        clientSecret: The client secret for the Water Company API (set by the child class).
        alerts_table: The filename of the table that contains (manually generated) alerts.
        build_all_histories_locally: A method to build the history of all active monitors using the manually created alerts table.
        active_monitors: A dictionary of active monitors accessed by site name.
        active_monitor_names: A list of the names of active monitors.
        accumulator: The D8 flow accumulator for the region of the water company.
        discharging_monitors: A list of all monitors that are currently recording a discharge event.
        recently_discharging_monitors: A list of all monitors that have discharged in the last 48 hours.
    Methods:
        update: Updates the active_monitors list and the timestamp.
        set_all_histories: Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        history_to_discharge_df: Convert a water company's total discharge history to a dataframe
        get_downstream_geojson: Get a geojson of the downstream points for all current discharges in BNG coordinates.
        get_downstream_info_geojson: Get a GeoJSON feature collection of more detailed information at the downstream points for current discharges.
        get_historical_downstream_info_geojson: Get a GeoJSON feature collection of more detailed information at the downstream points for discharges *AT A GIVEN HISTORICAL TIME*.
        plot_current_status: Plot the current status of the Water Company network showing the downstream impact & monitor statuses.
        get_historical_downstream_impact_at: Calculates the downstream extent of all monitors that were discharging (or, optionally, recently discharging) at a given time *AT A GIVEN HISTORICAL TIME*.
    """

    def __init__(self, clientID: str, clientSecret: str):
        """
        Initialize attributes to describe a Water Company network.

        Args:
            clientID: The client ID for the Water Company API.
            clientSecret: The client secret for the Water Company API.
        """
        self._clientID = clientID
        self._clientSecret = clientSecret
        self._timestamp: datetime.datetime = datetime.datetime.now()
        self._active_monitors: Dict[str, Monitor] = self._fetch_active_monitors()
        self._accumulator: D8Accumulator = None
        self._d8_file_path: str = None
        self._history_timestamp: datetime.datetime = (
            None  # Will be set if all monitor histories are set
        )

    @abstractmethod
    def _fetch_monitor_history(self, monitor: Monitor) -> List[Event]:
        """
        Get the history of events for a monitor.

        Args:
            monitor: The monitor for which to get the history.

        Returns:
            A list of events.
        """
        pass

    @abstractmethod
    def set_all_histories(self) -> None:
        """
        Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        """
        pass

    def _fetch_current_status_df(self) -> pd.DataFrame:
        """
        Get the current status of the monitors by calling the API.
        """
        print(
            "\033[36m"
            + f"Requesting current status data from {self.name} API..."
            + "\033[0m"
        )
        url = self.API_ROOT + self.CURRENT_API_RESOURCE
        params = {
            "outFields": "*",
            "where": "1=1",
            "f": "json",
            "resultOffset": 0,
            "resultRecordCount": self.API_LIMIT,  # Adjust the limit as needed
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

    def build_all_histories_locally(self) -> None:
        """
        Uses the manually created alerts table, built from repeated calls to the current status API, to build the history of all active monitors.
        Note that this method is only recommended for use when the API is not available or when the historical data is not available from the API.
        The results will only be as good as the alerts table, which is built from repeated calls to the API.

        IF YOU HAVE NOT BUILT AN ALERTS TABLE BY REGULARLY CALLING THE API, THIS METHOD WILL NOT PRODUCE SENSIBLE RESULTS AND SHOULD NOT BE USED
        """

        # Print a VERY BIG warning in general to say that this method is only recommended if an API is not available and
        # that the results are dependent on the alerts table which is built from repeated calls to the API. If this has
        # not been done the results will be nonsense.
        # Ask the user to confirm they have understood this. Make the message surrounded by a box of #'s and in red

        print(
            "\033[91m"
            + "###############################################################################################"
            + "\n\t\t\t\tIMPORTANT MESSAGE PLEASE READ \nThis method is not recommended for use by most users!"
            + "\nHISTORIES ARE ONLY VALID IF THE .update_alerts_table() method has been called regularly"
            + "\nIf this is not the case THE RESULTS WILL BE NONSENSE"
            + "\n\t\t\t\tDO YOU UNDERSTAND AND WISH TO PROCEED? [y/n]"
            + "\n###############################################################################################"
            + "\033[0m"
        )

        # Wait for user input
        user_input = input("Enter 'y' to proceed or 'n' to cancel...: ").strip().lower()
        if user_input != "y":
            print("Operation cancelled by user.")
            return

        if self.name == "Thames Water":
            warnings.warn(
                "\033[31m"
                + f"! ALERT ! This method is not recommended for Thames Water. Use the historical API instead with .set_all_histories()."
                + "\033[0m"
            )

        # Check if the alerts table exists
        if not os.path.exists(self._alerts_table):
            raise FileNotFoundError(
                f"Alerts table not found at {self.alerts_table}!\
                                    \nTry running update_alerts_table() regularly over a period of time to build the alerts table."
            )

        # Set the history timestamp to the current time
        self._history_timestamp = datetime.datetime.now()
        df = pd.read_csv(self.alerts_table)
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

    def _fetch_d8_file(self, url: str, known_hash: str) -> str:
        """
        Get the path to the D8 file for the catchment. If the file is not present, it will download it from the given url.
        This is all handled by the pooch package. The hash of the file is checked against the known hash to ensure the file is not corrupted.
        If the file is already present in the pooch cache, it will not be downloaded again.
        """
        file_path = pooch.retrieve(url=url, known_hash=known_hash)

        return file_path

    # Define the getters for the WaterCompany class
    @property
    def name(self) -> str:
        """Return the name of the Water Company network."""
        return self._name

    @property
    def timestamp(self) -> datetime.datetime:
        """Return the timestamp of the last update."""
        return self._timestamp

    @property
    def history_timestamp(self) -> datetime.datetime:
        """Return the timestamp of the last historical data update."""
        if self._history_timestamp is None:
            warnings.warn("History has not been set. Returning None.")
            return None
        return self._history_timestamp

    @property
    def clientID(self) -> str:
        """Return the client ID for the API."""
        return self._clientID

    @property
    def clientSecret(self) -> str:
        """Return the client secret for the API."""
        return self._clientSecret

    @property
    def alerts_table(self) -> str:
        """Return the filename of the alerts file."""
        return self._alerts_table

    @property
    def active_monitors(self) -> List[Monitor]:
        """Return the active monitors."""
        return self._active_monitors

    @property
    def active_monitor_names(self) -> List[str]:
        """Return the names of active monitors."""
        return list(self._active_monitors.keys())

    @property
    def discharging_monitors(self) -> List[Monitor]:
        """Return a list of all monitors that are currently recording a discharge event."""
        return [
            monitor
            for monitor in self._active_monitors.values()
            if monitor.current_status == "Discharging"
        ]

    @property
    def recently_discharging_monitors(self) -> List[Monitor]:
        """Return a list of all monitors that have discharged in the last 48 hours."""
        return [
            monitor
            for monitor in self._active_monitors.values()
            if monitor.discharge_in_last_48h
        ]

    @property
    def accumulator(self) -> D8Accumulator:
        """Return the D8 flow accumulator for the area of the water company."""
        if self._accumulator is None:
            self._accumulator = D8Accumulator(self._d8_file_path)
        return self._accumulator

    def update(self):
        """
        Update the active_monitors list and the timestamp.
        """
        self._active_monitors = self._fetch_active_monitors()
        self._timestamp = datetime.datetime.now()

    def _calculate_downstream_impact(
        self, source_monitors: List[Monitor]
    ) -> np.ndarray:
        """
        Given a list of source monitors, returns a 2D array which is the number of upstream discharges at each
        cell in the DEM.

        Args:
            source_monitors: A list of Monitors which we want to calculate the downstream impact of

        Returns:
            2D numpy array of the domain area showing number of discharges upstream of a given point.
        """
        # Extract all the xy coordinates of active discharges
        accumulator = self.accumulator
        # Coords of all sources in OSGB
        source_nodes = [
            accumulator.coord_to_node(discharge.x_coord, discharge.y_coord)
            for discharge in source_monitors
        ]
        # Set up the source array for propagating discharges downstream
        source_array = np.zeros(accumulator.arr.shape).flatten()
        source_array[source_nodes] = 1
        source_array = source_array.reshape(accumulator.arr.shape)
        # Propagate the discharges downstream and add the result to the WaterCompany object
        return accumulator.accumulate(source_array)

    def get_historical_downstream_impact_at(
        self, time: datetime.datetime, include_recent_discharges: bool = False
    ) -> np.ndarray:
        """
        Calculates the downstream impact of all monitors that were discharging (or, optionally, recently discharging)
        at the given time.

        Args:
            time: The time to check for discharges.
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A 2D numpy array of the domain area showing the number of active/recently active discharges at the given time.
        """
        # Create the list of the source monitors that were discharging/recently discharging at the specified time
        sources = self._get_sources_at(time, include_recent_discharges)
        # Return the impact array
        return self._calculate_downstream_impact(sources)

    def get_downstream_geojson(
        self, include_recent_discharges: bool = False
    ) -> MultiLineString:
        """
        Get a MultiLineString of the downstream points for all active discharges in BNG coordinates. Note that this
        specific function is largely retained for legacy purposes

        Args:
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A geojson MultiLineString of the downstream points for all active (or optionally recent) discharges.
        """
        # Calculate the downstream impact
        if include_recent_discharges:
            sources = self.recently_discharging_monitors
        else:
            sources = self.discharging_monitors
        downstream_impact = self._calculate_downstream_impact(source_monitors=sources)
        # Convert the downstream impact to a geojson
        return self._accumulator.get_channel_segments(downstream_impact, threshold=0.9)

    def _calculate_downstream_info(self, sources: List[Monitor]) -> FeatureCollection:
        """
        Calculate the downstream impact of a list of source monitors and return a GeoJSON FeatureCollection of the downstream points.
        Contains information on number of upstream sources, the list of CSOs and the number of CSOs per km2.

        Args:
            sources: A list of source monitors for which to calculate the downstream impact.

        Returns:
            A GeoJSON FeatureCollection of the downstream points for all active discharges.
        """
        # Calculate downstream impact
        impact = self._calculate_downstream_impact(source_monitors=sources)

        # Calculate the upstream area
        trsfm = self.accumulator.ds.GetGeoTransform()
        cell_area = (trsfm[1] * trsfm[5] * -1) / 1000000
        areas = np.ones(self.accumulator.arr.shape) * cell_area
        drainage_area = self.accumulator.accumulate(areas)

        # Calculate relative importance of each area
        impact_per_area = impact / drainage_area
        impact = impact.flatten()
        impact_per_area = impact_per_area.flatten()
        dstream_nodes = np.where(impact > 0)[0]

        # Create a dictionary of properties for each downstream node
        dstream_info = {
            node: {
                "number_upstream_CSOs": impact[node],
                "number_CSOs_per_km2": impact_per_area[node],
                "CSOs": [],
            }
            for node in dstream_nodes
        }

        # Add the sources for each impacted node to the dictionary of properties
        for monitor in sources:
            node = self.accumulator.coord_to_node(monitor.x_coord, monitor.y_coord)
            dstream, _ = self.accumulator.get_profile(node)
            for node in dstream:
                dstream_info[node]["CSOs"].append(monitor.site_name)

        # Create a list of coordinates and properties for each impacted node in the network
        coordinates = []
        properties = []
        for node in dstream_nodes:
            coord = self.accumulator.node_to_coord(node)
            coordinates.append(coord)
            properties.append(dstream_info[node])

        # Create a list of GeoJSON features from the coordinates and properties
        features = [
            Feature(geometry=Point(coord), properties=prop)
            for coord, prop in zip(coordinates, properties)
        ]
        # Create a GeoJSON feature collection from the list of features
        feature_collection = FeatureCollection(features)
        return feature_collection

    def _get_sources_at(
        self, time: datetime.datetime, include_recent_discharges: bool
    ) -> List[Monitor]:
        """
        Get the sources that were discharging (or, optionally, recently discharging) at the given time.

        Args:
            time: The time to check for discharges.
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A list of monitors that were discharging (or, optionally, recently discharging) at the given time.

        Raises:
            ValueError: If the target time is in the future.
        """

        if time > datetime.datetime.now():
            raise ValueError("The target time cannot be in the future.")
        sources = []
        if include_recent_discharges:
            for monitor in self.active_monitors.values():
                if monitor.recent_discharge_at(time):
                    sources.append(monitor)
        else:
            for monitor in self.active_monitors.values():
                event = monitor.event_at(time)
                if event is not None and event.event_type == "Discharging":
                    sources.append(monitor)
        return sources

    def get_downstream_info_geojson(
        self, include_recent_discharges=False
    ) -> FeatureCollection:
        """
        Get a GeoJSON feature collection of the downstream points for all CURRENT active discharges in BNG coordinates.
        Contains information on number of upstream sources, the list of CSOs and the number of CSOs per km2.

        Args:
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A GeoJSON FeatureCollection of the downstream points for all active discharges.
        """
        # Check that "include_recent_discharges" is a boolean
        if not isinstance(include_recent_discharges, bool):
            raise ValueError("include_recent_discharges must be a boolean")

        if include_recent_discharges:
            sources = self.recently_discharging_monitors
        else:
            sources = self.discharging_monitors
        return self._calculate_downstream_info(sources)

    def get_historical_downstream_info_geojson_at(
        self, time: datetime.datetime, include_recent_discharges=False
    ) -> FeatureCollection:
        """
        Get a GeoJSON feature collection of the downstream points for all active discharges in BNG coordinates at a given time in the past.
        Contains information on number of upstream sources, the list of CSOs and the number of CSOs per km2.

        Args:
            time: The time to check for discharges.
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A GeoJSON FeatureCollection of the downstream points for all active discharges at the given time.
        """
        # Check that "include_recent_discharges" is a boolean
        if not isinstance(include_recent_discharges, bool):
            raise ValueError("include_recent_discharges must be a boolean")
        sources = self._get_sources_at(time, include_recent_discharges)
        return self._calculate_downstream_info(sources)

    def _alerts_df_to_events_list(
        self, df: pd.DataFrame, monitor: Monitor
    ) -> List[Event]:
        """
        Takes a standard dataframe of "Alerts" (e.g., that which would be created by the ThamesWater API or the .update_alerts_table method)
        and converts it into a list of Events.
        """

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

    def get_monitor_timeseries(
        self, since: datetime.datetime
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Returns a pandas DataFrame containing timeseries of the number of CSOs that 1) were active, 2) were active in last
        48 hours, 3) online at a list of times every 15 minutes since the given datetime. This can be used to plot the
        number of active CSOs and monitors over time. NB that for "online" we conservatively assume that every monitor was
        _offline_ until we receive any positive event from it. This means that if a monitor is installed but recording
        'notdischarging' for a month until its first discharge event, it will be counted as offline for that month. Lacking
        any other information, this is the most conservative assumption we can make.

        Args:
            since: The datetime to start the timeseries from.

        Returns:
            A pandas DataFrame containing timeseries of the number of CSOs that 1) were active, 2) were active in last
            48 hours, 3) online at a list of times every 15 minutes since the given datetime.

        Raises:
            ValueError: If the history is not yet set. Run set_all_histories() first.
        """

        if self.history_timestamp is None:
            raise ValueError(
                "History may not yet be set. Try running set_all_histories() first."
            )

        times = []
        now = datetime.datetime.now()
        time = since
        while time < now:
            times.append(time)
            time += datetime.timedelta(minutes=15)

        active = np.zeros(len(times), dtype=int)
        recent = np.zeros(len(times), dtype=int)
        online = np.zeros(len(times), dtype=int)

        for monitor in self.active_monitors.values():
            print(f"Processing {monitor.site_name}")
            mon_online, mon_active, mon_recent = monitor._history_masks(times)
            active += mon_active.astype(int)
            recent += mon_recent.astype(int)
            online += mon_online.astype(int)

        return pd.DataFrame(
            {
                "datetime": times,
                "number_discharging": active,
                "number_recently_discharging": recent,
                "number_online": online,
            }
        )

    def plot_current_status(self) -> None:
        """
        Plot the current status of the Water Company network.
        """

        plt.figure(figsize=(11, 8))
        acc = self.accumulator
        geojson = self.get_downstream_geojson(include_recent_discharges=True)
        dx, dy = acc.ds.GetGeoTransform()[1], acc.ds.GetGeoTransform()[5]
        cell_area = dx * dy * -1
        upstream_area = acc.accumulate(weights=cell_area * np.ones(acc.arr.shape))

        # Plot the rivers
        plt.imshow(upstream_area, norm=LogNorm(), extent=acc.extent, cmap="Blues")
        # Add a hillshade
        plt.imshow(acc.arr, cmap="Greys_r", alpha=0.2, extent=acc.extent)
        for line in geojson.coordinates:
            x = [c[0] for c in line]
            y = [c[1] for c in line]
            plt.plot(x, y, color="brown", linewidth=2)

        # Plot the status of the monitors
        for monitor in self.active_monitors.values():
            if monitor.current_status == "Discharging":
                colour = "red"
                size = 100
            elif monitor.discharge_in_last_48h:
                colour = "orange"
                size = 50
            elif monitor.current_status == "Not Discharging":
                colour = "green"
                size = 10
            elif monitor.current_status == "Offline":
                colour = "grey"
                size = 25
            plt.scatter(
                monitor.x_coord,
                monitor.y_coord,
                color=colour,
                s=size,
                zorder=10,
                marker="x",
            )
        # Set the axis to be equal
        plt.axis("equal")
        plt.tight_layout()

        plt.xlabel("Easting (m)")
        plt.ylabel("Northing (m)")
        plt.title(self.name + ": " + self.timestamp.strftime("%Y-%m-%d %H:%M"))

    def history_to_discharge_df(self) -> pd.DataFrame:
        """
        Convert a water company's discharge history to a dataframe

        Returns:
            A dataframe of discharge events.

        Raises:
            ValueError: If the history is not yet set. Run set_all_histories() first.

        """
        if self.history_timestamp is None:
            raise ValueError(
                "History may not yet be set. Try running set_all_histories() first."
            )
        print("\033[36m" + f"Building output data-table" + "\033[0m")
        df = pd.DataFrame()
        for monitor in self.active_monitors.values():
            print("\033[36m" + f"\tProcessing {monitor.site_name}" + "\033[0m")
            for event in monitor.history:
                if event.event_type == "Discharging":
                    df = pd.concat([df, event._to_row()], ignore_index=True)

        df.sort_values(
            by="StartDateTime", inplace=True, ignore_index=True, ascending=False
        )
        return df

    def history_to_offline_df(self) -> pd.DataFrame:
        """
        Convert a water company's offline history to a dataframe

        Returns:
            A dataframe of discharge events.

        Raises:
            ValueError: If the history is not yet set. Run set_all_histories() first.

        """
        if self.history_timestamp is None:
            raise ValueError(
                "History may not yet be set. Try running set_all_histories() first."
            )
        print("\033[36m" + f"Building output data-table" + "\033[0m")
        df = pd.DataFrame()
        for monitor in self.active_monitors.values():
            print("\033[36m" + f"\tProcessing {monitor.site_name}" + "\033[0m")
            for event in monitor.history:
                if event.event_type == "Offline":
                    df = pd.concat([df, event._to_row()], ignore_index=True)

        df.sort_values(
            by="StartDateTime", inplace=True, ignore_index=True, ascending=False
        )
        return df

    def update_alerts_table(self, verbose: bool = False) -> None:
        """
        Function that automatically generates a table of alerts based on the current status of the monitors and changes in status,
        relative previously created alerts. This function is designed to be run at regular intervals to update the alerts table.
        The alerts file is stored in '[WaterCompanyName].alerts_table.csv' and contains "alerts" which indicate a change of status
        of a particular monitor. It is modelled on the ThamesWater historical API data.

        The logic of alert updates is: 1) Alerts are given for every change of status 2) All starts must be followed by a stop
        (i.e., (offline) start- > (offline) stop) 3) We assume the previous event continues until the new event. Thus, we 'cannot see'
        events that start and finish between update times. If the function is run for the first time, it will create a new alerts table.
        The function will then update the alerts table based on the current status of the monitors and changes in status, relative to
        previously created alerts. The alerts table can thus be used to create a history of alerts for the water company network, even
        if the data is not directly available from an API.

        Sometimes, events do not have a start-time associated with them (e.g., WelshWater for some offline events). As a result, so as to
        not exclude this information we set the start-time of the event to be when we first *observe* the event (i.e., when the current status API
        first detects that event). This is a conservative approach, but it ensures that we do not lose information and are not excluding, for instance
        offline events from the system.

        Note that "stop" events are generally 'inferred'. i.e., they are created by the function if a stop event is necessary to continue
        the logic that all starts must be followed by a stop. For instance, if we go from "Start" to "Offline Start" the function will add a
        "Stop" event 1 second before the "Offline Start" event.

        Args:
            verbose: Whether to print out the changes in status of the monitors. Defaults to False.
        """

        alerts_filename = self._alerts_table
        # If file doesn't exist initiate it and put in the first alerts
        if not os.path.exists(alerts_filename):
            if os.path.exists(self._alerts_table_update_list):
                # Delete it
                os.remove(self._alerts_table_update_list)
            print("Alerts table doesn't exist! \nCreating new alerts table...")
            alerts = pd.DataFrame()
            for monitor in self.active_monitors.values():
                row = _make_start_alert_row(monitor)
                alerts = pd.concat([alerts, row])

        # We have a file, so we need to update it
        else:
            # Load in current table of alerts
            alerts = pd.read_csv(alerts_filename)

            # Loop through all monitors operated by the water company
            for name, monitor in self.active_monitors.items():
                if name not in alerts["LocationName"].values:
                    # If the monitor is currently not in the alerts table, we add the current event to the alerts table.
                    # This might occur if a new monitor has been added to the network (or if the alerts table has been deleted)
                    print(
                        f"Monitor {name} (currently {monitor.current_event.event_type}) had no previous recorded events so adding to alerts table..."
                    )
                    row = _make_start_alert_row(monitor)
                    alerts = pd.concat([row, alerts])

                else:
                    # Get the last alert from that monitor
                    last_alert = alerts[alerts["LocationName"] == name].iloc[0]
                    last_time = last_alert["DateTime"]
                    # Get alert corresponding to current status of monitor
                    current_alert_row = _make_start_alert_row(monitor)
                    current_time = current_alert_row["DateTime"].values[0]
                    # The underlying logic of this sequence is:
                    # 1) Alerts are given for every change of status
                    # 2) All starts must be followed by a stop (i.e., (offline) start- > (offline) stop)
                    # 3) We assume the previous event continues until the new event.
                    # Thus, we 'cannot see' events that start and finish between update times.
                    if last_time != current_time:
                        # Current alert doesn't match existing alert, so status has changed
                        prev_alert = last_alert["AlertType"]
                        new_alert = current_alert_row["AlertType"].values[0]

                        if prev_alert == "Stop" and new_alert == "Start":
                            # If a spill has started we add the "Start" alert row to dataframe
                            alerts = pd.concat([current_alert_row, alerts])
                            (
                                print(f"Monitor '{name}' has started discharging!")
                                if verbose
                                else None
                            )

                        elif prev_alert == "Start" and new_alert == "Stop":
                            # If a spill has ended we add the "Stop" alert row to dataframe
                            alerts = pd.concat([current_alert_row, alerts])
                            (
                                print(f"Monitor '{name}' has stopped discharging!")
                                if verbose
                                else None
                            )

                        elif prev_alert == "Offline start" and new_alert == "Start":
                            # If offline period has ended and turned into a discharge...
                            # We add an offline stop for 1s before start event
                            # Check if last_time is after current_time using datetime objects
                            if pd.to_datetime(current_time) < pd.to_datetime(last_time):
                                # The time of the current event is before the last event! Probably means that
                                # the offline event ended but status has _reverted_ to original no discharge status
                                # So, we push the _reverted_ event to actually begin 1s after *now*.

                                print(
                                    f"Monitor '{name}' is assumed to have reverted to a discharge status after an offline period. Adjusting start time to 1s after now."
                                )
                                off_stop = _make_offline_stop_alert_row(
                                    monitor,
                                    monitor.water_company.timestamp
                                    - datetime.timedelta(seconds=1),
                                )
                                alerts = pd.concat([off_stop, alerts])
                                shifted_start = make_alert_row(
                                    monitor,
                                    "Start",
                                    monitor.water_company.timestamp,
                                    note="Start time shifted forwards to update time after reversion from an offline period.",
                                )
                                alerts = pd.concat([shifted_start, alerts])
                                (
                                    print(
                                        f"Monitor '{name}' has stopped being offline and started discharging! \nHowever, start-time predates last event. Adjusting start time to now to allow for continuous sequence of events.."
                                    )
                                    if verbose
                                    else None
                                )
                            else:
                                # Assume that the offline period has ended and the monitor has started discharging.
                                # Add an offline stop alert for 1s before the start of the current event
                                off_stop = _make_offline_stop_alert_row(
                                    monitor,
                                    monitor.current_event.start_time
                                    - datetime.timedelta(seconds=1),
                                )
                                alerts = pd.concat([off_stop, alerts])
                                alerts = pd.concat([current_alert_row, alerts])
                                (
                                    print(
                                        f"Monitor '{name}' has stopped being offline and started discharging!"
                                    )
                                    if verbose
                                    else None
                                )

                        elif prev_alert == "Offline start" and new_alert == "Stop":
                            # If offline period has ended and turned into no discharge...
                            # We add an offline stop alert for 1s before the start of the current event...
                            # ...but then do nothing!
                            if pd.to_datetime(current_time) < pd.to_datetime(last_time):
                                # The time of the current event is before the last event! Probably means that
                                # the offline event ended but status has _reverted_ to original no discharge status
                                # So, we push the _reverted_ event to actually begin 1s after *now*.

                                print(
                                    f"Monitor '{name}' is assumed to have reverted to a no discharge status after an offline period. Adjusting start time to 1s after now."
                                )
                                off_stop = _make_offline_stop_alert_row(
                                    monitor,
                                    monitor.water_company.timestamp
                                    - datetime.timedelta(seconds=1),
                                )
                                alerts = pd.concat([off_stop, alerts])
                                shifted_start = make_alert_row(
                                    monitor,
                                    "Stop",
                                    monitor.water_company.timestamp,
                                    note="Stop time shifted *forwards* to update time after assumed reversion to prior event from an offline period.",
                                )
                                alerts = pd.concat([shifted_start, alerts])
                                (
                                    print(
                                        f"Monitor '{name}' has stopped being offline (and is not discharging)! \nHowever, start-time predates last event. Adjusting start time to now to allow for continuous sequence of events.."
                                    )
                                    if verbose
                                    else None
                                )

                            else:
                                if monitor.current_event.start_time is None:
                                    # If the monitor didnt have a start time we just make it now
                                    start_time = datetime.datetime.now()
                                else:
                                    start_time = monitor.current_event.start_time
                                off_stop = _make_offline_stop_alert_row(
                                    monitor,
                                    start_time - datetime.timedelta(seconds=1),
                                )
                                alerts = pd.concat([off_stop, alerts])
                                (
                                    print(
                                        f"Monitor '{name}' has stopped being offline (and is not discharging)!"
                                    )
                                    if verbose
                                    else None
                                )

                        elif prev_alert == "Stop" and new_alert == "Offline start":
                            # If no discharge followed by offline, add offline start to alerts
                            alerts = pd.concat([current_alert_row, alerts])
                            (
                                print(f"Monitor '{name}' has gone offline!")
                                if verbose
                                else None
                            )

                        elif (
                            prev_alert == "Offline stop"
                            and new_alert == "Offline start"
                        ):
                            # Period of offline, followed by no discharge, then offline again. So, add offline start to alerts
                            alerts = pd.concat([current_alert_row, alerts])
                            (
                                print(f"Monitor '{name}' has gone offline!")
                                if verbose
                                else None
                            )

                        elif prev_alert == "Start" and new_alert == "Offline start":
                            # If discharge event followed by offline
                            # We need to add a "Stop" event before the Offline event starts
                            stop = _make_stop_alert_row(
                                monitor,
                                monitor.current_event.start_time
                                - datetime.timedelta(minutes=1),
                            )
                            alerts = pd.concat([stop, alerts])
                            alerts = pd.concat([current_alert_row, alerts])
                            (
                                print(
                                    f"Monitor '{name}' has gone offline in the middle of a discharge event!"
                                )
                                if verbose
                                else None
                            )

                        elif prev_alert == new_alert:
                            # There are two cases where this can occur:
                            # 1) An event has no start time associated with it and we are simply revisiting it (we can ignore this)
                            # [We suppress the warnings here as we know that the start time is None]
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                if monitor.current_event.start_time is None:
                                    # # Have commented out the print statement here as it was happening too frequently...
                                    # (
                                    #     print(
                                    #         f"Monitor {monitor.site_name} (still) has no start time associated with current event. Ignoring."
                                    #     )
                                    #     if verbose
                                    #     else None
                                    # )
                                    continue

                                # Check that the current_event.start_time is before the last event
                                if pd.to_datetime(current_time) < pd.to_datetime(
                                    last_time
                                ):
                                    # This probably means that the event previously *reverted* to a (no) discharge status
                                    # from an offline period and the current status has no "memory" of the offline period.
                                    continue
                                else:
                                    # 2) An event has started and finished between last history checks that we are missing
                                    (
                                        print(
                                            f"For monitor {monitor.site_name}, event type has not changed but time of event start has.\
                                            \nLikely, an event started and finished between last history checks that we are missing."
                                        )
                                        if verbose
                                        else None
                                    )
                                    # Access the row index of the desired entry
                                    row_index = alerts[
                                        alerts["LocationName"] == name
                                    ].index[0]
                                    # Modify the entry directly in the DataFrame
                                    alerts.at[row_index, "Note"] = (
                                        f"One or more offline or discharge events may have been missed between {last_time} and {current_time}"
                                    )
                                    continue
                        elif prev_alert == "Offline stop" and new_alert == "Stop":
                            # If offline period has ended but latest event is a stop event it suggests that a spill has started and stopped (missed!).
                            # So, we do nothing but just add a note to the alert to say that a spill may have been missed.
                            (
                                print(
                                    f"For monitor {monitor.site_name}, event type has changed from {prev_alert} to {new_alert} \
                                            which suggests that a spill may have been missed between {last_time} and {current_time}."
                                )
                                if verbose
                                else None
                            )
                            row_index = alerts[alerts["LocationName"] == name].index[0]
                            # Modify the entry directly in the DataFrame
                            alerts.at[row_index, "Note"] = (
                                f"One or more discharge events may have been missed between {last_time} and {current_time}"
                            )
                        else:
                            raise RuntimeError(
                                f"For monitor {monitor.site_name}, event type has changed from {prev_alert} to {new_alert} but no corresponding action has been implemented."
                            )

        # Sort output from oldest bottom to newest top
        alerts.sort_values(by="DateTime", inplace=True, ascending=False)
        # Reset index to ensure it is in order
        alerts.reset_index(drop=True, inplace=True)
        # Overwrite the previous alerts table
        alerts.to_csv(
            alerts_filename,
            index=False,
        )
        # Add the update time to the update list
        with open(self._alerts_table_update_list, "a") as f:
            f.write(f"{self.timestamp}\n")
        print(
            "Alerts table updated successfully at",
            self.timestamp.strftime("%Y-%m-%d %H:%M"),
        )


def make_alert_row(
    monitor: Monitor, alert_type: str, datetime_obj: datetime.datetime, note: str = ""
) -> pd.DataFrame:
    """
    Creates an alert row for a given monitor and alert type.

    Args:
        monitor (Monitor): The monitor object to which the alert belongs.
        alert_type (str): The type of alert (e.g., "Start", "Stop", "Offline start", "Offline stop").
        datetime_obj (datetime.datetime): The datetime object representing the time of the alert.

    Returns:
        pd.DataFrame: A DataFrame containing the alert row.
    """

    # Check that the alert type is valid
    if alert_type not in ["Start", "Stop", "Offline start", "Offline stop"]:
        raise ValueError("Invalid alert type.")
    return pd.DataFrame(
        {
            "LocationName": monitor.site_name,
            "PermitNumber": monitor.permit_number,
            "DateTime": datetime_obj.strftime("%Y-%m-%dT%H:%M:%S"),
            "AlertType": alert_type,
            "X": monitor.x_coord,
            "Y": monitor.y_coord,
            "ReceivingWaterCourse": monitor.receiving_watercourse,
            "AlertCreated": monitor.water_company.timestamp.strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "Note": note,
        },
        index=[0],
    )


def _make_start_alert_row(monitor: Monitor):
    """
    Takes an Event object and returns a row which corresponds to an alert signalling the start of the current event of the monitor.
    """
    event = monitor.current_event
    # Determine the alert type based on the event type
    if event.event_type == "Not Discharging":
        alert_type = "Stop"
    elif event.event_type == "Discharging":
        alert_type = "Start"
    elif event.event_type == "Offline":
        alert_type = "Offline start"
    else:
        raise ValueError("Event type not recognised.")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if event.start_time is None:
            # If the event has no start time, we use the timestamp of the watercompany.
            # Implicitly it says that the event has started at the time of checking but we cannot be sure exactly when.
            datetime = monitor.water_company.timestamp
            # Add a note to say that the event had no start time associated with it
            note = "Event had no start time. Alert time set to time of last update."
            return make_alert_row(monitor, alert_type, datetime, note)
        else:
            datetime = event.start_time
            return make_alert_row(monitor, alert_type, datetime)


def _make_offline_stop_alert_row(monitor: Monitor, endtime: datetime.datetime):
    """
    Makes an alert row corresponding to the end of an offline event.
    """
    return make_alert_row(monitor, "Offline stop", endtime, note="Imputed")


def _make_stop_alert_row(monitor: Monitor, endtime: datetime.datetime):
    """
    Makes an alert row corresponding to the end of a discharging event.
    """
    return make_alert_row(monitor, "Stop", endtime, note="Imputed")


def round_time_down_15(time: datetime.datetime) -> datetime.datetime:
    """
    Rounds a datetime down to the nearest 15 minutes.
    """
    minutes = time.minute
    if minutes < 15:
        minutes = 0
    elif minutes < 30:
        minutes = 15
    elif minutes < 45:
        minutes = 30
    else:
        minutes = 45
    return datetime.datetime(time.year, time.month, time.day, time.hour, minutes, 0, 0)


def round_time_up_15(time: datetime.datetime) -> datetime.datetime:
    """
    Rounds a datetime up to the nearest 15 minutes.
    """
    minutes = time.minute
    if minutes < 15:
        minutes = 15
    elif minutes < 30:
        minutes = 30
    elif minutes < 45:
        minutes = 45
    else:
        minutes = 0
        time += datetime.timedelta(hours=1)
    return datetime.datetime(time.year, time.month, time.day, time.hour, minutes, 0, 0)
