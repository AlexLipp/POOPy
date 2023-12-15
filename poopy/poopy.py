from abc import ABC, abstractmethod
import datetime
import pickle
import warnings
from typing import Dict, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pooch
from geojson import FeatureCollection
from landlab import RasterModelGrid
from landlab.components.flow_accum.flow_accum_bw import find_drainage_area_and_discharge

from poopy.aux import (
    geographic_coords_to_model_xy,
    profiler_data_struct_to_geojson,
    save_json,
)
from poopy.profiler import ChannelProfiler


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

    def get_history(self) -> None:
        """
        Get the historical data for the monitor and store it in the history attribute.
        """
        self._history = self.water_company._get_monitor_history(self)

    @property
    def history(self) -> List["Event"]:
        """Return a list of all past events at the monitor.

        Raises:
            ValueError: If the history is not yet set. Run get_history() first
        """
        if self._history is None:
            raise ValueError("History is not yet set. Run get_history() first.")
        return self._history

    @property
    def discharge_in_last_48h(self) -> bool:
        # Raise a warning if the discharge_in_last_48h is not set
        if self._discharge_in_last_48h is None:
            warnings.warn("discharge_in_last_48h is not set. Returning None.")
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
        """

        events = self.history
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
        self._duration = self.duration
        self._event_type = event_type
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
        if not self.ongoing:
            return (self._end_time - self._start_time).total_seconds() / 60
        else:
            return (datetime.datetime.now() - self._start_time).total_seconds() / 60

    @property
    def ongoing(self) -> bool:
        """Return if the event is ongoing."""
        return self._ongoing

    @property
    def start_time(self) -> datetime.datetime:
        """Return the start time of the event."""
        return self._start_time

    @property
    def end_time(self) -> Union[datetime.datetime, None]:
        """Return the end time of the event."""
        # If the event is Ongoing raise a Warning that the event is ongoing and has no end time but allow program to continue
        if self._ongoing:
            warnings.warn(
                "\033[91m"
                + "!WARNING! Event is ongoing and has no end time. Returning None."
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


class Discharge(Event):
    """A class to represent a discharge event at a CSO."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Discharging"

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
                "OngoingDischarge": self.ongoing,
            },
            index=[0],
        )
        return row


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
        active_monitors: A dictionary of active monitors accessed by site name.
        active_monitor_names: A list of the names of active monitors.
        model_grid: The model grid that the monitors are located on for routing flow.

    Methods:
        update: Updates the active_monitors list and the timestamp.
        set_all_histories: Sets the historical data for all active monitors and store it in the history attribute of each monitor.
        history_to_discharge_df: Convert a water company's total discharge history to a dataframe
        save_history_json: Save a water company's discharge history to a JSON file
        save_history_csv: Save a water company's discharge history to a csv file
        get_downstream_geojson: Get a geojson of the downstream points for all active discharges in BNG coordinates.
        save_downstream_geojson: Save a geojson (WGS84) of the downstream points for all active discharges. Optionally specify a filename.
    """

    def __init__(self, clientID: str, clientSecret: str):
        """
        Initialize attributes to describe a Water Company network.

        Args:
            clientID: The client ID for the Water Company API.
            clientSecret: The client secret for the Water Company API.
        """
        self._name: str = None
        self._clientID = clientID
        self._clientSecret = clientSecret
        self._active_monitors: Dict[str, Monitor] = self._fetch_active_monitors()
        self._timestamp: datetime.datetime = datetime.datetime.now()
        self._model_grid_file_path: str = None
        self._model_grid: RasterModelGrid = None

    @abstractmethod
    def _fetch_active_monitors(self) -> Dict[str, Monitor]:
        """
        Get the current status of the monitors by calling the API.

        Returns:
            A dictionary of active monitors accessed by site name.
        """
        pass

    @abstractmethod
    def _get_monitor_history(self, monitor: Monitor) -> List[Event]:
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

    def _fetch_model_grid_file(self, url: str, known_hash: str) -> str:
        """
        Get the path to the model grid file. If the file is not present, it will download it from the given url and unzip it.
        This is all handled by the pooch package. The hash of the file is checked against the known hash to ensure the file is not corrupted.
        If the file is already present in the pooch cache, it will not be downloaded again.
        """
        return pooch.retrieve(
            # URL to one of Pooch's test files
            url=url,
            known_hash=known_hash,
            processor=pooch.Unzip(),
        )[0]

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
    def model_grid(self) -> RasterModelGrid:
        """Return the model grid that the monitors are located on for routing flow."""
        if self._model_grid is None:
            print("Loading model grid from memory...")
            print("...can take a bit of time...")
            with open(self._model_grid_file_path, "rb") as handle:
                self._model_grid = pickle.load(handle)
        return self._model_grid

    def update(self):
        """
        Update the active_monitors list and the timestamp.
        """
        self._active_monitors = self._fetch_active_monitors()
        self._timestamp = datetime.datetime.now()

    def _calculate_downstream_points(
        self, include_recent_discharges: bool = False
    ) -> None:
        """
        Calculate the downstream points for all active discharges. Adds a field to the model grid called 'number_upstream_discharges'
        that contains the number of upstream discharges at each node. The optional argument include_recent_discharges allows you to
        include discharges that have occurred in the last 48 hours.

        Args:
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.
        """

        # Extract all the xy coordinates of active discharges
        grid = self.model_grid
        # Coords of all active discharges in OSGB
        if not include_recent_discharges:
            discharge_locs = [
                (discharge.x_coord, discharge.y_coord)
                for discharge in self.discharging_monitors
            ]
        else:
            discharge_locs = [
                (discharge.x_coord, discharge.y_coord)
                for discharge in self.recently_discharging_monitors
            ]
        # Convert to model grid coordinates
        locs_model = [
            geographic_coords_to_model_xy(loc, grid) for loc in discharge_locs
        ]

        # Get the model grid node ID for each discharge
        nodes = [
            np.ravel_multi_index((y.astype(int), x.astype(int)), grid.shape)
            for x, y in locs_model
        ]

        # Set up the source array for propagating discharges downstream
        source_array = np.zeros(grid.shape).flatten()
        source_array[nodes] = 1

        # Propagate the discharges downstream
        _, number_upstream_sources = find_drainage_area_and_discharge(
            grid.at_node["flow__upstream_node_order"],
            r=grid.at_node["flow__receiver_node"],
            runoff=source_array,
        )
        grid.add_field(
            "number_upstream_discharges", number_upstream_sources, clobber=True
        )

    def get_downstream_geojson(
        self, include_recent_discharges: bool = False
    ) -> FeatureCollection:
        """
        Get a geojson of the downstream points for all active discharges in BNG coordinates.

        Args:
            include_recent_discharges: Whether to include discharges that have occurred in the last 48 hours. Defaults to False.

        Returns:
            A geojson FeatureCollection of the downstream points for all active discharges.
        """
        self._calculate_downstream_points(include_recent_discharges)
        print("Building downstream geojson...")
        print("...can take a bit of time...")
        cp = ChannelProfiler(
            self.model_grid,
            "number_upstream_discharges",
            minimum_channel_threshold=0.9,
            minimum_outlet_threshold=0.9,
        )
        cp.run_one_step()
        out_geojson = profiler_data_struct_to_geojson(
            cp.data_structure, self.model_grid, "number_upstream_discharges"
        )
        return out_geojson

    def save_downstream_geojson(self, filename: str = None) -> None:
        """
        Gets the geojson of the downstream points for all active discharges and saves them to file. Optionally specify a filename.
        """
        # File path concatantes the name of the water company with the timestamp of the last update
        if filename is None:
            file_path = (
                f"{self.name}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.geojson"
            )
        else:
            file_path = filename
        save_json(self.get_downstream_geojson(), file_path)

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

    def save_history_json(self, filename: str = None) -> None:
        """
        Save a water company's discharge history to a JSON file

        Args:
            filename: The filename to save the history to. Defaults to the timestamp of the last update.
        """
        df = self.history_to_discharge_df()
        if filename is None:
            file_path = (
                f"{self.name}_{self.history_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            )
        else:
            file_path = filename
        print(f"Saving history to \033[92m{file_path}\033[0m")
        df.to_json(file_path)

    def save_history_csv(self, filename: str = None) -> None:
        """
        Save a water company's discharge history to a csv file

        Args:
            filename: The filename to save the history to. Defaults to the timestamp of the last update.
        """
        df = self.history_to_discharge_df()
        if filename is None:
            file_path = (
                f"{self.name}_{self.history_timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
            )
        else:
            file_path = filename
        print(f"Saving history to \033[92m{file_path}\033[0m")
        df.to_csv(file_path, index=False, header=True)
