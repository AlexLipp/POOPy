from abc import ABC, abstractmethod
import datetime
import warnings
import pickle
from typing import Union, Optional, List, Dict, Tuple
from geojson import FeatureCollection
import numpy as np
from landlab import RasterModelGrid
from landlab.components.flow_accum.flow_accum_bw import find_drainage_area_and_discharge
from poopy.aux import (
    geographic_coords_to_model_xy,
    model_xy_to_geographic_coords,
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
        current_event: The current event at the monitor.
        history: The history of events at the monitor.

    Methods:
        print_status: Print the current status of the monitor.
    """

    def __init__(
        self,
        site_name: str,
        permit_number: str,
        x_coord: float,
        y_coord: float,
        receiving_watercourse: str,
        water_company: "WaterCompany",
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
        """
        self._site_name: str = site_name
        self._permit_number: str = permit_number
        self._x_coord: float = x_coord
        self._y_coord: float = y_coord
        self._receiving_watercourse: str = receiving_watercourse
        self._water_company: WaterCompany = water_company
        self._current_event: Event = None
        self._history: List[Event]

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
        if self._current_event is None:
            raise ValueError("Current event is not set.")
        else:
            return self._current_event.event_type

    @property
    def current_event(self) -> "Event":
        """Return the current event of the monitor."""
        return self._current_event

    @current_event.setter
    def current_event(self, event: "Event") -> None:
        """Set the current event of the monitor."""
        if not event.ongoing:
            raise ValueError("Current Event must be ongoing.")
        else:
            self._current_event = event

    def print_status(self) -> None:
        """Print the current status of the monitor."""
        if self._current_event is None:
            raise ValueError("Current event is not set.")
        else:
            self._current_event.print_status()

    # TODO Add a function to get the history of events
    # TODO Add a plot history function
    # TODO Add a 'cumulative discharge' function that sums the duration of all discharge events with optional "since" argument
    # TODO Add a 'total offline' function that sums the duration of all offline events with optional "since" argument
    # TODO Add a 'total not discharging' function that sums the duration of all not discharging events with optional "since" argument
    # TODO Add a _validate_history function that checks that the history is valid (i.e. no overlapping events, no ongoing events, etc.)
    # TODO Add a 'last_event' function that returns the last discharge event in the history


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
            print_status: Print a summary of the event.
        """
        self._monitor = monitor
        self._start_time = start_time
        self._ongoing = ongoing
        self._end_time = end_time
        self._duration = self.duration
        self._event_type = event_type
        self._validate()

    def _validate(self):
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
            warnings.warn("Event is ongoing and has no end time. Returning None.")
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
    def ongoing(self, value: bool):
        """Set the ongoing status of the event."""
        if value:
            raise ValueError("Ongoing status can only be set to False.")
        # Check if the discharge event is already not ongoing
        if not self._ongoing:
            raise ValueError("Event is already not ongoing.")
        else:
            self._ongoing = value
            self._end_time = datetime.datetime.now()
            self._duration = self.duration

    def print_status(self) -> None:
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
        name: The name of the Water Company network.
        timestamp: The timestamp of the last update.
        clientID: The client ID for the Water Company API.
        clientSecret: The client secret for the Water Company API.
        active_monitors: A dictionary of active monitors accessed by site name.
        active_monitor_names: A list of the names of active monitors.
        model_grid_file_path: The file path to the model grid that the monitors are located on for routing flow.
        model_grid: The model grid that the monitors are located on for routing flow.

    Methods:
        update: Updates the active_monitors list and the timestamp.
        calculate_downstream_points: Calculate the downstream points for all active discharges. Returns the downstream x and y coordinates and the number of upstream discharges at each point.
        save_downstream_geojson: Save a geojson (WGS84) of the downstream points for all active discharges. Optionally specify a filename.
    """

    def __init__(self, clientID: str, clientSecret: str):
        """
        Initialize attributes to describe a Water Company network.

        Args:
            name: The name of the Water Company network.
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
    def model_grid(self) -> RasterModelGrid:
        """Return the model grid that the monitors are located on for routing flow."""
        if self._model_grid is None:
            print("Loading model grid from memory...")
            print("...can take a bit of time...")
            with open(self._model_grid_file_path, "rb") as handle:
                self._model_grid = pickle.load(handle)
        return self._model_grid

    # define an update function that updates the active_monitors list and the timestamp
    def update(self):
        """
        Update the active_monitors list and the timestamp.
        """
        self._active_monitors = self._fetch_active_monitors()
        self._timestamp = datetime.datetime.now()

    def calculate_downstream_points(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Calculate the downstream points for all active discharges. Returns the downstream x and y coordinates and the number of upstream discharges at each point.
        Also adds a field to the model grid called 'number_upstream_discharges' that contains the number of upstream discharges at each node.
        """

        # Extract all the xy coordinates of active discharges
        grid = self.model_grid
        # Coords of all active discharges in OSGB
        discharge_locs = [
            (discharge.x_coord, discharge.y_coord)
            for discharge in self.discharging_monitors
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
        grid.add_field("number_upstream_discharges", number_upstream_sources)

        # Find the downstream nodes of the discharges
        dstr_polluted_nodes = np.where(number_upstream_sources != 0)[0]
        # Number of upstream nodes at sites
        n_upstream = number_upstream_sources[dstr_polluted_nodes]
        dstr_polluted_gridy, dstr_polluted_gridx = np.unravel_index(
            dstr_polluted_nodes, grid.shape
        )
        downstream_x, downstream_y = model_xy_to_geographic_coords(
            (dstr_polluted_gridx, dstr_polluted_gridy), grid
        )

        return downstream_x, downstream_y, n_upstream

    def _get_downstream_geojson(self) -> FeatureCollection:
        """
        Get a geojson of the downstream points for all active discharges.
        """
        # Raise a warning if number_upstream_discharges is not a field in the model grid
        # and run the calculate_downstream_points function
        if "number_upstream_discharges" not in self.model_grid.at_node:
            warnings.warn(
                "number_upstream_discharges is not a field in the model grid. Calculating downstream points..."
            )
            _, _, _ = self.calculate_downstream_points()

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
        Save a geojson of the downstream points for all active discharges. Optionally specify a filename.
        """
        # File path concatantes the name of the water company with the timestamp of the last update
        if filename is None:
            file_path = (
                f"{self.name}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.geojson"
            )
        else:
            file_path = filename
        save_json(self._get_downstream_geojson(), file_path)
