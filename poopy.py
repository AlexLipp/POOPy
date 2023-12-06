from abc import ABC, abstractmethod
import datetime
from typing import Union, Optional, List


class Monitor:
    """A class to represent a CSO monitor.
    
    Attributes:
        site_name: The name of the site.
        permit_number: The permit number of the monitor.
        x_coord: The X coordinate of the site.
        y_coord: The Y coordinate of the site.
        receiving_watercourse: The receiving watercourse of the site.
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
    ) -> None:
        # Add docstring for init in Google style
        """
        Initialize attributes to describe a CSO monitor.

        Args:
            site_name: The name of the site.
            permit_number: The permit number of the monitor.
            x_coord: The X coordinate of the site.
            y_coord: The Y coordinate of the site.
            receiving_watercourse: The receiving watercourse of the site.     
        """
        self._site_name: str = site_name
        self._permit_number: str = permit_number
        self._x_coord: float = x_coord
        self._y_coord: float = y_coord
        self._receiving_watercourse: str = receiving_watercourse
        self._history: List[Event]
        self._current_event: Event

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
            self._current_event.summary()


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
            print_summary: Print a summary of the event.
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
        return self._end_time

    @property
    def event_type(self) -> str:
        """Return the type of event."""
        return self._event_type

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

    def print_summary(self) -> None:
        print(
            f"""
        --------------------------------------
        Event Type: {self.event_type}
        Site Name: {self.monitor.site_name}
        Permit Number: {self.monitor.permit_number}
        OSGB Coordinates: ({self.monitor.x_coord}, {self.monitor.y_coord})
        Receiving Watercourse: {self.monitor.receiving_watercourse}
        Start Time: {self.start_time}
        End Time: {self.end_time if not self.ongoing else "Ongoing"}
        Duration: {round(self.duration)} minutes
        """
        )


class Discharge(Event):
    """A class to represent a discharge event at a CSO."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Discharging"


class Offline(Event):
    """A class to represent a CSO monitor being offline."""

    # TODO - add a "probable status" that is inferred from the last known status.

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Offline"


class NoDischarge(Event):
    """A class to represent a CSO not discharging."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Not Discharging"
