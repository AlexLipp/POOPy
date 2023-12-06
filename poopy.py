from abc import ABC, abstractmethod
import datetime
from typing import Union, Optional


class Event(ABC):
    """A class to represent an event at a CSO monitor."""

    @abstractmethod
    def __init__(
        self,
        site_name: str,
        permit_number: str,
        x_coord: float,
        y_coord: float,
        receiving_watercourse: str,
        ongoing: bool,
        start_time: datetime.datetime,
        end_time: Optional[datetime.datetime] = None,
        event_type: Optional[str] = "Unknown",
    ) -> None:
        """
        Initialize attributes to describe an event.

        Args:
            site_name: The name of the site.
            permit_number: The permit number of the site.
            x_coord: The OSGB X coordinate of the site.
            y_coord: The OSGB Y coordinate of the site.
            receiving_watercourse: The watercourse receiving any discharge.
            ongoing: Whether the event is ongoing.
            start_time: The start time of the event.
            end_time: The end time of the event. Defaults to None.
            event_type: The type of event. Defaults to "Unknown".
        """
        self._site_name = site_name
        self._permit_number = permit_number
        self._x_coord = x_coord
        self._y_coord = y_coord
        self._receiving_watercourse = receiving_watercourse
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

    # Define getters for all attributes such that they are immutable (no setters)
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

    def summary(self) -> None:
        print(
            f"""
        --- Event Summary ---
        Event Type: {self.event_type}
        Site Name: {self.site_name}
        Permit Number: {self.permit_number}
        OSGB Coordinates: ({self.x_coord}, {self.y_coord})
        Receiving Watercourse: {self.receiving_watercourse}
        Start Time: {self.start_time}
        End Time: {self.end_time if self.end_time else 'Ongoing'}
        Duration: {round(self.duration)} minutes
        """
        )


class Discharge(Event):
    """A class to represent a discharge event at a CSO."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._event_type = "Discharge"


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
        self._event_type = "NoDischarge"


class WCCurrent:
    """A class to represent current status of all monitors managed by a water company."""

    # Has an immutable string for the WaterCompany name
    # Contains an immutable list of Discharge, Offline and NoDischarge events
    # An immutable time-stamp for when it was last updated
    # Has an 'update' method that changes the time-stamp and updates the list of events
    # Does checks to ensure that all events are Ongoing, and that there is only one Event per site.
    # Has a 'calculate' downstream impact method that reads in DEM and identifes downstream impact.

class ThamesWaterCurrent(WCCurrent):
    """Subclass of WCCurrent for Thames Water."""
    # Overrides the update method and __init__ method to get data from the Thames Water API

class Monitor:
    """A class that represents the history of a single CSO monitor."""
    # Has a name, permit number, x and y coordinates, and receiving watercourse etc. 
    # Has a list of Discharge, Offline and NoDischarge events
    # Has a single time-series of status (Discharge, Offline, NoDischarge) over time.
    # Contains total time of discharge, total time of offline, total time of no discharge.
    # Has a method to plot the time-series of status over time.
    # Has current status which points to the most recent event.