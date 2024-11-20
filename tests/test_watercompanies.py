import datetime
import os
import warnings

from numpy import isnan

from poopy.companies import ThamesWater, WelshWater, SouthernWater
from poopy.poopy import Monitor, WaterCompany, Event

# Retrieve Thames Water API credentials from environment variables
TW_CLIENTID = os.getenv("TW_CLIENT_ID")
TW_CLIENTSECRET = os.getenv("TW_CLIENT_SECRET")

if TW_CLIENTID is None or TW_CLIENTSECRET is None:
    raise ValueError(
        "Thames Water API keys are missing from the environment!\n Please set them and try again."
    )


def check_current_event_init(current: Event, monitor: Monitor):
    """
    Tests the initialization of the (current) event attribute w.r.t the Monitor object it belongs to

    Args:
        current: Event object
        monitor: The monitor object to which the event belongs.
    """
    # assert that the event belongs to the monitor
    assert current.monitor == monitor
    # assert that the event is ongoing (as current event is always ongoing)
    assert current.ongoing
    # assert that current.end_time is not None (as the current event is always ongoing)
    # ignore the advisory warning that is raised when the end_time is None for ongoing events (as it should be)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert current.end_time is None

    # assert that current.event_type is either "Discharging", "Offline", or "Not Discharging"
    assert current.event_type in ["Discharging", "Offline", "Not Discharging"]

    if current.start_time is not None:
        # Check that the start time is in the past
        assert current.start_time < datetime.datetime.now()
        # Assert that the duration is close to the current time minus the start time
        expct_duration = datetime.datetime.now() - current.start_time
        assert abs(current.duration - expct_duration.total_seconds() / 60) < 5
    else:
        # Check that the duration is NaN if the start time is None
        assert isnan(current.duration)


def check_monitor(monitor: Monitor, wc: WaterCompany):
    """
    Tests the initialization of a Monitor object w.r.t the WaterCompany object it belongs to

    Args:
        monitor: The monitor object to be tested.
        wc: The water company object to which the monitor belongs.

    """

    # assert that the monitor belongs to the water company
    assert monitor.water_company == wc
    # assert that monitor is within the extent of the accumulator domain
    assert wc.accumulator.extent[0] <= monitor.x_coord <= wc.accumulator.extent[1]
    assert wc.accumulator.extent[2] <= monitor.y_coord <= wc.accumulator.extent[3]

    # assert that the monitor has the correct attributes
    assert type(monitor.site_name) == str
    assert type(monitor.receiving_watercourse) == str
    assert type(monitor.permit_number) == str
    # assert that the monitor has a valid status
    assert monitor.current_status in ["Discharging", "Not Discharging", "Offline"]

    # Extract the current event and test it
    current = monitor.current_event
    check_current_event_init(current, monitor)


def check_watercompany(wc: WaterCompany):
    """
    Tests the initialization of a WaterCompany object

    Args:
        wc: The water company object to be tested.
    """
    # Check that the D8 file is created correctly
    assert os.path.exists(wc._d8_file_path)
    assert os.path.isfile(wc._d8_file_path)

    # Assert that the timestamp is within 1 minute of the current time (allowing for some slowness in the test)
    assert (datetime.datetime.now() - wc._timestamp) < datetime.timedelta(minutes=1)
    # Assert that the set of active monitor names is the same as the set of active monitors
    assert set(wc.active_monitor_names) == set(wc.active_monitors.keys())

    for monitor in wc.active_monitors.values():
        # Assert that every monitor object passes generic checks
        check_monitor(monitor, wc)

    # Check that the discharging monitors subset is correctly being created
    for monitor in wc.discharging_monitors:
        assert monitor.site_name in wc.active_monitor_names
        # Check that the monitor is discharging
        assert monitor.current_status == "Discharging"
        # Check that the monitor has a discharging event in the last 48 hours
        assert monitor.discharge_in_last_48h == True

    # Check that the list of recently discharging monitors is correctly being created
    for monitor in wc.recently_discharging_monitors:
        assert monitor.site_name in wc.active_monitor_names
        # Check that each monitor has recorded a discharge in the last 48 hours
        assert monitor.discharge_in_last_48h == True


def test_thames_water_init():
    """
    Test the basic initialization of a ThamesWater object
    """
    tw = ThamesWater(TW_CLIENTID, TW_CLIENTSECRET)
    assert tw.name == "ThamesWater"
    assert tw.clientID == TW_CLIENTID
    assert tw.clientSecret == TW_CLIENTSECRET

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert tw.accumulator.extent == [319975.0, 620025.0, 79975.0, 280025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(tw)


def test_southern_water_init():
    """
    Test the basic initialization of a SouthernWater object
    """

    sw = SouthernWater()
    assert sw.name == "SouthernWater"
    assert sw.clientID == ""
    assert sw.clientSecret == ""

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert sw.accumulator.extent == [409975.0, 659975.0, 70025.0, 190025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(sw)


def test_welsh_water_init():
    """
    Test the basic initialization of a WelshWater object
    """

    ww = WelshWater()
    assert ww.name == "WelshWater"
    assert ww.clientID == ""
    assert ww.clientSecret == ""

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert ww.accumulator.extent == [159975.0, 499975.0, 160025.0, 400025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(ww)
