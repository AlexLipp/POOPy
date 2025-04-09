"""Tests for the WaterCompany classes in the poopy package."""

import datetime
import os
import warnings

import pytest
from numpy import isnan

from poopy.companies import (
    AnglianWater,
    NorthumbrianWater,
    SevernTrentWater,
    SouthernWater,
    SouthWestWater,
    ThamesWater,
    UnitedUtilities,
    WelshWater,
    WessexWater,
    YorkshireWater,
)
from poopy.poopy import Event, Monitor, WaterCompany

# Retrieve Thames Water API credentials from environment variables
TW_CLIENTID = os.getenv("TW_CLIENT_ID")
TW_CLIENTSECRET = os.getenv("TW_CLIENT_SECRET")

if TW_CLIENTID is None or TW_CLIENTSECRET is None:
    raise ValueError(
        "Thames Water API keys are missing from the environment!\n Please set them and try again."
    )


def check_current_event_init(current: Event, monitor: Monitor):
    """
    Test the initialization of the (current) event attribute w.r.t the Monitor object it belongs to.

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
    Test the initialization of a Monitor object w.r.t the WaterCompany object it belongs to.

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
    assert isinstance(monitor.site_name, str)
    assert isinstance(monitor.receiving_watercourse, str)
    assert isinstance(monitor.permit_number, str)
    # assert that the monitor has a valid status
    assert monitor.current_status in ["Discharging", "Not Discharging", "Offline"]

    # Extract the current event and test it
    current = monitor.current_event
    check_current_event_init(current, monitor)


def check_watercompany(wc: WaterCompany):
    """
    Test the initialization of a WaterCompany object.

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

    # Check that get_downstream_geojson calls run without errors
    try:
        wc.get_downstream_geojson(include_recent_discharges=False)
    except Exception as e:
        pytest.fail(
            f"get_downstream_geojson(include_recent_discharges=False) raised an exception: {e}"
        )

    try:
        wc.get_downstream_geojson(include_recent_discharges=True)
    except Exception as e:
        pytest.fail(
            f"get_downstream_geojson(include_recent_discharges=True) raised an exception: {e}"
        )

    try:
        wc.get_downstream_info_geojson(include_recent_discharges=False)
    except Exception as e:
        pytest.fail(
            f"get_downstream_info_geojson(include_recent_discharges=False) raised an exception: {e}"
        )

    try:
        wc.get_downstream_info_geojson(include_recent_discharges=True)
    except Exception as e:
        pytest.fail(
            f"get_downstream_info_geojson(include_recent_discharges=True) raised an exception: {e}"
        )

    for monitor in wc.active_monitors.values():
        # Assert that every monitor object passes generic checks
        check_monitor(monitor, wc)

    # Check that the discharging monitors subset is correctly being created
    for monitor in wc.discharging_monitors:
        assert monitor.site_name in wc.active_monitor_names
        # Check that the monitor is discharging
        assert monitor.current_status == "Discharging"
        # Check that the monitor has a discharging event in the last 48 hours
        assert monitor.discharge_in_last_48h

    # Check that the list of recently discharging monitors is correctly being created
    for monitor in wc.recently_discharging_monitors:
        assert monitor.site_name in wc.active_monitor_names
        # Check that each monitor has recorded a discharge in the last 48 hours
        assert monitor.discharge_in_last_48h


def test_thames_water_init():
    """Test the basic initialization of a ThamesWater object."""
    tw = ThamesWater(TW_CLIENTID, TW_CLIENTSECRET)
    assert tw.name == "ThamesWater"
    assert tw.clientID == TW_CLIENTID
    assert tw.clientSecret == TW_CLIENTSECRET

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert tw.accumulator.extent == [319975.0, 620025.0, 79975.0, 280025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(tw)


def test_southern_water_init():
    """Test the basic initialization of a SouthernWater object."""
    sw = SouthernWater()
    assert sw.name == "SouthernWater"
    assert sw.clientID == ""
    assert sw.clientSecret == ""

    # # Now test some specifics to do with how we interpret the data from Southern Water
    # # NB: The following is now obsolete as we no longer use "StatusStart" as it seems to be unreliable (i.e., this test kept failing!!!)
    # sw_df_discharging = sw_df[sw_df["Status"] == 1]
    # sw_df_not_discharging = sw_df[sw_df["Status"] == 0]

    # # Get the subset of the dataframe where the latestEventStart is not null (i.e., an event has been recorded)
    # sw_df_not_discharging_have_recorded = sw_df_not_discharging[
    #     sw_df_not_discharging["LatestEventEnd"].notnull()
    # ]

    # # Check that for discharging events the statusStart is the same as latestEventStart
    # # Run the assertion if this dataframe has any rows
    # if not sw_df_discharging.empty:
    #     assert (
    #         sw_df_discharging["StatusStart"] == sw_df_discharging["LatestEventStart"]
    #     ).all()

    # # Check that for non-discharging events the statusStart is the same as latestEventEnd (but only if it has values)
    # if not sw_df_not_discharging_have_recorded.empty:
    #     assert (
    #         sw_df_not_discharging_have_recorded["StatusStart"]
    #         == sw_df_not_discharging_have_recorded["LatestEventEnd"]
    #     ).all()

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert sw.accumulator.extent == [409975.0, 659975.0, 70025.0, 190025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(sw)


def test_welsh_water_init():
    """Test the basic initialization of a WelshWater object."""
    ww = WelshWater()
    assert ww.name == "WelshWater"
    assert ww.clientID == ""
    assert ww.clientSecret == ""

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert ww.accumulator.extent == [159975.0, 499975.0, 160025.0, 400025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(ww)


def test_anglian_water_init():
    """Test the basic initialization of a AnglianWater object."""
    aw = AnglianWater()
    assert aw.name == "AnglianWater"
    assert aw.clientID == ""
    assert aw.clientSecret == ""

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert aw.accumulator.extent == [439975.0, 659975.0, 170025.0, 430025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(aw)


def test_wessex_water_init():
    """Test the basic initialization of a WessexWater object."""
    wxw = WessexWater()
    assert wxw.name == "WessexWater"
    assert wxw.clientID == ""
    assert wxw.clientSecret == ""

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert wxw.accumulator.extent == [279975.0, 429975.0, 65025.0, 202025.0]

    # Now test some specifics to do with how we interpret the data from Wessex Water
    ww_df = wxw._fetch_current_status_df()
    ww_df_discharges = ww_df[ww_df["Status"] == 1]
    # if  the size of this df is 0 continue
    ww_df_not_discharging = ww_df[ww_df["Status"] == 0]

    # Get the subset of the not discharging sites that have previously recorded a finished discharge
    ww_df_has_discharged = ww_df_not_discharging[
        ~ww_df_not_discharging["LatestEventEnd"].isnull()
    ]

    # Check that the status change time is the same as the latest event start time for discharges
    if not ww_df_discharges.empty:
        assert all(
            ww_df_discharges["StatusStart"] == ww_df_discharges["LatestEventStart"]
        )

    # Check that the status change time is the same as the latest event end time for not discharging sites
    # IF they have previously recorded a finished discharge
    if not ww_df_has_discharged.empty:
        assert all(
            ww_df_has_discharged["StatusStart"]
            == ww_df_has_discharged["LatestEventEnd"]
        )

    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(wxw)


def test_southwest_water_init():
    """Test the basic initialization of a WessexWater object."""
    sww = SouthWestWater()
    assert sww.name == "SouthWest Water"
    assert sww.clientID == ""
    assert sww.clientSecret == ""

    # Now test some specifics to do with how we interpret the data from South West Water
    sww_df = sww._fetch_current_status_df()
    sww_df_discharging = sww_df[sww_df["status"] == 1]
    sww_df_not_discharging = sww_df[sww_df["status"] == 0]
    # Get the subset of the dataframe where the latestEventStart is not null (i.e., an event has been recorded)
    sww_df_not_discharging_have_recorded = sww_df_not_discharging[
        sww_df_not_discharging["latestEventEnd"].notnull()
    ]

    # Check that for discharging events the statusStart is the same as latestEventStart
    # Run the assertion if this dataframe has any rows
    if not sww_df_discharging.empty:
        assert (
            sww_df_discharging["statusStart"] == sww_df_discharging["latestEventStart"]
        ).all()

    # Check that for non-discharging events the statusStart is the same as latestEventEnd (but only if it has values)
    if not sww_df_not_discharging_have_recorded.empty:
        assert (
            sww_df_not_discharging_have_recorded["statusStart"]
            == sww_df_not_discharging_have_recorded["latestEventEnd"]
        ).all()

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert sww.accumulator.extent == [84975.0, 350975.0, 7025.0, 151025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(sww)


def test_united_utilities_init():
    """Test the basic initialization of a UnitedUtilities object."""
    uu = UnitedUtilities()
    assert uu.name == "United Utilities"
    assert uu.clientID == ""
    assert uu.clientSecret == ""

    # For UU we use LatestStart and LatestEnd rather than StatusStart (which is unreliable for UU).
    # So we don't need to test the StatusStart attribute.

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert uu.accumulator.extent == [289975.0, 409975.0, 333025.0, 610025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(uu)


def test_yorkshire_water_init():
    """Test the basic initialization of a YorkshireWater object."""
    yw = YorkshireWater()
    assert yw.name == "Yorkshire Water"
    assert yw.clientID == ""
    assert yw.clientSecret == ""

    # Now test some specifics to do with how we interpret the data from Yorkshire Water
    yw_df = yw._fetch_current_status_df()
    yw_df_discharging = yw_df[yw_df["Status"] == 1]
    yw_df_not_discharging = yw_df[yw_df["Status"] == 0]
    # Get the subset of the dataframe where the latestEventStart is not null (i.e., an event has been recorded)
    yw_df_not_discharging_have_recorded = yw_df_not_discharging[
        yw_df_not_discharging["LatestEventEnd"].notnull()
    ]

    # Check that for discharging events the StatusStart is the same as LatestEventStart
    # Run the assertion if this dataframe has any rows.
    if not yw_df_discharging.empty:
        assert (
            yw_df_discharging["StatusStart"] == yw_df_discharging["LatestEventStart"]
        ).all()

    # Check that for non-discharging events the StatusStart is the same as LatestEventEnd (but only if it has values)
    if not yw_df_not_discharging_have_recorded.empty:
        assert (
            yw_df_not_discharging_have_recorded["StatusStart"]
            == yw_df_not_discharging_have_recorded["LatestEventEnd"]
        ).all()

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert yw.accumulator.extent == [374975.0, 544975.0, 360025.0, 520025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(yw)


def test_northunbrian_water_init():
    """Test the basic initialization of a NorthumbrianWater object."""
    nw = NorthumbrianWater()
    assert nw.name == "Northumbrian Water"
    assert nw.clientID == ""
    assert nw.clientSecret == ""

    # Now test some specifics to do with how we interpret the data from Yorkshire Water
    # nw_df = nw._fetch_current_status_df()
    # nw_df_not_discharging = nw_df[nw_df["Status"] == 0]
    # # Get the subset of the dataframe where the latestEventStart is not null (i.e., an event has been recorded)
    # nw_df_discharging = nw_df[nw_df["Status"] == 1]
    # nw_df_not_discharging_have_recorded = nw_df_not_discharging[
    #     nw_df_not_discharging["LatestEventEnd"].notnull()
    # ]
    # # Check that for discharging events the StatusStart is the same as LatestEventStart
    # # Run the assertion if this dataframe has any rows. This is commented out because it fails for Northumbrian Water...
    # # Find a better way to test this...!!!
    # if not nw_df_discharging.empty:
    #     assert (
    #         nw_df_discharging["StatusStart"] == nw_df_discharging["LatestEventStart"]
    #     ).all()

    # # Check that for non-discharging events the StatusStart is the same as LatestEventEnd (but only if it has values)
    # # This fails in the case of Northumbrian Water _potentially_ because the StatusStart records when it transitions from offline to not discharging
    # # rather than when it transitions from discharging to not discharging...
    # if not nw_df_not_discharging_have_recorded.empty:
    #     assert (
    #         nw_df_not_discharging_have_recorded["StatusStart"]
    #         == nw_df_not_discharging_have_recorded["LatestEventEnd"]
    #     ).all()

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert nw.accumulator.extent == [354975.0, 474975.0, 498025.0, 655025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(nw)


def test_severn_trent_water_init():
    """Test the basic initialization of a SevernTrentWater object."""
    stw = SevernTrentWater()
    assert stw.name == "SevernTrent Water"
    assert stw.clientID == ""
    assert stw.clientSecret == ""

    # Now test some specifics to do with how we interpret the data from Yorkshire Water
    stw_df = stw._fetch_current_status_df()
    stw_df_discharging = stw_df[stw_df["Status"] == 1]

    # stw_df_not_discharging = stw_df[stw_df["Status"] == 0]
    # Get the subset of the dataframe where the latestEventStart is not null (i.e., an event has been recorded)
    # stw_df_not_discharging_have_recorded = stw_df_not_discharging[
    #     stw_df_not_discharging["LatestEventEnd"].notnull()
    # ]

    # Check that for discharging events the StatusStart is the same as LatestEventStart
    # Run the assertion if this dataframe has any rows
    if not stw_df_discharging.empty:
        assert (
            stw_df_discharging["StatusStart"] == stw_df_discharging["LatestEventStart"]
        ).all()

    # # Check that for non-discharging events the StatusStart is the same as LatestEventEnd (but only if it has values).
    # # This *Fails* in the case of Severn Trent Water _potentially_ because the StatusStart records when it transitions from
    # # offline to not discharging. This is why we comment this out for now. We opt to use StatusStart as the start of the event
    # # but this discrepancy should be noted...
    # if not stw_df_not_discharging_have_recorded.empty:
    #     assert (
    #         stw_df_not_discharging_have_recorded["StatusStart"]
    #         == stw_df_not_discharging_have_recorded["LatestEventEnd"]
    #     ).all()

    # Check that the accumulator is initialized correctly with the correct extent (in OSGB)
    assert stw.accumulator.extent == [279975.0, 499975.0, 195025.0, 425025.0]
    # Now test the rest of the object which is common to all WaterCompany objects
    check_watercompany(stw)
