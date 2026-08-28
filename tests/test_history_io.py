"""
Unit tests for the history table (de)serialisation and merge logic.

Unlike the rest of this suite these tests are entirely offline: they build small
synthetic history tables rather than calling any API, because the merge is the
step that can silently destroy years of accumulated records and so needs to be
exercised deterministically.
"""

import datetime
import json
import os
import tempfile

import pandas as pd
import pytest

from poopy.history_io import (
    EVENT_KEY,
    HISTORY_COLUMNS,
    merge_history_tables,
    read_history_json,
    straddling_event_mask,
    write_history_json,
)


def make_row(
    location="Site A",
    permit="TEMP.1",
    start="2026-01-01 00:00:00",
    stop="2026-01-01 01:00:00",
    ongoing=False,
    x=100,
    y=200,
    watercourse="River Test",
):
    """Build a single history row with sensible defaults."""
    start_dt = pd.to_datetime(start)
    stop_dt = pd.NaT if stop is None else pd.to_datetime(stop)
    duration = (
        float("nan") if pd.isna(stop_dt) else (stop_dt - start_dt).total_seconds() / 60
    )
    return {
        "LocationName": location,
        "PermitNumber": permit,
        "X": x,
        "Y": y,
        "ReceivingWaterCourse": watercourse,
        "StartDateTime": start_dt,
        "StopDateTime": stop_dt,
        "Duration": duration,
        "OngoingEvent": ongoing,
    }


def make_df(rows):
    """Build a history DataFrame from a list of row dicts."""
    return pd.DataFrame(rows, columns=HISTORY_COLUMNS)


# --------------------------------------------------------------------------
# Serialisation
# --------------------------------------------------------------------------


def test_write_read_roundtrip_preserves_values():
    """Writing then reading a history table returns the same values."""
    df = make_df(
        [
            make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00"),
            make_row(
                location="Site B",
                permit="TEMP.2",
                start="2026-02-01 06:00:00",
                stop="2026-02-01 09:30:00",
            ),
        ]
    )
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "history.json")
        write_history_json(df, path)
        result = read_history_json(path)

    pd.testing.assert_frame_equal(df, result)


def test_written_timestamps_are_epoch_milliseconds_not_strings():
    """
    Timestamps must serialise as integers.

    The website does `new Date(value)`, which is correct for an epoch-millisecond
    number but yields `Invalid Date` for the same value as a string.
    """
    df = make_df([make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00")])
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "history.json")
        write_history_json(df, path)
        with open(path) as f:
            raw = json.load(f)

    start = raw["StartDateTime"]["0"]
    stop = raw["StopDateTime"]["0"]
    assert isinstance(start, int), f"StartDateTime serialised as {type(start)}"
    assert isinstance(stop, int), f"StopDateTime serialised as {type(stop)}"
    # Epoch milliseconds, not seconds.
    assert start == 1767225600000


def test_written_json_uses_column_orientation():
    """The published contract is a dict of columns, not a list of records."""
    df = make_df([make_row(), make_row(location="Site B", permit="TEMP.2")])
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "history.json")
        write_history_json(df, path)
        with open(path) as f:
            raw = json.load(f)

    assert list(raw.keys()) == HISTORY_COLUMNS
    assert raw["LocationName"] == {"0": "Site A", "1": "Site B"}


def test_write_is_atomic_and_leaves_no_temp_file():
    """A successful write leaves only the destination file behind."""
    df = make_df([make_row()])
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "history.json")
        write_history_json(df, path)
        assert os.listdir(d) == ["history.json"]


def test_read_rejects_missing_columns():
    """A file lacking required columns is rejected rather than half-loaded."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "bad.json")
        with open(path, "w") as f:
            json.dump({"LocationName": {"0": "Site A"}}, f)
        with pytest.raises(ValueError, match="missing required column"):
            read_history_json(path)


def test_write_rejects_missing_columns():
    """Writing a frame without the full schema fails loudly."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "out.json")
        with pytest.raises(ValueError, match="missing column"):
            write_history_json(pd.DataFrame({"LocationName": ["Site A"]}), path)


def test_read_tolerates_iso_string_timestamps():
    """Hand-edited or third-party files using ISO strings still load."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "iso.json")
        payload = {
            "LocationName": {"0": "Site A"},
            "PermitNumber": {"0": "TEMP.1"},
            "X": {"0": 100},
            "Y": {"0": 200},
            "ReceivingWaterCourse": {"0": "River Test"},
            "StartDateTime": {"0": "2026-01-01T00:00:00"},
            "StopDateTime": {"0": "2026-01-01T01:00:00"},
            "Duration": {"0": 60.0},
            "OngoingEvent": {"0": False},
        }
        with open(path, "w") as f:
            json.dump(payload, f)
        df = read_history_json(path)

    assert df["StartDateTime"].iloc[0] == pd.Timestamp("2026-01-01 00:00:00")


# --------------------------------------------------------------------------
# Merging
# --------------------------------------------------------------------------

CUTOFF = datetime.datetime(2026, 6, 1)


def test_merge_appends_new_events():
    """Events newer than the master are added."""
    master = make_df(
        [make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00")]
    )
    new = make_df([make_row(start="2026-07-01 00:00:00", stop="2026-07-01 02:00:00")])

    merged = merge_history_tables(master, new, CUTOFF)

    assert len(merged) == 2
    assert merged["StartDateTime"].tolist() == [
        pd.Timestamp("2026-07-01 00:00:00"),
        pd.Timestamp("2026-01-01 00:00:00"),
    ]


def test_merge_overwrites_in_window_with_api_version():
    """Inside the window the API wins, so corrected stop times propagate."""
    master = make_df(
        [make_row(start="2026-07-01 00:00:00", stop="2026-07-01 01:00:00")]
    )
    # Same event, corrected by Thames to have run two hours longer.
    new = make_df([make_row(start="2026-07-01 00:00:00", stop="2026-07-01 03:00:00")])

    merged = merge_history_tables(master, new, CUTOFF)

    assert len(merged) == 1
    assert merged["StopDateTime"].iloc[0] == pd.Timestamp("2026-07-01 03:00:00")
    assert merged["Duration"].iloc[0] == 180.0


def test_merge_leaves_pre_window_master_rows_untouched():
    """History older than the window is never rewritten by the API."""
    old = make_row(start="2022-05-11 09:26:00", stop="2022-05-11 09:34:00")
    master = make_df([old])
    # The API disagrees about this old event, but it is outside the window.
    new = make_df(
        [
            make_row(start="2022-05-11 09:26:00", stop="2099-01-01 00:00:00"),
            make_row(start="2026-07-01 00:00:00", stop="2026-07-01 02:00:00"),
        ]
    )

    merged = merge_history_tables(master, new, CUTOFF)

    kept = merged[merged["StartDateTime"] == pd.Timestamp("2022-05-11 09:26:00")]
    assert len(kept) == 1
    assert kept["StopDateTime"].iloc[0] == pd.Timestamp("2022-05-11 09:34:00")


def test_merge_preserves_retired_monitors():
    """
    A monitor absent from the API response keeps its history.

    `history_to_discharge_df` only walks *active* monitors, so a monitor that has
    been decommissioned vanishes from every fresh fetch. Merging at the table
    level is what stops that from erasing its past.
    """
    master = make_df(
        [
            make_row(
                location="Retired Site",
                permit="OLD.1",
                start="2023-01-01 00:00:00",
                stop="2023-01-01 01:00:00",
            ),
            make_row(
                location="Site A",
                start="2023-02-01 00:00:00",
                stop="2023-02-01 01:00:00",
            ),
        ]
    )
    new = make_df(
        [
            make_row(
                location="Site A",
                start="2026-07-01 00:00:00",
                stop="2026-07-01 01:00:00",
            )
        ]
    )

    merged = merge_history_tables(master, new, CUTOFF)

    assert "Retired Site" in merged["LocationName"].tolist()


def test_merge_is_idempotent():
    """Merging the same window twice changes nothing."""
    master = make_df(
        [make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00")]
    )
    new = make_df([make_row(start="2026-07-01 00:00:00", stop="2026-07-01 02:00:00")])

    once = merge_history_tables(master, new, CUTOFF)
    twice = merge_history_tables(once, new, CUTOFF)

    pd.testing.assert_frame_equal(once, twice)


def test_merge_distinguishes_locations_sharing_a_permit():
    """
    The merge key includes LocationName.

    In the real record 518 permit numbers map to 542 location names, so keying on
    (PermitNumber, StartDateTime) alone would collapse distinct events.
    """
    shared_start = "2026-07-01 00:00:00"
    new = make_df(
        [
            make_row(
                location="Site A",
                permit="SHARED.1",
                start=shared_start,
                stop="2026-07-01 01:00:00",
            ),
            make_row(
                location="Site B",
                permit="SHARED.1",
                start=shared_start,
                stop="2026-07-01 02:00:00",
            ),
        ]
    )
    master = make_df(
        [make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00")]
    )

    merged = merge_history_tables(master, new, CUTOFF)

    in_window = merged[merged["StartDateTime"] == pd.Timestamp(shared_start)]
    assert len(in_window) == 2, "events sharing a permit number were collapsed"


def test_merge_refuses_to_drop_history_for_an_empty_response():
    """An empty API response must not silently truncate the master."""
    master = make_df(
        [make_row(start="2026-07-01 00:00:00", stop="2026-07-01 01:00:00")]
    )
    empty = make_df([])

    with pytest.raises(ValueError, match="Refusing to merge"):
        merge_history_tables(master, empty, CUTOFF)


def test_merge_allows_empty_response_when_nothing_would_be_lost():
    """A genuinely quiet window is fine so long as no master rows are dropped."""
    master = make_df(
        [make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00")]
    )
    merged = merge_history_tables(master, make_df([]), CUTOFF)
    assert len(merged) == 1


def test_merge_rejects_frames_missing_columns():
    """Both inputs are schema-checked before anything is discarded."""
    master = make_df([make_row()])
    with pytest.raises(ValueError, match="`new` is missing required column"):
        merge_history_tables(master, pd.DataFrame({"LocationName": ["x"]}), CUTOFF)


def test_merge_output_has_no_duplicate_keys():
    """The merged table is unique on the event key."""
    master = make_df(
        [
            make_row(start="2026-01-01 00:00:00", stop="2026-01-01 01:00:00"),
            make_row(start="2026-07-01 00:00:00", stop="2026-07-01 01:00:00"),
        ]
    )
    new = make_df(
        [
            make_row(start="2026-07-01 00:00:00", stop="2026-07-01 03:00:00"),
            make_row(start="2026-08-01 00:00:00", stop="2026-08-01 01:00:00"),
        ]
    )

    merged = merge_history_tables(master, new, CUTOFF)

    assert not merged.duplicated(subset=EVENT_KEY).any()


# --------------------------------------------------------------------------
# Straddling events
# --------------------------------------------------------------------------


def test_straddling_mask_selects_ongoing_events_from_before_the_window():
    """An ongoing event that began before the window needs a targeted refresh."""
    master = make_df(
        [
            make_row(
                location="Ongoing", start="2026-03-01 00:00:00", stop=None, ongoing=True
            ),
            make_row(
                location="Old closed",
                start="2026-01-01 00:00:00",
                stop="2026-01-01 01:00:00",
            ),
            make_row(
                location="In window",
                start="2026-07-01 00:00:00",
                stop="2026-07-01 01:00:00",
            ),
        ]
    )

    mask = straddling_event_mask(master, CUTOFF)

    assert master[mask]["LocationName"].tolist() == ["Ongoing"]


def test_straddling_mask_ignores_events_with_a_recorded_stop_time():
    """
    A closed event spanning the boundary is left alone.

    Its stop time was recorded while it sat inside an earlier run's window, so it
    is already authoritative. Refreshing these would pull in every long offline
    period crossing the boundary - dozens of extra API calls for no new data.
    """
    master = make_df(
        [
            make_row(
                location="Closed straddler",
                start="2026-05-01 00:00:00",
                stop="2026-06-15 00:00:00",
            ),
            make_row(
                location="Old closed",
                start="2026-01-01 00:00:00",
                stop="2026-01-02 00:00:00",
            ),
        ]
    )

    mask = straddling_event_mask(master, CUTOFF)

    assert master[mask]["LocationName"].tolist() == []


def test_straddling_mask_ignores_ongoing_events_inside_the_window():
    """An ongoing event that began inside the window is refreshed by the merge itself."""
    master = make_df(
        [
            make_row(
                location="In window",
                start="2026-07-01 00:00:00",
                stop=None,
                ongoing=True,
            ),
        ]
    )

    assert master[straddling_event_mask(master, CUTOFF)].empty
