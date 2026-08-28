"""
Read, write and merge water company discharge-history tables.

The canonical on-disk format is the one produced by
`WaterCompany.history_to_discharge_df()` / `history_to_offline_df()` and
serialised with `pandas.DataFrame.to_json(orient="columns")`. It is the format
already published at
`https://d1kmd884co9q6x.cloudfront.net/discharges_to_date/up_to_now.json` and
emitted by the historical-EIR pipeline, so it is the natural interchange format
between a long-lived "master" history and a freshly fetched window of it.

Two properties of that format are load-bearing and must not be changed:

* ``orient="columns"`` — a dict of column name to {row index: value}, *not* a
  list of records. The website indexes the columns by shared row keys.
* ``date_format="epoch"`` — timestamps are epoch **milliseconds as integers**.
  The website does ``new Date(value)``, which is correct for a number but yields
  ``Invalid Date`` for the same value as a string.

The merge logic here is deliberately pure (DataFrame in, DataFrame out) so it can
be unit tested without touching the network.
"""

import datetime
import json
import os
import warnings

import pandas as pd

# The nine columns of the canonical history schema, in order. Produced by
# `Event._to_row()`.
HISTORY_COLUMNS = [
    "LocationName",
    "PermitNumber",
    "X",
    "Y",
    "ReceivingWaterCourse",
    "StartDateTime",
    "StopDateTime",
    "Duration",
    "OngoingEvent",
]

# Columns that uniquely identify an event. Verified against the full published
# history (77,161 rows): this triple is unique, whereas
# (PermitNumber, StartDateTime) is *not* — 518 permit numbers map to 542 distinct
# location names, so permit number alone cannot identify a monitor.
EVENT_KEY = ["LocationName", "PermitNumber", "StartDateTime"]

_DATETIME_COLUMNS = ["StartDateTime", "StopDateTime"]

# The datetime resolution this pandas version produces natively, probed the same
# way `Event._to_row` builds a row. pandas 2 uses nanoseconds throughout, while
# pandas 3 uses microseconds; parsing epoch milliseconds gives a third answer
# again. Normalising reads to this keeps a round-trip dtype-stable and lets
# `merge_history_tables` concatenate a loaded master with a freshly fetched
# frame without a resolution mismatch.
_NATIVE_DATETIME_DTYPE = pd.DataFrame(
    {"probe": pd.to_datetime("2000-01-01T00:00:00")}, index=[0]
)["probe"].dtype


def read_history_json(path_or_buf) -> pd.DataFrame:
    """
    Read a history table from the canonical column-oriented JSON format.

    Epoch-millisecond timestamps are converted back to datetimes.

    Args:
        path_or_buf: Path to a JSON file, or anything `pandas.read_json` accepts.

    Returns:
        A DataFrame with the columns of `HISTORY_COLUMNS`.

    Raises:
        ValueError: If required columns are missing.

    """
    df = pd.read_json(path_or_buf, orient="columns", convert_dates=False)

    missing = [c for c in HISTORY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"History JSON is missing required column(s): {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    for column in _DATETIME_COLUMNS:
        # Values are epoch milliseconds when written by `write_history_json`, but
        # tolerate ISO strings so hand-edited or third-party files also load.
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = pd.to_datetime(df[column], unit="ms")
        else:
            df[column] = pd.to_datetime(df[column])
        df[column] = df[column].astype(_NATIVE_DATETIME_DTYPE)

    # Keep the remaining dtypes stable across a round-trip too. A float column
    # whose values all happen to be whole numbers is written as `60` and read
    # back as int64, and `merge_history_tables` concatenates a master frame with
    # a freshly-fetched one, so the two must agree.
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce").astype("float64")
    df["OngoingEvent"] = df["OngoingEvent"].fillna(False).astype(bool)

    return df[HISTORY_COLUMNS].reset_index(drop=True)


def write_history_json(df: pd.DataFrame, path: str) -> None:
    """
    Write a history table to the canonical column-oriented JSON format, atomically.

    The write goes to a temporary file in the same directory and is then moved
    into place with `os.replace`, so a crash or a full disk cannot leave a
    truncated history behind for the next run (or the website) to read.

    Args:
        df: A history DataFrame carrying at least `HISTORY_COLUMNS`.
        path: Destination path.

    Raises:
        ValueError: If required columns are missing.

    """
    missing = [c for c in HISTORY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Cannot write history: missing column(s) {missing}")

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = os.path.join(directory, f".{os.path.basename(path)}.tmp-{os.getpid()}")

    try:
        # orient="columns" and the default epoch-millisecond date format are the
        # published contract - see the module docstring before changing either.
        df[HISTORY_COLUMNS].to_json(temporary, orient="columns")

        # Cheap sanity check that we produced readable JSON before overwriting a
        # good file with it.
        with open(temporary) as f:
            json.load(f)

        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def merge_history_tables(
    master: pd.DataFrame,
    new: pd.DataFrame,
    authoritative_since: datetime.datetime,
) -> pd.DataFrame:
    """
    Splice a freshly fetched window of history into a long-lived master table.

    Within the authoritative window the API is treated as the source of truth:
    every master row starting at or after `authoritative_since` is discarded and
    replaced by what the API returned. Older master rows are kept untouched,
    which is what preserves history for monitors that have since been retired
    (`history_to_discharge_df` only walks *active* monitors, so a full rebuild
    silently drops them).

    Args:
        master: The long-lived history, covering the whole record.
        new: A freshly fetched history covering at least the authoritative window.
        authoritative_since: Start of the window in which `new` overrides `master`.

    Returns:
        The merged table, sorted by `StartDateTime` descending.

    Raises:
        ValueError: If either frame is missing required columns, or if the merge
            would shrink the history — a guard against an empty or truncated API
            response silently destroying years of records.

    """
    for name, frame in (("master", master), ("new", new)):
        missing = [c for c in HISTORY_COLUMNS if c not in frame.columns]
        if missing:
            raise ValueError(f"`{name}` is missing required column(s): {missing}")

    kept = master[master["StartDateTime"] < authoritative_since]
    incoming = new[new["StartDateTime"] >= authoritative_since]
    replaced = len(master) - len(kept)

    if incoming.empty and replaced > 0:
        raise ValueError(
            f"Refusing to merge: the new data contains no events at or after "
            f"{authoritative_since}, but doing so would drop {replaced} existing "
            f"master row(s). This usually means the API returned an empty or "
            f"truncated response rather than that the events really vanished."
        )

    merged = pd.concat([kept, incoming], ignore_index=True)

    # `incoming` rows come last, so keep="last" lets the API win any collision.
    merged = merged.drop_duplicates(subset=EVENT_KEY, keep="last")
    merged = merged.sort_values(by="StartDateTime", ascending=False, ignore_index=True)

    if len(merged) < len(kept):
        raise ValueError(
            f"Refusing to merge: result ({len(merged)} rows) is smaller than the "
            f"retained pre-window history ({len(kept)} rows)."
        )

    print(
        "\033[36m"
        + f"Merged history: kept {len(kept)} row(s) before {authoritative_since}, "
        + f"replaced {replaced} with {len(incoming)} from the API "
        + f"-> {len(merged)} total."
        + "\033[0m"
    )
    return merged


def straddling_event_mask(
    master: pd.DataFrame, authoritative_since: datetime.datetime
) -> pd.Series:
    """
    Identify master events that the window rule cannot refresh.

    An event that *started* before the authoritative window is never replaced by
    the merge, which matches rows on start time. That is only a problem for an
    event still marked ongoing: its stop time is a placeholder that ages, and on
    the website it renders as a bar that never ends.

    Events that started before the window and have a *recorded* stop time need no
    refresh. Their stop time was written while they sat inside an earlier run's
    window, so it was authoritative then; picking up any later revision to it is
    the job of a periodic full rebuild, not of every incremental run. Including
    them here would drag in every long offline period spanning the boundary and
    turn a targeted top-up into dozens of extra API calls.

    Args:
        master: The long-lived history table.
        authoritative_since: Start of the window in which the API is authoritative.

    Returns:
        A boolean Series selecting the rows needing a targeted refresh.

    """
    started_before = master["StartDateTime"] < authoritative_since
    still_open = master["OngoingEvent"].fillna(False).astype(bool)
    return started_before & still_open


def events_to_history_df(events: list) -> pd.DataFrame:
    """
    Convert a list of `Event` objects to a history table.

    A small helper mirroring `WaterCompany.history_to_discharge_df` for cases
    where you already hold the events, e.g. after a targeted per-monitor refetch.

    Args:
        events: Event objects to convert.

    Returns:
        A history DataFrame, sorted by `StartDateTime` descending.

    """
    if not events:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    df = pd.concat([event._to_row() for event in events], ignore_index=True)
    return df.sort_values(by="StartDateTime", ascending=False, ignore_index=True)


def history_df_to_events(
    df: pd.DataFrame, monitor, event_types: tuple[str, ...] = ("Discharging", "Offline")
) -> list:
    """
    Rebuild `Event` objects for a single monitor from a history table.

    Events are returned newest-first, which is the ordering POOPy assumes
    everywhere (`_history_masks`, `recent_discharge_at` and `plot_history` all
    walk the list backwards in time).

    Args:
        df: Rows of the history table belonging to this monitor.
        monitor: The `Monitor` the events belong to.
        event_types: Which event types the rows represent. A history table does
            not record its own event type, so the caller states it: discharge
            tables build `Discharge` events, offline tables build `Offline` ones.

    Returns:
        A list of `Event` objects, newest first.

    """
    # Imported here to avoid a circular import at module load time.
    from poopy.poopy import Discharge, Offline

    constructor = Discharge if "Discharging" in event_types else Offline

    events = []
    ordered = df.sort_values(by="StartDateTime", ascending=False)
    for _, row in ordered.iterrows():
        ongoing = bool(row["OngoingEvent"])
        start = pd.to_datetime(row["StartDateTime"])
        stop = pd.to_datetime(row["StopDateTime"])

        if pd.isna(start):
            warnings.warn(
                f"\033[91m! WARNING ! Skipping an event for '{monitor.site_name}' "
                f"with no start time.\033[0m"
            )
            continue

        # `Event._validate` forbids an end time on an ongoing event.
        events.append(
            constructor(
                monitor=monitor,
                ongoing=ongoing,
                start_time=start,
                end_time=None if ongoing or pd.isna(stop) else stop,
            )
        )
    return events
