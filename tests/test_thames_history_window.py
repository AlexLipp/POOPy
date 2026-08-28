"""
Offline tests for the time-bounded history pagination.

The Thames alerts API has no server-side date filter, so bounding a fetch in time
is done purely by deciding when to stop paginating. Getting that decision wrong is
expensive and silent - it either fetches four years when it meant to fetch one
month, or stops early and loses events - so it is tested here against a fake API
rather than the live one.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from poopy.companies import ThamesWater


class FakeResponse:
    """Stands in for a `requests.Response` from the alerts endpoint."""

    def __init__(self, items, url="https://fake/alerts"):
        """Store the page of items this response should return."""
        self.status_code = 200
        self.url = url
        self._items = items

    def json(self):
        """Return the response body, omitting "items" to signal the end of the record."""
        return {"items": self._items} if self._items is not None else {}


def make_page(start: datetime, n: int, step_minutes: int = 60):
    """Build a page of n alerts running backwards in time from `start`."""
    return [
        {
            "locationName": "Site A",
            "permitNumber": "TEMP.1",
            "x": 1,
            "y": 2,
            "receivingWaterCourse": "River Test",
            "alertType": "Start" if i % 2 else "Stop",
            "datetime": (start - timedelta(minutes=step_minutes * i)).isoformat(),
        }
        for i in range(n)
    ]


@pytest.fixture
def company():
    """Build a ThamesWater instance without making any network calls."""
    return object.__new__(ThamesWater)


def run_fetch(company, monkeypatch, pages, since=None):
    """Drive `_handle_history_api_response` against a canned list of pages."""
    calls = []

    def fake_get(url, params=None, timeout=None, **kwargs):
        calls.append(dict(params))
        index = params["offset"] // params["limit"]
        items = pages[index] if index < len(pages) else []
        return FakeResponse(items)

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)

    df = company._handle_history_api_response(
        url="https://fake/alerts",
        params={"limit": ThamesWater.API_LIMIT, "offset": 0},
        since=since,
    )
    return df, calls


def test_since_stops_at_first_page_crossing_the_bound(company, monkeypatch):
    """
    A full page that already reaches past `since` ends the fetch.

    Records come back newest-first and every page is a full 1000 long until the
    very end of the record, so a stop condition that also demanded a short page
    would page all the way back to 2022 no matter what `since` was set to.
    """
    now = datetime(2026, 8, 28, 12, 0)
    # One full page spanning ~41 days back, well past the 30-day bound.
    pages = [make_page(now, ThamesWater.API_LIMIT)]

    df, calls = run_fetch(company, monkeypatch, pages, since=now - timedelta(days=30))

    assert len(calls) == 1, f"expected to stop after one page, made {len(calls)}"
    assert len(df) == ThamesWater.API_LIMIT


def test_since_keeps_paging_until_the_bound_is_reached(company, monkeypatch):
    """Pagination continues while pages are still newer than `since`."""
    now = datetime(2026, 8, 28, 12, 0)
    # Each page covers ~16 hours, so three are needed to pass a 24-hour bound.
    pages = [
        make_page(now - timedelta(hours=16 * i), ThamesWater.API_LIMIT, step_minutes=1)
        for i in range(4)
    ]

    df, calls = run_fetch(company, monkeypatch, pages, since=now - timedelta(hours=24))

    assert len(calls) == 2
    assert len(df) == 2 * ThamesWater.API_LIMIT


def test_since_fetches_past_the_bound_for_overlap(company, monkeypatch):
    """
    The fetch reaches strictly further back than `since`.

    Events straddling the boundary are only reconstructable if their start alert
    was fetched too, which is why the caller's buffer relies on the final page
    being kept whole rather than truncated at `since`.
    """
    now = datetime(2026, 8, 28, 12, 0)
    pages = [make_page(now, ThamesWater.API_LIMIT, step_minutes=60)]
    since = now - timedelta(days=30)

    df, _ = run_fetch(company, monkeypatch, pages, since=since)

    oldest = pd.to_datetime(df["DateTime"]).min()
    assert oldest < since, "final page was truncated, losing the overlap"


def test_full_fetch_is_unchanged_and_needs_a_short_page_to_stop(company, monkeypatch):
    """
    Without `since` the original behaviour is preserved.

    A full fetch cannot use the date bound alone, because the record genuinely
    ends at HISTORY_VALID_UNTIL; a short page is what marks the true end.
    """
    old = ThamesWater.HISTORY_VALID_UNTIL
    pages = [
        make_page(old + timedelta(days=2), ThamesWater.API_LIMIT, step_minutes=1),
        make_page(old + timedelta(days=1), ThamesWater.API_LIMIT, step_minutes=1),
        # A short final page: the end of the record.
        make_page(old - timedelta(minutes=1), 10, step_minutes=1),
    ]

    df, calls = run_fetch(company, monkeypatch, pages, since=None)

    assert len(calls) == 3
    assert len(df) == 2 * ThamesWater.API_LIMIT + 10


def test_offsets_advance_by_the_page_limit(company, monkeypatch):
    """Each successive request asks for the next page."""
    now = datetime(2026, 8, 28, 12, 0)
    pages = [
        make_page(now - timedelta(hours=16 * i), ThamesWater.API_LIMIT, step_minutes=1)
        for i in range(4)
    ]

    _, calls = run_fetch(company, monkeypatch, pages, since=now - timedelta(hours=24))

    assert [c["offset"] for c in calls] == [0, ThamesWater.API_LIMIT]


def test_requests_carry_a_timeout(company, monkeypatch):
    """Every request is bounded in time, so a hung connection cannot stall a run."""
    now = datetime(2026, 8, 28, 12, 0)
    seen = {}

    def fake_get(url, params=None, timeout=None, **kwargs):
        seen["timeout"] = timeout
        return FakeResponse(make_page(now, 10))

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)
    # The single page reaches back 9 hours, so a 1-hour bound ends the fetch.
    company._handle_history_api_response(
        url="https://fake/alerts",
        params={"limit": ThamesWater.API_LIMIT, "offset": 0},
        since=now - timedelta(hours=1),
    )

    assert seen["timeout"] == ThamesWater.REQUEST_TIMEOUT


# --------------------------------------------------------------------------
# Retry behaviour
# --------------------------------------------------------------------------


class FailingResponse:
    """A response carrying an error status code."""

    def __init__(self, status_code, payload=None, url="https://fake/alerts"):
        """Store the status code and optional error body for this response."""
        self.status_code = status_code
        self.url = url
        self.text = "" if payload is None else str(payload)
        self._payload = payload

    def json(self):
        """Return the error body, raising if it is not JSON at all."""
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """Make backoff instantaneous so retry tests stay fast."""
    monkeypatch.setattr("poopy.companies.thames_water.time.sleep", lambda _: None)


@pytest.mark.parametrize("status", [429, 500, 503])
def test_history_fetch_retries_recoverable_errors(company, monkeypatch, status):
    """A recoverable failure is retried rather than aborting the run."""
    now = datetime(2026, 8, 28, 12, 0)
    responses = [
        FailingResponse(status, {"error": "Quota has been exceeded"}),
        FakeResponse(make_page(now, 10)),
    ]

    def fake_get(url, params=None, timeout=None, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)
    df = company._handle_history_api_response(
        url="https://fake/alerts",
        params={"limit": ThamesWater.API_LIMIT, "offset": 0},
        since=now - timedelta(hours=1),
    )

    assert len(df) == 10, "the retried request's data was not returned"
    assert responses == [], "the request was not retried"


def test_current_fetch_also_retries(company, monkeypatch):
    """
    The current-status endpoint retries too.

    This is the path the ThamesWater constructor uses, so without a retry a
    single quota blip aborts the run before any history work begins.
    """
    now = datetime(2026, 8, 28, 12, 0)
    responses = [
        FailingResponse(429, {"error": "Quota has been exceeded"}),
        FakeResponse(make_page(now, 5)),
        FakeResponse(None),  # no "items" -> end of records
    ]

    def fake_get(url, params=None, timeout=None, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)
    df = company._handle_current_api_response(
        url="https://fake/status", params={"limit": ThamesWater.API_LIMIT, "offset": 0}
    )

    assert len(df) == 5
    assert responses == []


def test_non_recoverable_status_is_not_retried(company, monkeypatch):
    """A 404 is a bug, not a blip, so it fails immediately."""
    calls = []

    def fake_get(url, params=None, timeout=None, **kwargs):
        calls.append(url)
        return FailingResponse(404, {"error": "Not found"})

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)
    with pytest.raises(Exception, match="404"):
        company._handle_history_api_response(
            url="https://fake/alerts",
            params={"limit": ThamesWater.API_LIMIT, "offset": 0},
            since=datetime(2026, 8, 1),
        )

    assert len(calls) == 1, "a non-recoverable error should not be retried"


def test_error_text_survives_a_non_json_body(company, monkeypatch):
    """
    An error body that is not JSON still produces a useful message.

    Calling `.json()` directly in an error path would raise a decode error that
    masks the status code that actually caused the failure.
    """

    def fake_get(url, params=None, timeout=None, **kwargs):
        return FailingResponse(404, payload=None)

    monkeypatch.setattr("poopy.companies.thames_water.requests.get", fake_get)
    with pytest.raises(Exception, match="404"):
        company._handle_history_api_response(
            url="https://fake/alerts",
            params={"limit": ThamesWater.API_LIMIT, "offset": 0},
            since=datetime(2026, 8, 1),
        )
