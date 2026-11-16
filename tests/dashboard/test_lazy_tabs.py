"""Only the selected tab's content may be computed and sent to the browser."""

from unittest import mock

import pytest
from streamlit.testing.v1 import AppTest

from src.dashboard import app


def _count(node) -> int:
    total = 0
    for child in getattr(node, "children", {}).values():
        total += 1 + _count(child)
    return total


@pytest.fixture
def app_test(isolated_data, monkeypatch):
    monkeypatch.setattr(app, "STORAGE_ROOT", str(isolated_data))
    at = AppTest.from_function(_entrypoint, args=(str(isolated_data),), default_timeout=120)
    return at


def _entrypoint(storage_root: str):
    from src.dashboard import app as dashboard_app

    dashboard_app.STORAGE_ROOT = storage_root
    dashboard_app.main()


def test_inactive_tabs_render_no_elements(app_test):
    at = app_test.run()
    assert not at.exception
    tabs = list(at.tabs)
    assert len(tabs) == 4
    assert _count(tabs[0]) > 0, "selected tab must render"
    assert _count(tabs[1]) == 0, "inactive KPI tab must not be computed"
    assert _count(tabs[2]) == 0, "inactive audit tab must not be computed"
    assert _count(tabs[3]) == 0, "inactive health tab must not be computed"


def test_selecting_kpi_tab_renders_only_that_tab(app_test):
    at = app_test.run()
    kpi_label = list(at.tabs)[1].label
    at.session_state["main_tabs"] = kpi_label
    at.run()
    assert not at.exception
    tabs = list(at.tabs)
    assert _count(tabs[1]) > 0, "selected KPI tab must render"
    assert _count(tabs[0]) == 0, "previously selected tab must no longer be computed"
    assert _count(tabs[2]) == 0 and _count(tabs[3]) == 0


def test_make_tabs_falls_back_when_streamlit_has_no_lazy_tabs():
    real_tabs = app.st.tabs

    def old_tabs(labels, **kwargs):
        if kwargs:
            raise TypeError("tabs() got an unexpected keyword argument 'on_change'")
        return real_tabs(labels)

    with mock.patch.object(app.st, "tabs", side_effect=old_tabs):
        tabs, lazy = app._make_tabs(["a", "b"])

    assert lazy is False
    assert len(tabs) == 2
    # without lazy support every tab counts as active, so nothing disappears
    assert all(app._tab_active(t, lazy) for t in tabs)
