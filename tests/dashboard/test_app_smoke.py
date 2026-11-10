"""Smoke test for Streamlit dashboard application coordinator and UI components."""

from pathlib import Path
from unittest import mock

from src.dashboard import app


def test_dashboard_main_renders_without_exception(isolated_data, monkeypatch):
    """main() renders every tab against an isolated copy of data/, never touching the repo's data/."""
    monkeypatch.setattr(app, "STORAGE_ROOT", str(isolated_data))
    with mock.patch("streamlit.set_page_config") as mock_cfg, mock.patch("streamlit.rerun"):
        app.main()
        assert mock_cfg.called


def test_dashboard_script_file_exists():
    """Verify that the dashboard main script file exists at the expected path."""
    app_path = Path(__file__).resolve().parents[2] / "src" / "dashboard" / "app.py"
    assert app_path.exists()
    assert app_path.is_file()
