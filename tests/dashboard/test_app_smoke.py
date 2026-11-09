from pathlib import Path
from streamlit.testing.v1 import AppTest


def test_streamlit_app_renders_without_exception():
    app_path = Path(__file__).resolve().parents[2] / "src" / "dashboard" / "app.py"
    at = AppTest.from_file(str(app_path), default_timeout=30)
    at.run()
    assert not at.exception
    assert len(at.tabs) == 3
