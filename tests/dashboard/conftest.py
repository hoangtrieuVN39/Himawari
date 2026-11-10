"""Shared test fixtures for dashboard tests."""

import shutil
import sys
from pathlib import Path
import pytest


@pytest.fixture(autouse=True)
def _restore_main_module():
    """AppTest runs its generated script as `__main__` and leaves it there; Windows multiprocessing
    (spawn) later re-executes that script in child processes (NameError: __args) and breaks unrelated
    tests such as tests/ingestion/test_raw_store.py::test_concurrent_multiprocess_writers."""
    main = sys.modules.get("__main__")
    yield
    if main is not None:
        sys.modules["__main__"] = main


@pytest.fixture
def isolated_data(tmp_path: Path) -> Path:
    """Creates an isolated copy of data/ directory for testing, excluding heavy HSD binary files."""
    dest = tmp_path / "data"
    src = Path("data").resolve()

    def ignore_patterns(dir_path: str, names: list[str]) -> list[str]:
        ignored = []
        try:
            rel = Path(dir_path).relative_to(src)
            if rel == Path("raw") and "himawari_hsd" in names:
                ignored.append("himawari_hsd")
        except ValueError:
            pass
        for n in names:
            if n.endswith(".tmp") or n.startswith(".storage-locks"):
                ignored.append(n)
        return ignored

    shutil.copytree(src, dest, ignore=ignore_patterns)
    return dest

