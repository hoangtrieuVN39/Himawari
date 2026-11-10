"""Shared test fixtures for dashboard tests."""

import shutil
from pathlib import Path
import pytest


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

