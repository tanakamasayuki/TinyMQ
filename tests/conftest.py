import pytest

from helpers import target


@pytest.fixture(autouse=True)
def _reset_target_host():
    # A board test points target.HOST at the board; if it fails before
    # setting it back, the next module's peers would connect to the board.
    yield
    target.HOST = "127.0.0.1"
