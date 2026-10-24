import pytest

from ghpipe.client import ApiClient
from ghpipe.config import Settings
from ghpipe.mock_server import MockGitHub


@pytest.fixture
def server():
    s = MockGitHub(n_issues=250).start()
    yield s
    s.stop()


@pytest.fixture
def waits():
    return []


@pytest.fixture
def make_client(server, waits):
    clients = []

    def _make(**kw):
        kw.setdefault("sleep", waits.append)  # never really sleep in tests; record what we *would* wait
        c = ApiClient(server.url, "dev-token", **kw)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture
def settings(server, tmp_path):
    return Settings(base_url=server.url, token="dev-token", data_dir=tmp_path, per_page=100)
