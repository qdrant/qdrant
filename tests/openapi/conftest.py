import http.server
import pytest
import socketserver
import threading
import os


HTTP_SERVER_HOST = os.environ.get("HTTP_SERVER_HOST", "localhost")

@pytest.fixture(params=[False, True], scope="module")
def on_disk_vectors(request):
    return request.param


@pytest.fixture(params=[False, True], scope="module")
def on_disk_payload(request):
    return request.param


@pytest.fixture
def http_server(tmpdir):
    """
    Starts a HTTP server serving files from a temporary directory.
    Yields a tuple (tmpdir, url).
    """

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            # Serve files from the temporary directory
            super().__init__(*args, directory=str(tmpdir), **kwargs)

        def log_request(self, *args, **kwargs):
            # Silence logging
            pass

    with socketserver.TCPServer(("0.0.0.0", 0), Handler) as httpd:
        httpd.allow_reuse_address = True
        thread = threading.Thread(
            target=httpd.serve_forever,
            # Lower the shutdown poll interval to speed up tests
            kwargs={"poll_interval": 0.1},
        )
        thread.start()
        yield (tmpdir, f"http://{HTTP_SERVER_HOST}:{httpd.server_address[1]}")
        httpd.shutdown()
        thread.join()


@pytest.fixture(scope='module', autouse=True)
def collection_name(request):
    return request.node.name.removesuffix(".py")
