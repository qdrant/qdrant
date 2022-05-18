# Tracks processes that need to be killed at the end of the test
processes = []


def pytest_sessionfinish(session, exitstatus):
    print()
    for p in processes:
        print(f"Killing {p.pid}")
        p.kill()
