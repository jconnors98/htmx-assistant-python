import sys, os

# Ensure the project root is on sys.path regardless of deployment path.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# If your app object is in app.py and named "app"
from app import app as application, scrape_scheduler

# --- Start APScheduler on Apache/mod_wsgi process startup ---
#
# Notes:
# - mod_wsgi can run multiple processes; without a guard you'd start multiple schedulers.
# - We use a best-effort inter-process file lock so only one process starts the scheduler.
# - To ensure this runs on *Apache boot* (not first request), pair with Apache's WSGIImportScript.

def _strtobool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _start_scrape_scheduler_once() -> None:
    if not _strtobool(os.environ.get("SCRAPE_SCHEDULER_ENABLE", "1")):
        return

    lock_path = os.environ.get(
        "SCRAPE_SCHEDULER_LOCKFILE",
        "/tmp/htmx_assistant_scrape_scheduler.lock",
    )

    # Keep the lock fd alive for the lifetime of the process.
    global _SCRAPE_SCHEDULER_LOCK_FD  # noqa: PLW0603
    _SCRAPE_SCHEDULER_LOCK_FD = None

    try:
        import fcntl  # type: ignore
    except Exception:
        fcntl = None

    if fcntl is not None:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Another Apache process already owns the scheduler lock.
            os.close(fd)
            return
        _SCRAPE_SCHEDULER_LOCK_FD = fd

    try:
        scrape_scheduler.start()
    except Exception as e:
        # Don't fail the whole WSGI app import if scheduler startup fails.
        print(f"WARNING: failed to start scrape_scheduler under mod_wsgi: {e}")


_start_scrape_scheduler_once()