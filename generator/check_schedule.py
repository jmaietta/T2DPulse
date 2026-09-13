"""Publishing gate for the scheduled workflow.

Writes ``build=true|false`` to ``$GITHUB_OUTPUT`` so later steps can be
skipped with ``if: steps.schedule.outputs.build == 'true'``. (The previous gate
called ``SystemExit(0)``, which passes the step and never stopped anything.)

The cron expression in the workflow controls *which hours* runs happen; this
gate only enforces the weekday rule, so a run that GitHub starts late is not
dropped for landing a few minutes outside the window.

* ``workflow_dispatch`` (manual) runs always build.
* Scheduled runs build every day unless ``PULSE_WEEKDAYS_ONLY=1``, in which case
  Saturday and Sunday (Eastern) are skipped and Friday's edition stays live.
"""
import os
from datetime import datetime
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def should_build(event_name: str, now: datetime, weekdays_only: bool = False) -> bool:
    if not weekdays_only or event_name != "schedule":
        return True
    return now.astimezone(EASTERN).weekday() < 5


def main() -> None:
    now = datetime.now(EASTERN)
    event = os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch")
    weekdays_only = os.environ.get("PULSE_WEEKDAYS_ONLY", "0") == "1"
    allowed = should_build(event, now, weekdays_only=weekdays_only)
    print(f"Eastern time: {now.isoformat(timespec='seconds')}; event={event}; "
          f"weekdays_only={weekdays_only}; build={allowed}")
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"build={str(allowed).lower()}\n")


if __name__ == "__main__":
    main()
