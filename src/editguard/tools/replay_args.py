"""Print dbt --args for the replay-count check from a replay report.

Usage: uv run python -m editguard.tools.replay_args data/replays/replay-<window>.json
(make replay-check calls this.)
"""

import json
import sys
from pathlib import Path


def replay_args(report: dict) -> str:
    """The replay_count macro's arguments: the window and the distinct events sent."""
    if not report.get("complete"):
        raise SystemExit("this replay did not complete; run it again before checking")
    return json.dumps(
        {
            "since": report["since"],
            "until": report["until"],
            "expected": report["distinct_event_ids"],
        }
    )


if __name__ == "__main__":
    print(replay_args(json.loads(Path(sys.argv[1]).read_text())))
