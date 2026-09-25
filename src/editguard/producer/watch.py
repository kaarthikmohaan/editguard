"""Print a few live edits from the target wikis. A learning tool, not the producer."""

import argparse

import httpx

from editguard.common.config import TARGET_WIKIS, get_settings
from editguard.producer.sse import iter_events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10, help="target-wiki events to print")
    args = parser.parse_args()

    settings = get_settings()
    seen = shown = 0
    timeout = httpx.Timeout(10.0, read=60.0)
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=timeout) as client:
        for event in iter_events(client, "mediawiki.page_change.v1"):
            seen += 1
            data = event.data
            if data["wiki_id"] not in TARGET_WIKIS:
                continue
            shown += 1
            print(
                f"{data['wiki_id']:<8} {data['page_change_kind']:<7} "
                f"bot={data['performer']['is_bot']!s:<5} "
                f"offset={data['meta']['offset']}  {data['page']['page_title']}"
            )
            if shown >= args.count:
                break
    print(f"\nKept {shown} of {seen} events ({shown / seen:.0%}).")


if __name__ == "__main__":
    main()
