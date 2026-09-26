"""Day-1 test: can the local model keep up with the English flag rate? (design section 13)

Sends diff-sized prompts to Ollama one at a time (the enricher is one worker) and reports
calls per second against the required rate: 1.5 x (2% of non-bot English article edits).

Usage: uv run python -m editguard.tools.llm_bench --non-bot-per-hour 3870 [--calls 30]
"""

import argparse
import json
import statistics
import time

import httpx

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
REVIEW_BUDGET = 0.02  # design headline: recall at a 2% review budget
HEADROOM = 1.5  # design pass rule: at least 1.5x the English flag rate

# Synthetic but realistic: an article paragraph and a changed version (~450 tokens in).
DIFF = """--- before
The river rises in the northern hills and flows south for 212 kilometres before
joining the estuary near the old port. Its basin covers about 3,400 square
kilometres and supports farming, fishing and a small hydroelectric plant built in
1964. The main tributaries are the Alder, the Keld and the Holm. Flooding was common
until embankments were completed in 1978; the last major flood was in 1976, when the
lower town was evacuated for nine days. The river gives its name to the county.
+++ after
The river rises in the northern hills and flows south for 212 kilometres before
joining the estuary near the old port. Its basin covers about 3,400 square
kilometres and supports farming, fishing and a small hydroelectric plant built in
1964. The main tributaries are the Alder, the Keld and the Holm. {change}
The river gives its name to the county."""

CHANGES = [
    "Flooding was common until embankments were completed in 1978.",
    "THIS RIVER IS THE WORST AND EVERYONE WHO LIVES HERE IS STUPID LOL",
    "Flooding was common until embankments were completed in 1987; the last major flood "
    "was in 1976.",
    "Visit www.cheap-river-tours.example for the best deals on boat trips!!!",
    "Flooding was common until embankments were completed in 1978; the last major flood "
    "was in 1976, when the lower town was evacuated for nine days.[1]",
]

SYSTEM = (
    "You review Wikipedia edits. Classify the change shown in the diff as one of: "
    "vandalism, spam, unsourced_change, good_faith. Reply with JSON only: "
    '{"label": "...", "reason": "one short sentence"}'
)


def required_calls_per_second(non_bot_edits_per_hour: float) -> float:
    """Flags per second at the 2% review budget, times the 1.5x headroom."""
    return non_bot_edits_per_hour * REVIEW_BUDGET * HEADROOM / 3600


def call(client: httpx.Client, model: str, change: str) -> tuple[float, dict[str, object]]:
    """One verdict request. Returns (seconds, parsed JSON reply)."""
    body = {
        "model": model,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0, "num_predict": 120},
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": DIFF.format(change=change)},
        ],
    }
    start = time.perf_counter()
    response = client.post(OLLAMA_URL, json=body)
    response.raise_for_status()
    elapsed = time.perf_counter() - start
    return elapsed, json.loads(response.json()["message"]["content"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--model", default="qwen3:4b-instruct")
    parser.add_argument("--calls", type=int, default=30)
    parser.add_argument("--non-bot-per-hour", type=float, required=True, help="from tools.day1")
    args = parser.parse_args()

    with httpx.Client(timeout=120) as client:
        call(client, args.model, CHANGES[0])  # warm-up: loads the model into memory
        timings, labels = [], []
        wall_start = time.perf_counter()
        for i in range(args.calls):
            seconds, reply = call(client, args.model, CHANGES[i % len(CHANGES)])
            timings.append(seconds)
            labels.append(reply.get("label"))
        wall = time.perf_counter() - wall_start

    achieved = args.calls / wall
    required = required_calls_per_second(args.non_bot_per_hour)
    print(f"Model: {args.model}, {args.calls} sequential calls in {wall:.1f} s")
    print(f"  latency p50 {statistics.median(timings):.2f} s, max {max(timings):.2f} s")
    print(f"  achieved: {achieved:.2f} calls/s")
    print(f"  required: {required:.4f} calls/s (1.5 x 2% of {args.non_bot_per_hour:g} edits/h)")
    print(f"  ratio: {achieved / required:.0f}x the required rate")
    print(f"  labels for the {len(CHANGES)} sample changes: {labels[: len(CHANGES)]}")
    print("PASS" if achieved >= required else "FAIL")


if __name__ == "__main__":
    main()
