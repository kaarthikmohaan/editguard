from editguard.streaming.live_job import build_parser


def test_bronze_commits_less_often_than_scoring() -> None:
    """Bronze: one Iceberg snapshot per batch, so 60 s. Scoring: no commits, so 10 s."""
    args = build_parser().parse_args(["--env", "prod"])
    assert args.bronze_trigger_seconds == 60
    assert args.scoring_trigger_seconds == 10
    # Contract: edit-to-bronze p95 < 90 s. Worst case = one full trigger plus the commit.
    assert args.bronze_trigger_seconds + 15 < 90
