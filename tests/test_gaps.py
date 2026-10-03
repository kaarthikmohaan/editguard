import json

from editguard.producer.gaps import (
    Gap,
    GapDetector,
    decode_checkpoint,
    encode_checkpoint,
    resume_id,
)

T = "eqiad.mediawiki.page_change.v1"


def test_contiguous_offsets_have_no_gap() -> None:
    detector = GapDetector()
    assert [detector.observe(T, 0, o) for o in (10, 11, 12)] == [None, None, None]


def test_skipped_offsets_are_reported() -> None:
    detector = GapDetector()
    detector.observe(T, 0, 10)
    gap = detector.observe(T, 0, 14)
    assert gap == Gap(T, 0, expected=11, got=14)
    assert gap.missing == 3


def test_replayed_duplicates_are_not_gaps() -> None:
    detector = GapDetector()
    detector.observe(T, 0, 10)
    detector.observe(T, 0, 11)
    assert detector.observe(T, 0, 9) is None
    assert detector.observe(T, 0, 12) is None


def test_partitions_are_tracked_separately() -> None:
    detector = GapDetector()
    detector.observe(T, 0, 100)
    detector.observe("codfw.mediawiki.page_change.v1", 0, 5)
    assert detector.observe(T, 0, 101) is None


def test_seed_from_bookmark_catches_gap_across_restart() -> None:
    detector = GapDetector(seed={(T, 0): 10})
    assert detector.observe(T, 0, 11) is None
    assert GapDetector(seed={(T, 0): 10}).observe(T, 0, 20) == Gap(T, 0, 11, 20)


C = "codfw.mediawiki.page_change.v1"
# Wikimedia's real resume ID for eqiad offset 1110858454 (2026-09-28): a timestamp, not an offset.
WIKIMEDIA_ID = json.dumps(
    [
        {"offset": -1, "partition": 0, "topic": C},
        {"topic": T, "partition": 0, "timestamp": 1790621385996},
    ]
)


def test_checkpoint_round_trip_resumes_every_partition_at_its_next_offset() -> None:
    token = encode_checkpoint(WIKIMEDIA_ID, {(T, 0): 1110858454, (C, 0): 838990626})
    resume, positions = decode_checkpoint(token)
    assert positions == {(T, 0): 1110858454, (C, 0): 838990626}
    assert sorted(json.loads(resume), key=lambda a: a["topic"]) == [
        {"topic": C, "partition": 0, "offset": 838990627},
        {"topic": T, "partition": 0, "offset": 1110858455},  # not the timestamp: that skipped 455
    ]


def test_partition_without_a_position_keeps_wikimedias_entry() -> None:
    assert json.loads(resume_id(WIKIMEDIA_ID, {(T, 0): 5})) == [
        {"offset": -1, "partition": 0, "topic": C},
        {"topic": T, "partition": 0, "offset": 6},
    ]


def test_bookmark_from_before_the_fix_resumes_by_offset() -> None:
    old = json.dumps({"last_event_id": WIKIMEDIA_ID, "topic": T, "partition": 0, "offset": 9})
    resume, positions = decode_checkpoint(old)
    assert positions == {(T, 0): 9}
    assert {"topic": T, "partition": 0, "offset": 10} in json.loads(resume)


def test_old_bare_id_bookmark_still_resumes() -> None:
    old = '[{"topic":"eqiad.mediawiki.page_change.v1","partition":0,"timestamp":1}]'
    assert decode_checkpoint(old) == (old, {})
    assert decode_checkpoint(None) == (None, {})
