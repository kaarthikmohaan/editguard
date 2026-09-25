from editguard.producer.gaps import Gap, GapDetector, decode_checkpoint, encode_checkpoint

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


def test_checkpoint_round_trip() -> None:
    token = encode_checkpoint('[{"topic":"x"}]', T, 0, 42)
    assert decode_checkpoint(token) == ('[{"topic":"x"}]', {(T, 0): 42})


def test_old_bare_id_bookmark_still_resumes() -> None:
    old = '[{"topic":"eqiad.mediawiki.page_change.v1","partition":0,"timestamp":1}]'
    assert decode_checkpoint(old) == (old, {})
    assert decode_checkpoint(None) == (None, {})
