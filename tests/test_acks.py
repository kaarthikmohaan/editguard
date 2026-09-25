import random

from editguard.producer.acks import AckTracker


def test_nothing_done_means_no_bookmark() -> None:
    tracker = AckTracker()
    tracker.register("a")
    assert tracker.safe_event_id() is None


def test_in_order_acks_advance_the_bookmark() -> None:
    tracker = AckTracker()
    a, b = tracker.register("a"), tracker.register("b")
    tracker.done(a)
    assert tracker.safe_event_id() == "a"
    tracker.done(b)
    assert tracker.safe_event_id() == "b"


def test_out_of_order_ack_waits_for_the_gap_to_close() -> None:
    tracker = AckTracker()
    a, b, c = tracker.register("a"), tracker.register("b"), tracker.register("c")
    tracker.done(a)
    tracker.done(c)
    assert tracker.safe_event_id() == "a"  # b is still in flight, so c is not safe yet
    tracker.done(b)
    assert tracker.safe_event_id() == "c"


def test_in_flight_counts_unconfirmed_events() -> None:
    tracker = AckTracker()
    a = tracker.register("a")
    tracker.register("b")
    tracker.done(a)
    assert tracker.in_flight == 1


def test_memory_is_released_for_old_events() -> None:
    tracker = AckTracker()
    for i in range(1000):
        tracker.done(tracker.register(str(i)))
    assert tracker.safe_event_id() == "999"
    assert len(tracker._ids) <= 2


def test_bookmark_never_passes_an_unconfirmed_event() -> None:
    rng = random.Random(42)  # fixed seed: the same "random" orders on every run
    for _ in range(200):
        tracker = AckTracker()
        seqs = [tracker.register(str(i)) for i in range(50)]
        rng.shuffle(seqs)
        confirmed: set[int] = set()
        for seq in seqs:
            tracker.done(seq)
            confirmed.add(seq)
            bookmark = tracker.safe_event_id()
            if bookmark is not None:
                assert set(range(int(bookmark) + 1)) <= confirmed
