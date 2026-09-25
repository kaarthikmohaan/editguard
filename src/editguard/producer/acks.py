"""Track which stream events are safely done, so the resume bookmark never skips one.

Kafka can confirm messages out of order (different partitions, retries). If event 7 is
confirmed before event 6 and we crash, resuming after 7 would lose 6 forever. So the
bookmark only moves up to the newest event whose predecessors are all done.
"""


class AckTracker:
    """Gives each event a sequence number and reports the latest safe resume ID."""

    def __init__(self) -> None:
        self._next_seq = 0
        self._ids: dict[int, str] = {}
        self._pending: set[int] = set()
        self._safe_seq = -1

    def register(self, event_id: str) -> int:
        """Record an event as in flight and return its sequence number."""
        seq = self._next_seq
        self._next_seq += 1
        self._ids[seq] = event_id
        self._pending.add(seq)
        return seq

    def done(self, seq: int) -> None:
        """Mark an event as safely handled: delivered to Kafka, or filtered out."""
        self._pending.discard(seq)
        new_safe = min(self._pending, default=self._next_seq) - 1
        for old in range(max(self._safe_seq, 0), new_safe):
            self._ids.pop(old, None)  # older than the bookmark; no longer needed
        self._safe_seq = max(self._safe_seq, new_safe)

    @property
    def in_flight(self) -> int:
        """Events sent but not yet confirmed."""
        return len(self._pending)

    def safe_event_id(self) -> str | None:
        """Newest event ID with every earlier event done, or None if nothing is done yet."""
        return self._ids.get(self._safe_seq)
