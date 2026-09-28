import datetime, typing, pydantic, decimal
"One record per edit event on the 8 target Wikipedias, taken from Wikimedia's mediawiki.page_change.v1 stream, plus flagged edits and Wikimedia baseline scores.\n"

class Edits(pydantic.BaseModel):
    """Edit events as received, one row per upstream event."""
    event_id: str
    'Upstream meta.id (UUID). Dedup key.'
    event_time: datetime.datetime
    'Top-level dt, when the change happened (equals revision.rev_dt for edits, creations and moves; the delete time for deletes). Event time for watermarks. ADR 0010.'
    emitted_at: datetime.datetime
    'meta.dt, when Wikimedia emitted the event. Used for lag.'
    ingested_at: datetime.datetime
    'When the producer received the event.'
    wiki_id: str
    'Wiki database name.'
    page_id: int
    page_title: str
    namespace_id: int
    '0 = article namespace.'
    page_change_kind: str
    'One of edit, create, move, delete, undelete, visibility_change.'
    rev_id: int
    'Revision ID. Unique within a wiki, not per page.'
    rev_parent_id: typing.Optional[int]
    rev_size: int
    'Size of the new revision in bytes.'
    prior_rev_size: typing.Optional[int]
    'prior_state.revision.rev_size. Null for page creations.'
    is_minor_edit: bool
    comment: typing.Optional[str]
    'Edit summary. Hidden when is_comment_visible is false.'
    is_content_visible: bool
    is_comment_visible: bool
    is_editor_visible: bool
    performer_user_text: typing.Optional[str]
    'Username. Bronze only; hashed in silver and gold.'
    performer_is_bot: bool
    performer_is_temp: bool
    'Temporary account (logged-out editor).'
    performer_groups: typing.Optional[list[typing.Any]]
    performer_edit_count: typing.Optional[int]
    "Editor's edit count at the time of the edit."
    performer_registration_dt: typing.Optional[datetime.datetime]
    'Account registration time. Null for about 0.9% of events.'
    revert_method: typing.Optional[str]
    'rollback, undo or manual when this edit is a revert; else null.'
    rev_reverted_oldest_id: typing.Optional[int]
    rev_reverted_newest_id: typing.Optional[int]
    rev_original_id: typing.Optional[int]
    'Revision restored by this revert.'
    upstream_topic: str
    upstream_partition: int
    upstream_offset: int
    'Used by the gap detector.'
    schema_version: str
    'Upstream $schema, e.g. /mediawiki/page/change/1.12.0.'
    raw_json: str
    'Full upstream event, kept for reprocessing. Purged for suppressed revisions.'

class Flagged_edits(pydantic.BaseModel):
    """Edits whose score passed the flag threshold."""
    wiki_id: str
    rev_id: int
    page_title: str
    event_time: datetime.datetime
    score: float
    '0 to 1; higher means more likely damaging.'
    score_version: str
    history_snapshot_id: int
    'Iceberg snapshot of user_history_asof_hour used for features.'
    scored_at: datetime.datetime

class Baseline_scores(pydantic.BaseModel):
    """Wikimedia revert-risk predictions for article edits."""
    wiki_id: str
    rev_id: int
    model_name: str
    'e.g. revertrisk-language-agnostic.'
    model_version: str
    probability_true: float
    'Predicted probability the edit is reverted.'
