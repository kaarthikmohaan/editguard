# Data dictionary

Contract tables are generated from `contracts/edits.odcs.yaml` (regenerate with `make docs`). Silver and gold tables are defined in the dbt project and described below.

## bronze.edits

Edit events as received, one row per upstream event.

| Field | Type | Required | Key | Meaning |
| --- | --- | --- | --- | --- |
| event_id | string | yes | PK | Upstream meta.id (UUID). Dedup key. |
| event_time | timestamp | yes |  | revision.rev_dt, when the edit happened. Event time for watermarks. |
| emitted_at | timestamp | yes |  | meta.dt, when Wikimedia emitted the event. Used for lag. |
| ingested_at | timestamp | yes |  | When the producer received the event. |
| wiki_id | string | yes |  | Wiki database name. |
| page_id | bigint | yes |  |  |
| page_title | string | yes |  |  |
| namespace_id | int | yes |  | 0 = article namespace. |
| page_change_kind | string | yes |  | One of edit, create, move, delete, undelete, visibility_change. |
| rev_id | bigint | yes |  | Revision ID. Unique within a wiki, not per page. |
| rev_parent_id | bigint | no |  |  |
| rev_size | bigint | yes |  | Size of the new revision in bytes. |
| prior_rev_size | bigint | no |  | prior_state.revision.rev_size. Null for page creations. |
| is_minor_edit | boolean | yes |  |  |
| comment | string | no |  | Edit summary. Hidden when is_comment_visible is false. |
| is_content_visible | boolean | yes |  |  |
| is_comment_visible | boolean | yes |  |  |
| is_editor_visible | boolean | yes |  |  |
| performer_user_text | string | no |  | Username. Bronze only; hashed in silver and gold. |
| performer_is_bot | boolean | yes |  |  |
| performer_is_temp | boolean | yes |  | Temporary account (logged-out editor). |
| performer_groups | array<string> | no |  |  |
| performer_edit_count | bigint | no |  | Editor's edit count at the time of the edit. |
| performer_registration_dt | timestamp | no |  | Account registration time. Null for about 0.9% of events. |
| revert_method | string | no |  | rollback, undo or manual when this edit is a revert; else null. |
| rev_reverted_oldest_id | bigint | no |  |  |
| rev_reverted_newest_id | bigint | no |  |  |
| rev_original_id | bigint | no |  | Revision restored by this revert. |
| upstream_topic | string | yes |  |  |
| upstream_partition | int | yes |  |  |
| upstream_offset | bigint | yes |  | Used by the gap detector. |
| schema_version | string | yes |  | Upstream $schema, e.g. /mediawiki/page/change/1.12.0. |
| raw_json | string | yes |  | Full upstream event, kept for reprocessing. Purged for suppressed revisions. |

## edits.flagged

Edits whose score passed the flag threshold.

| Field | Type | Required | Key | Meaning |
| --- | --- | --- | --- | --- |
| wiki_id | string | yes | PK |  |
| rev_id | bigint | yes | PK |  |
| page_title | string | yes |  |  |
| event_time | timestamp | yes |  |  |
| score | double | yes |  | 0 to 1; higher means more likely damaging. |
| score_version | string | yes |  |  |
| history_snapshot_id | bigint | yes |  | Iceberg snapshot of user_history_asof_hour used for features. |
| scored_at | timestamp | yes |  |  |

## silver.baseline_scores

Wikimedia revert-risk predictions for article edits.

| Field | Type | Required | Key | Meaning |
| --- | --- | --- | --- | --- |
| wiki_id | string | yes | PK |  |
| rev_id | bigint | yes | PK |  |
| model_name | string | yes |  | e.g. revertrisk-language-agnostic. |
| model_version | string | yes |  |  |
| probability_true | double | yes |  | Predicted probability the edit is reverted. |
## silver.edits

Deduplicated union of `bronze.edits` and `bronze.edits_replay`, article-namespace edits only. Same fields as `bronze.edits` except that `performer_user_text` and `raw_json` (which also holds the username) are dropped, and:

| Field | Type | Meaning |
| --- | --- | --- |
| user_hash | string | Salted SHA-256 (64 lowercase hex characters) of `performer_user_text`; null when the username is null. Replaces the username. |
| byte_delta | bigint | `rev_size - prior_rev_size`. |
| source | string | `live` or `replay`. |
| is_scored_population | boolean | Non-bot article edit on a target wiki. |

## silver.labels

| Field | Type | Meaning |
| --- | --- | --- |
| wiki_id | string | Wiki database name. |
| rev_id | bigint | The labelled edit. |
| label | string | `damaging`, `bot_caught`, `ok` or `label_unknown`. |
| reverting_rev_id | bigint | Revert that covered this edit, if any. |
| revert_method | string | rollback, undo or manual. |
| reverter_is_bot | boolean | Whether the reverting account is a bot. |
| minutes_to_revert | double | Time from edit to revert. |
| label_final_at | timestamp | event_time + 48 h; label frozen after this. |

## silver.user_history_asof_hour

| Field | Type | Meaning |
| --- | --- | --- |
| wiki_id | string | Wiki database name. |
| user_hash | string | Hashed editor. |
| asof_hour | timestamp | History known at the end of this hour. |
| reverts_received_30d | int | Damaging labels in the 30 days before `asof_hour`. |
| edits_30d | int | Edits in the 30 days before `asof_hour`. |

Features for an edit at hour H read the row for H − 1.

## gold.fact_edit

| Field | Type | Meaning |
| --- | --- | --- |
| wiki_id, rev_id | string, bigint | Primary key. |
| date_key | int | YYYYMMDD of event_time; joins `dim_date`. |
| user_hash | string | Joins `dim_user_hashed`. |
| event_time | timestamp | When the edit happened. |
| page_id | bigint | Page. |
| byte_delta | bigint | Size change. |
| is_minor_edit | boolean | Minor edit flag. |
| is_temp_at_edit | boolean | Temporary account at edit time. |
| edit_count_at_edit | bigint | Editor's edit count at edit time. |
| account_age_days_at_edit | int | Days since registration at edit time; null if unknown. |

## gold.fact_score

| Field | Type | Meaning |
| --- | --- | --- |
| wiki_id, rev_id, score_version | string, bigint, string | Primary key. |
| score | double | EditGuard score. |
| flagged | boolean | Score above the threshold for that version. |
| llm_label | string | LLM category, if enriched. |
| llm_model_id, llm_prompt_version | string | Reproducibility. |
| scored_at | timestamp | When scored. |
| source | string | `live` or `replay`. |

## gold.fact_label, gold.fact_baseline

Frozen copies of `silver.labels` and `silver.baseline_scores` for the edits in `fact_edit`.

## gold.dim_wiki, gold.dim_date, gold.dim_user_hashed

| Table | Fields |
| --- | --- |
| dim_wiki | wiki_id, language, language_group (`english`, `indian`) |
| dim_date | date_key, date, day_of_week, is_weekend |
| dim_user_hashed | user_hash, is_temp, is_registered, first_seen, current_groups |

## pg.flags (Postgres)

| Field | Type | Meaning |
| --- | --- | --- |
| wiki_id, rev_id | text, bigint | Primary key. |
| page_title | text | Title for display. |
| score, score_version | double, text | From `edits.flagged`. |
| llm_label, llm_reason | text | LLM output; null if skipped. |
| enrichment | text | `done`, `skipped` or `pending`. |
| model_id, prompt_version | text | Reproducibility. |
| hidden | boolean | True once the revision is suppressed. |
| feedback | text | Reviewer verdict: correct, wrong, unsure. |
| flagged_at, updated_at | timestamptz | Timestamps. |
