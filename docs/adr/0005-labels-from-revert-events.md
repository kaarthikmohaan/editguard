# 0005. Labels from page_change revert events, scoped to the page

- Status: accepted
- Date: 2026-09-25

## Context
Page-change schema 1.12.0 carries `revision.revert` with method and the reverted revision ID range, plus the reverter's bot flag. Revision IDs are unique across a whole wiki, not per page.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| `mw-reverted` tag via API | Simple | Rate limits; doesn't say who reverted |
| Revert events from the stream, range only | No API calls | Mislabels edits on other pages |
| Revert events, joined on (wiki_id, page_id) and ID range | Correct; no API calls | Needs the reverted edits in bronze |

## Decision
Join on (wiki_id, page_id) and `rev_id BETWEEN rev_reverted_oldest_id AND rev_reverted_newest_id`. Freeze labels at 48 hours. Edits before recording start are `label_unknown`.

## Consequences
- A dbt test asserts reverted and reverting edits share a page.
- 50 cases audited by hand; a revert is still only a proxy for damage.
