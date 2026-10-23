# Piwigo → native ScrapydWeb Timer Tasks

Local implementation, not deployed. `PUT /collections/v1/<collection_id>/`
creates a native Task and an APScheduler cron job. There is no second PHP scheduler.
Authentication requires `X-Piwigo-Token` and the existing HTTP Basic credentials
when `ENABLE_AUTH` is set. Publish behind HTTPS. Empty token disables this API.

Configure in server settings, with secrets outside Git:

- `COLLECTIONS_API_TOKEN`: dedicated inbound token.
- `COLLECTIONS_GATE_URL`: HTTPS Piwigo `/data_collections_gate.php` endpoint.
- `COLLECTIONS_GATE_TOKEN`: different token for execution authorization.
- `COLLECTIONS_LOCK_PATH`: writable file in shared persistent data directory.
- Source mappings live in Piwigo's `data_collection_sources` database table.
  The authenticated gate resolves the source only after checking the collection
  digest, revision, tenant, rights and scope. No file route fallback is used.
  The mapping is re-read before every managed task dispatch; changed targets,
  disabled sources and unavailable gates fail closed.
- `COLLECTIONS_TIMEZONE` / `COLLECTIONS_HOUR`: defaults Europe/Paris and 06:00,
  operational assumptions to validate before enabling a pilot.

Daily runs each day, weekly runs on the start-date weekday, monthly on the
start-date day. Monthly starts after the 28th are refused pending an explicit
short-month rule. Inclusive end date, no immediate fire, native coalescing and
600-second misfire grace. Existing unmanaged tasks retain their behavior.

`CollectionTask` is a new mapping table created through the existing `db.create_all`
startup mechanism. Review and back up the task database before deployment.
A collection/revision pair has one immutable payload and at most one native task.
A new revision pauses older managed tasks and creates a new task if reserved;
a draft/archive revision records a paused tombstone. Old native execution history
remains associated with its immutable collection revision. Replay and recovery
reuse the task after an APScheduler failure. A conflicting or stale revision is
rejected. This API does not delete unmanaged tasks or enable the global scheduler.

Before actual dispatch, Piwigo must approve the current payload digest, company,
revision, license, author rights, scope, quality commitment and remaining capacity.
Unavailable Piwigo fails closed. Managed native task dispatch also verifies dates,
spider, version, node and arguments against the qualified route. This applies to
normal Timer Tasks and the native schedule/task route. Direct staff operations on
unmanaged spiders remain governed by existing ScrapydWeb permissions.

Release prevents future launches; it does not cancel an already submitted job.
Changing frequency means release → edit → reserve, producing a new Timer Task
revision. Native TaskResult/TaskJobResult keep job history. Managed tasks disable
blind retries after an uncertain schedule response; investigate such cases before
manual replay. A finished job is not evidence of successful ingestion or export.

The API and executor share an interprocess file lock. Use the existing single
APScheduler owner process; this change does not make multi-owner scheduling safe.
Timer-task edits in ScrapydWeb can create drift; qualified-route and Piwigo gates
refuse changed targets. Prefer editing managed collection intent in Piwigo.
Dispatch, preflight and same-revision synchronization also reject changes to
native cron fields, including frequency, time, timezone and date bounds.

Tests: `python -m pytest -q rs_tests/test_collections_api.py`. The suite loads the
real models and APScheduler with SQLite/memory stores, bypasses application startup,
and mocks gate HTTP and final crawl dispatch. No shared DB, real worker or email.
Full legacy startup, real HTTPS/basic authentication, actual crawl/ingestion and
cross-process concurrency remain staging acceptance work.

With the isolated PHP fixture mounted in `piwigo-dev`, run
`COLLECTION_PHP_CONTRACT=1 /tmp/scrapydweb-collectes-venv/bin/python -m pytest -q rs_tests/test_collections_api.py`
to include the real PHP service contract. This checks Salon daily M reservation,
canonical payload digest agreement, idempotent native task creation and immediate
gate revocation after PHP release, before pause synchronization. Both databases
are in memory. Gate transport uses fixture CLI RPC, not HTTPS; final dispatch
remains mocked. The default test run skips this optional cross-runtime case.

Read-only qualification: `POST /collections/v1/<collection_id>/check/` uses the
same dedicated token and existing Basic authentication. It validates the payload,
revision, declared route, cron and Piwigo gate without creating Task/CollectionTask
rows or APScheduler jobs. A `qualified` response does not establish deployed spider
availability, global scheduler readiness, network access to Scrapyd, or ingestion.
Piwigo now requires `--company=ID` for both `--check` and `--apply`.


Salon-de-Provence pilot: the Piwigo sizing decision is daily M. The qualified
source selector is `store_ids=161301-161301` on
`leclerc_drive_fr_stores_pagelist`, with server-owned
`fixed_arguments={"full_catalog": "true"}` and
`allowed_store_ids=["161301-161301"]`. Neither setting is supplied by the client.
Other stores are refused on this pilot route. Fixed arguments cannot override
store selection or reserved Scrapyd fields. The route still needs the verified
node, deployed version, domain ID and client ID before installation. Source review
is not worker deployment or catalogue completeness evidence.

Progress: `GET /collections/v1/<id>/progress/?company_id=…&revision=…` reads only
native TaskResult/TaskJobResult and node job snapshots. It exposes state and safe
counters, never worker URLs or logs. Historical ETA requires 3–5 finished runs with
items on the exact immutable task revision/node; median duration, max/min <= 2,
current snapshot <= 120 seconds old. Percent is elapsed/median (estimated, capped
at 95), not measured work completion. Overruns and insufficient/stale evidence
suppress ETA. A finished crawl does not prove ingestion or delivery. Native job
snapshot collection must be enabled for runtime evidence; this API never starts it.

## Spider version policy

Collection Timer Tasks always use ScrapydWeb `default: the latest version`. The worker resolves the latest deployed project version at each scheduled run; a historical source version must not pin new tasks. Existing pinned tasks require a scoped migration together with connector deployment, preserving task IDs, arguments and schedules.
