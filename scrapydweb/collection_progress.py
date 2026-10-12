"""Read-only progress from native job snapshots; ETA is historical, never completion evidence."""
from datetime import datetime
from statistics import median


def summarize(job, durations, now=None):
    now = now or datetime.now()
    state = {'0': 'queued', '1': 'running', '2': 'finished'}.get(job.status, 'unavailable')
    stale = state in ('queued', 'running') and (job.update_time is None or (now - job.update_time).total_seconds() > 120 or job.update_time > now)
    elapsed = max(0, int((now - job.start).total_seconds())) if job.start else None
    result = dict(state=state, stale=stale, pages=job.pages, items=job.items, elapsed_seconds=elapsed,
                  percent=None, eta_seconds=None, estimate_basis=None, sample_count=0)
    durations = [d for d in durations if isinstance(d, (int, float)) and 0 < d < 7 * 86400]
    if state != 'running' or stale or elapsed is None or len(durations) < 3:
        return result
    if max(durations) > 2 * min(durations):
        return result
    expected = median(durations)
    result['sample_count'] = len(durations)
    if elapsed >= expected:
        return result  # Do not keep a fictional countdown at zero during an overrun.
    result.update(percent=min(95, int(100 * elapsed / expected)), eta_seconds=int(expected - elapsed),
                  estimate_basis='historical_duration')
    return result


def progress(collection, company, revision):
    from .models import CollectionTask, Task, TaskResult, TaskJobResult
    from .vars import jobs_table_map
    binding = CollectionTask.query.filter_by(collection_id=collection, company_id=company, revision=revision).first()
    if not binding or not binding.task_id:
        return dict(state='not_scheduled')
    task = Task.query.get(binding.task_id)
    if not task:
        return dict(state='unavailable')
    # Only this immutable Timer Task revision, project, spider and node are comparable.
    executions = (TaskJobResult.query.join(TaskResult).filter(TaskResult.task_id == task.id)
                  .order_by(TaskJobResult.id.desc()).limit(20).all())
    if not executions:
        return dict(state='waiting')
    latest = executions[0]
    if latest.status != 'ok':
        return dict(state='submission_unconfirmed')
    Job = jobs_table_map.get(latest.node)
    if Job is None:
        return dict(state='unavailable')
    job = Job.query.filter_by(project=task.project, spider=task.spider, job=latest.result).first()
    if not job:
        return dict(state='unavailable')
    durations = []
    for previous in executions[1:]:
        if previous.node != latest.node or previous.status != 'ok':
            continue
        old = Job.query.filter_by(project=task.project, spider=task.spider, job=previous.result, status='2').first()
        if old and old.start and old.finish and old.items is not None and old.items > 0:
            durations.append((old.finish - old.start).total_seconds())
        if len(durations) == 5:
            break
    return summarize(job, durations)
