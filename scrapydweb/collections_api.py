"""Piwigo owns collection intent; native Timer Tasks own scheduling and job history."""
import fcntl
import hashlib
import hmac
import json
import re
import threading
from contextlib import contextmanager
from datetime import datetime

import requests
from apscheduler.triggers.cron import CronTrigger
from flask import Blueprint, current_app, jsonify, request

from .models import CollectionTask, Task, db

bp = Blueprint('collections_api', __name__)
_mutex = threading.RLock()


@contextmanager
def locked():
    # Same shared data path for every API worker. APScheduler remains in its native owner process.
    path = current_app.config.get('COLLECTIONS_LOCK_PATH')
    if not path:
        raise RuntimeError('Collection integration is not configured')
    with _mutex, open(path, 'a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def validate(data):
    keys = {'company_id', 'revision', 'state', 'domain_id', 'retailer_company_id', 'coverage',
            'store_id', 'frequency', 'starts_on', 'ends_on', 'quality_revision', 'fulfillment_scope'}
    if not isinstance(data, dict) or not keys <= set(data) or set(data) - keys - {'schedule', 'period'}:
        raise ValueError('Invalid collection document')
    for key in ('company_id', 'revision', 'domain_id', 'retailer_company_id', 'quality_revision'):
        if type(data[key]) is not int or not 0 < data[key] <= 2147483647:
            raise ValueError('Invalid identifier')
    if data['state'] not in ('reserved', 'draft', 'archived') or data['coverage'] not in ('national', 'store'):
        raise ValueError('Invalid collection state')
    if data['frequency'] not in ('daily', 'weekly', 'monthly', 'custom'):
        raise ValueError('Invalid frequency')
    if data['frequency'] == 'custom' and 'schedule' not in data:
        raise ValueError('Missing schedule')
    if 'schedule' in data:
        fields = {'year', 'month', 'day', 'week', 'day_of_week', 'hour', 'minute', 'second', 'timezone'}
        if not isinstance(data['schedule'], dict) or set(data['schedule']) != fields or not all(isinstance(v, str) and 0 < len(v) <= 100 for v in data['schedule'].values()):
            raise ValueError('Invalid schedule')
        CronTrigger(**data['schedule'])
    if 'period' in data:
        period=data['period']
        if not isinstance(period, dict) or set(period) != {'start_date', 'end_date'}:
            raise ValueError('Invalid contract period')
        start=datetime.fromisoformat(period['start_date'])
        end=datetime.fromisoformat(period['end_date'])
        if start.tzinfo is None or end.tzinfo is None or end < start:
            raise ValueError('Invalid contract period')
    for key in ('starts_on', 'ends_on'):
        if not isinstance(data[key], str) or datetime.strptime(data[key], '%Y-%m-%d').strftime('%Y-%m-%d') != data[key]:
            raise ValueError('Invalid dates')
    if data['ends_on'] < data['starts_on']:
        raise ValueError('Invalid dates')
    if data['coverage'] == 'national' and data['store_id'] is not None:
        raise ValueError('Unexpected store')
    if data['coverage'] == 'store' and (not isinstance(data['store_id'], str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', data['store_id'])):
        raise ValueError('Invalid store')
    if data['fulfillment_scope'] not in ('1P', '3P', 'both'):
        raise ValueError('Invalid fulfillment scope')
    return data


def schedule_spec(data, collection):
    route = resolve_source(collection, data)
    if not route or route.get('retailer_company_id') != data['retailer_company_id'] or data['company_id'] not in route.get('client_company_ids', []):
        raise ValueError('No qualified route')
    if data['fulfillment_scope'] not in route.get('fulfillment_scopes', []):
        raise ValueError('Unqualified fulfillment scope')
    for key in ('project', 'version', 'spider'):
        if not isinstance(route.get(key), str) or not re.fullmatch(r'[a-zA-Z0-9_.-]{1,120}', route[key]):
            raise ValueError('Invalid spider route')
    node = route.get('node')
    if type(node) is not int or not 1 <= node <= len(current_app.config.get('SCRAPYD_SERVERS', [])):
        raise ValueError('Invalid worker node')
    args = {'setting': []}
    fixed = route.get('fixed_arguments', {})
    if not isinstance(fixed, dict):
        raise ValueError('Invalid fixed arguments')
    for key, value in fixed.items():
        if not isinstance(key, str) or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{0,63}', key) or key in ('project', 'spider', 'jobid', 'setting', 'priority'):
            raise ValueError('Invalid fixed argument')
        if not isinstance(value, str) or len(value) > 512 or '\x00' in value:
            raise ValueError('Invalid fixed argument value')
        args[key] = value
    if data['coverage'] == 'store':
        key = route.get('store_argument', '')
        if not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{0,63}', key) or key in ('project', 'spider', 'jobid', 'setting', 'priority'):
            raise ValueError('Unqualified store argument')
        if key in fixed:
            raise ValueError('Fixed arguments cannot override the selected store')
        allowed = route.get('allowed_store_ids')
        if allowed is not None and (not isinstance(allowed, list) or not all(isinstance(value, str) for value in allowed) or data['store_id'] not in allowed):
            raise ValueError('Store outside pilot scope')
        args[key] = data['store_id']
    hour = current_app.config.get('COLLECTIONS_HOUR', 6)
    zone = current_app.config.get('COLLECTIONS_TIMEZONE', 'Europe/Paris')
    if type(hour) is not int or not 0 <= hour <= 23:
        raise ValueError('Invalid hour')
    start = datetime.strptime(data['starts_on'], '%Y-%m-%d')
    cron = dict(year='*', month='*', day='*', week='*', day_of_week='*', hour=str(hour), minute='0', second='0',
                start_date=data['starts_on'] + ' 00:00:00', end_date=data['ends_on'] + ' 23:59:59', timezone=zone, jitter=0)
    if 'schedule' in data:
        cron.update(data['schedule'])
    elif data['frequency'] == 'weekly':
        cron['day_of_week'] = str(start.weekday())
    elif data['frequency'] == 'monthly':
        # Explicitly reject ambiguous short-month anchors until a product rule is approved.
        if start.day > 28:
            raise ValueError('Monthly starts after day 28 require review')
        cron['day'] = str(start.day)
    if 'period' in data:
        cron.update(data['period'])
    CronTrigger(**cron)  # Validate before any mutation.
    return route, args, cron


def gate_response(collection, data, resolve=False):
    url = current_app.config.get('COLLECTIONS_GATE_URL', '')
    token = current_app.config.get('COLLECTIONS_GATE_TOKEN', '')
    schemes = ('https://', 'http://') if current_app.config.get('COLLECTIONS_ALLOW_HTTP') is True else ('https://',)
    if not url.startswith(schemes) or not token:
        return {}
    digest = hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    try:
        response = requests.post(url, json={'collection_id': collection, 'company_id': data['company_id'],
                                           'revision': data['revision'], 'digest': digest, **({'resolve_source': True} if resolve else {})},
                                 headers={'Authorization': 'Bearer ' + token}, timeout=(3, 8), allow_redirects=False)
        result = response.json() if response.status_code == 200 else {}
        return result if isinstance(result, dict) else {}
    except (requests.RequestException, ValueError):
        return {}


def approved(collection, data):
    return gate_response(collection, data) == {'allowed': True}


def resolve_source(collection, data):
    result = gate_response(collection, data, resolve=True)
    if result.get('allowed') is not True or not isinstance(result.get('route'), dict):
        raise ValueError('No authorized database source')
    return result['route']


def collection_today():
    zone = CronTrigger(timezone=current_app.config.get('COLLECTIONS_TIMEZONE', 'Europe/Paris')).timezone
    return datetime.now(zone).strftime('%Y-%m-%d')


def task_matches(task, spec):
    route, args, cron = spec
    if not task or (task.project, task.version, task.spider) != (route['project'], route['version'], route['spider']):
        return False
    if json.loads(task.settings_arguments) != args or json.loads(task.selected_nodes) != [route['node']]:
        return False
    return all(str(getattr(task, key)) == str(value) for key, value in cron.items())


def task_allowed(task_id):
    binding = CollectionTask.query.filter_by(task_id=task_id).first()
    if not binding:
        return True  # Preserve unmanaged native tasks.
    latest = CollectionTask.query.filter_by(collection_id=binding.collection_id).order_by(CollectionTask.revision.desc()).first()
    data = json.loads(binding.document)
    today = (datetime.now(CronTrigger(timezone=data['schedule']['timezone']).timezone).strftime('%Y-%m-%d')
             if 'schedule' in data else collection_today())
    if latest.revision != binding.revision or data['state'] != 'reserved' or not data['starts_on'] <= today <= data['ends_on']:
        return False
    try:
        spec = schedule_spec(data, binding.collection_id)
        if not task_matches(Task.query.get(task_id), spec):
            return False
        return approved(binding.collection_id, data)
    except (ValueError, TypeError, KeyError):
        return False


def execute_collection(task_id):
    from .utils.scheduler import scheduler
    from .views.operations.execute_task import execute_task
    with db.app.app_context(), locked():
        if not task_allowed(task_id):
            job = scheduler.get_job(str(task_id))
            if job:
                job.pause()
            return
        # Native TaskResult / TaskJobResult history, with no blind retry on uncertain submission.
        execute_task(task_id, allow_retry=False)


def apply(collection, data, scheduler):
    document = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    with locked():
        latest = CollectionTask.query.filter_by(collection_id=collection).order_by(CollectionTask.revision.desc()).first()
        if latest and (latest.company_id != data['company_id'] or latest.revision > data['revision']
                       or (latest.revision == data['revision'] and latest.document != document)):
            return {'error': 'revision_conflict'}, 409
        spec = schedule_spec(data, collection) if data['state'] == 'reserved' else None
        if spec and latest and latest.revision == data['revision'] and not task_matches(Task.query.get(latest.task_id), spec):
            return {'error': 'native_task_drift'}, 409
        if spec and not approved(collection, data):
            return {'error': 'reservation_not_authorized'}, 409
        # Pause every older managed revision. Unmanaged Timer Tasks are never selected.
        for previous in CollectionTask.query.filter_by(collection_id=collection).all():
            if previous.task_id and previous.revision != data['revision']:
                job = scheduler.get_job(str(previous.task_id))
                if job:
                    job.pause()
        binding = latest if latest and latest.revision == data['revision'] else None
        if binding is None:
            binding = CollectionTask(collection_id=collection, revision=data['revision'], company_id=data['company_id'], document=document)
            if spec:
                route, args, cron = spec
                task = Task(name='Piwigo %s r%s' % (collection, data['revision']), trigger='cron',
                            project=route['project'], version=route['version'], spider=route['spider'], jobid='',
                            settings_arguments=json.dumps(args), selected_nodes=json.dumps([route['node']]),
                            coalesce='True', max_instances=1, misfire_grace_time=600, **cron)
                db.session.add(task)
                db.session.flush()
                binding.task_id = task.id
            db.session.add(binding)
            db.session.commit()  # Durable identity before APScheduler creation; retries reuse the same task.
        if spec:
            job = scheduler.get_job(str(binding.task_id))
            if job is None:
                # Create paused, then resume; never fire immediately at sync time.
                _, _, cron = spec
                scheduler.add_job(execute_collection, trigger='cron', id=str(binding.task_id),
                                  name='Piwigo %s r%s' % (collection, data['revision']),
                                  kwargs={'task_id': binding.task_id}, next_run_time=None,
                                  coalesce=True, max_instances=1, misfire_grace_time=600, **cron)
            # A matching replay keeps an existing next run unchanged; resume only if paused.
            job = scheduler.get_job(str(binding.task_id))
            if job and job.next_run_time is None:
                job.resume()
        job = scheduler.get_job(str(binding.task_id)) if spec else None
        state = ('scheduled' if job and job.next_run_time is not None else 'no_future_occurrence') if spec else 'paused'
        return {'collection_id': collection, 'revision': data['revision'], 'task_id': binding.task_id, 'state': state}, 200


@bp.route('/collections/v1/<collection>/', methods=['PUT'])
def synchronize(collection):
    token = current_app.config.get('COLLECTIONS_API_TOKEN', '')
    if not token or not hmac.compare_digest(request.headers.get('X-Piwigo-Token', ''), token):
        return jsonify(error='forbidden'), 403
    if not re.fullmatch(r'[a-f0-9]{32}', collection) or request.content_length is None or request.content_length > 8192:
        return jsonify(error='invalid_request'), 400
    try:
        data = validate(request.get_json(force=False))
        from .utils.scheduler import scheduler
        result, status = apply(collection, data, scheduler)
        return jsonify(result), status
    except (ValueError, TypeError, KeyError):
        db.session.rollback()
        return jsonify(error='invalid_collection'), 400
    except Exception:
        db.session.rollback()
        return jsonify(error='synchronization_unavailable'), 503


@bp.route('/collections/v1/<collection>/check/', methods=['POST'])
def preflight(collection):
    """Validate both directions without inserting a Task or modifying APScheduler."""
    token = current_app.config.get('COLLECTIONS_API_TOKEN', '')
    if not token or not hmac.compare_digest(request.headers.get('X-Piwigo-Token', ''), token):
        return jsonify(error='forbidden'), 403
    if not re.fullmatch(r'[a-f0-9]{32}', collection) or request.content_length is None or request.content_length > 8192:
        return jsonify(error='invalid_request'), 400
    try:
        data = validate(request.get_json(force=False))
        latest = CollectionTask.query.filter_by(collection_id=collection).order_by(CollectionTask.revision.desc()).first()
        document = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        if latest and (latest.company_id != data['company_id'] or latest.revision > data['revision']
                       or (latest.revision == data['revision'] and latest.document != document)):
            return jsonify(error='revision_conflict'), 409
        if not current_app.config.get('COLLECTIONS_LOCK_PATH'):
            return jsonify(error='integration_not_configured'), 503
        if data['state'] == 'reserved':
            spec = schedule_spec(data, collection)
            if latest and latest.revision == data['revision'] and not task_matches(Task.query.get(latest.task_id), spec):
                return jsonify(error='native_task_drift'), 409
            if not approved(collection, data):
                return jsonify(error='reservation_not_authorized'), 409
        return jsonify(collection_id=collection, revision=data['revision'], state='qualified', task_id=None), 200
    except (ValueError, TypeError, KeyError):
        return jsonify(error='invalid_collection'), 400
    except Exception:
        return jsonify(error='preflight_unavailable'), 503


@bp.route('/collections/v1/<collection>/progress/', methods=['GET'])
def collection_progress(collection):
    token = current_app.config.get('COLLECTIONS_API_TOKEN', '')
    if not token or not hmac.compare_digest(request.headers.get('X-Piwigo-Token', ''), token):
        return jsonify(error='forbidden'), 403
    if not re.fullmatch(r'[a-f0-9]{32}', collection):
        return jsonify(error='invalid_request'), 400
    company = request.args.get('company_id', type=int)
    revision = request.args.get('revision', type=int)
    if not company or company < 1 or not revision or revision < 1:
        return jsonify(error='invalid_request'), 400
    try:
        from .collection_progress import progress
        return jsonify(collection_id=collection, company_id=company, revision=revision,
                       progress=progress(collection, company, revision))
    except Exception:
        return jsonify(error='progress_unavailable'), 503


@bp.after_request
def no_collection_cache(response):
    response.headers['Cache-Control'] = 'private, no-store'
    return response
