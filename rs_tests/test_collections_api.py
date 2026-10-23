"""Isolated SQLite + native APScheduler; never boot app config, workers or networking."""
import importlib
import json
import sys
import types
from pathlib import Path

import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

# Avoid scrapydweb.__init__ side effects (real DB/config/scheduler startup).
package = types.ModuleType('scrapydweb')
package.__path__ = [str(Path(__file__).parents[1] / 'scrapydweb')]
sys.modules['scrapydweb'] = package
varmod = types.ModuleType('scrapydweb.vars'); varmod.STATE_RUNNING = 1
sys.modules['scrapydweb.vars'] = varmod
models = importlib.import_module('scrapydweb.models')
JobFixture = models.create_jobs_table('synthetic_jobs')
varmod.jobs_table_map = {1: JobFixture}
api = importlib.import_module('scrapydweb.collections_api')


@pytest.fixture
def env(tmp_path, monkeypatch):
    app = Flask(__name__)
    app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', SQLALCHEMY_TRACK_MODIFICATIONS=False,
                      SQLALCHEMY_BINDS={'metadata': 'sqlite://', 'jobs': 'sqlite://'},
                      COLLECTIONS_API_TOKEN='synthetic-token', COLLECTIONS_LOCK_PATH=str(tmp_path / 'lock'),
                      SCRAPYD_SERVERS=['synthetic-worker'], TEST_SOURCE_ROWS={'1:national': dict(
                          project='retail_shake', version='fixture1', spider='synthetic', node=1,
                          retailer_company_id=101, client_company_ids=[10], fulfillment_scopes=['both'])})
    models.db.init_app(app); models.db.app = app
    scheduler = BackgroundScheduler()
    scheduler.start(paused=True)
    mod = types.ModuleType('scrapydweb.utils.scheduler'); mod.scheduler = scheduler
    monkeypatch.setitem(sys.modules, 'scrapydweb.utils.scheduler', mod)
    monkeypatch.setattr(api, 'approved', lambda *args: True)
    monkeypatch.setattr(api, 'resolve_source', lambda collection, data: app.config['TEST_SOURCE_ROWS'].get('%s:%s' % (data['domain_id'], data['coverage'])))
    monkeypatch.setattr(api, 'collection_today', lambda: '2099-01-01')
    app.register_blueprint(api.bp)
    with app.app_context():
        models.db.create_all()
        yield app.test_client(), scheduler
        models.db.session.remove(); models.db.drop_all()
    scheduler.shutdown(wait=False)


def payload(**kwargs):
    return dict(company_id=10, revision=2, state='reserved', domain_id=1, retailer_company_id=101,
                coverage='national', store_id=None, frequency='weekly', starts_on='2099-01-01',
                ends_on='2099-02-28', quality_revision=3, fulfillment_scope='both', **kwargs)


def put(client, data, collection='a' * 32, token='synthetic-token'):
    return client.put('/collections/v1/%s/' % collection, json=data, headers={'X-Piwigo-Token': token})


def test_native_task_lifecycle(env):
    client, scheduler = env
    data = payload()
    assert put(client, data, token='bad').status_code == 403
    result = put(client, data)
    assert result.status_code == 200
    task_id = result.json['task_id']
    task = models.Task.query.get(task_id)
    assert task.project == 'retail_shake' and json.loads(task.selected_nodes) == [1]
    job = scheduler.get_job(str(task_id))
    assert job.func == api.execute_collection
    assert str(job.trigger.timezone) == 'Europe/Paris'
    next_run = job.next_run_time
    assert put(client, data).json['task_id'] == task_id
    assert models.Task.query.count() == 1 and job.next_run_time == next_run
    assert put(client, dict(data, company_id=20)).status_code == 409
    assert put(client, dict(data, frequency='daily')).status_code == 409
    assert put(client, dict(data, revision=1)).status_code == 409
    paused = put(client, dict(data, revision=3, state='draft'))
    assert paused.json['state'] == 'paused'
    assert scheduler.get_job(str(task_id)).next_run_time is None
    assert put(client, data).status_code == 409
    resumed = put(client, dict(data, revision=4, frequency='daily'))
    assert resumed.json['task_id'] != task_id
    assert models.CollectionTask.query.count() == 3
    assert scheduler.get_job(str(task_id)).next_run_time is None


def test_routes_gate_and_dates(env, monkeypatch):
    client, scheduler = env
    data = payload()
    for bad in [dict(data, company_id=20), dict(data, domain_id=2), dict(data, store_id='0007'),
                dict(data, ends_on='2000-01-01'), dict(data, revision=True), dict(data, frequency='hourly'),
                dict(data, frequency='monthly', starts_on='2099-01-31'), dict(data, extra='arbitrary')]:
        assert put(client, bad).status_code == 400
    monkeypatch.setattr(api, 'approved', lambda *args: False)
    assert put(client, data).status_code == 409
    assert models.Task.query.count() == 0


def test_retry_after_scheduler_failure_reuses_task(env, monkeypatch):
    client, scheduler = env
    original = scheduler.add_job
    def broken(*args, **kwargs):
        raise RuntimeError('synthetic failure')
    monkeypatch.setattr(scheduler, 'add_job', broken)
    assert put(client, payload()).status_code == 503
    assert models.Task.query.count() == 1
    monkeypatch.setattr(scheduler, 'add_job', original)
    assert put(client, payload()).status_code == 200
    assert models.Task.query.count() == 1 and len(scheduler.get_jobs()) == 1


def test_execution_gate_and_native_history_path(env, monkeypatch):
    client, scheduler = env
    calls = []
    mod = types.ModuleType('scrapydweb.views.operations.execute_task')
    mod.execute_task = lambda task_id, allow_retry: calls.append((task_id, allow_retry))
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    task_id = put(client, payload()).json['task_id']
    api.execute_collection(task_id)
    assert calls == [(task_id, False)]
    monkeypatch.setattr(api, 'approved', lambda *args: False)
    api.execute_collection(task_id)
    assert calls == [(task_id, False)]
    assert scheduler.get_job(str(task_id)).next_run_time is None


def test_managed_dispatch_rejects_tampering_and_out_of_period(env, monkeypatch):
    client, _ = env
    task_id = put(client, payload()).json['task_id']
    assert api.task_allowed(task_id)
    task = models.Task.query.get(task_id)
    task.spider = 'unapproved'
    models.db.session.commit()
    assert not api.task_allowed(task_id)
    task.spider = 'synthetic'
    models.db.session.commit()
    monkeypatch.setattr(api, 'collection_today', lambda: '2098-12-31')
    assert not api.task_allowed(task_id)
    monkeypatch.setattr(api, 'collection_today', lambda: '2099-03-01')
    assert not api.task_allowed(task_id)
    assert api.task_allowed(99999)  # Existing unmanaged task path is unaffected.


def test_preflight_does_not_create_tasks_or_change_scheduler(env, monkeypatch):
    client, scheduler = env
    url = '/collections/v1/' + 'a' * 32 + '/check/'
    assert client.post(url, json=payload()).status_code == 403
    response = client.post(url, json=payload(), headers={'X-Piwigo-Token': 'synthetic-token'})
    assert response.status_code == 200 and response.json['state'] == 'qualified'
    assert models.Task.query.count() == models.CollectionTask.query.count() == 0
    assert scheduler.get_jobs() == []
    monkeypatch.setattr(api, 'approved', lambda *args: False)
    assert client.post(url, json=payload(), headers={'X-Piwigo-Token': 'synthetic-token'}).status_code == 409
    assert models.Task.query.count() == 0 and scheduler.get_jobs() == []


def test_salon_pilot_store_route_and_fixed_catalogue_argument(env):
    client, scheduler = env
    from flask import current_app
    current_app.config['TEST_SOURCE_ROWS']['1:store'] = dict(
        project='retail_shake', version='fixture1', spider='leclerc_drive_fr_stores_pagelist', node=1,
        retailer_company_id=226, client_company_ids=[10], fulfillment_scopes=['1P'],
        store_argument='store_ids', allowed_store_ids=['161301-161301'], fixed_arguments={'full_catalog': 'true'})
    data = dict(payload(), retailer_company_id=226, coverage='store', store_id='161301-161301', frequency='daily', fulfillment_scope='1P')
    result = put(client, data)
    assert result.status_code == 200
    task = models.Task.query.get(result.json['task_id'])
    assert json.loads(task.settings_arguments) == {'setting': [], 'store_ids': '161301-161301', 'full_catalog': 'true'}
    assert task.spider == 'leclerc_drive_fr_stores_pagelist'
    assert put(client, dict(data, store_id='056801-056801'), collection='b' * 32).status_code == 400
    current_app.config['TEST_SOURCE_ROWS']['1:store']['fixed_arguments']['store_ids'] = 'other'
    assert put(client, data).status_code == 400
    assert models.Task.query.count() == 1


def test_progress_eta_suppresses_insufficient_stale_or_variable_samples():
    from datetime import datetime, timedelta
    from types import SimpleNamespace
    from scrapydweb.collection_progress import summarize
    now = datetime(2099, 1, 1, 6, 30)
    job = SimpleNamespace(status='1', update_time=now, start=now-timedelta(seconds=300), pages=12, items=120)
    result = summarize(job, [590, 600, 610], now)
    assert result['percent'] == 50 and result['eta_seconds'] == 300
    assert summarize(job, [600, 610], now)['eta_seconds'] is None
    assert summarize(job, [60, 600, 610], now)['eta_seconds'] is None
    assert summarize(job, [100, 100, 100], now)['percent'] is None
    job.update_time = now-timedelta(seconds=121)
    assert summarize(job, [600]*3, now)['stale'] is True
    assert summarize(job, [600]*3, now)['eta_seconds'] is None
    job.status = '2'
    assert summarize(job, [600]*3, now)['percent'] is None


def test_progress_endpoint_scopes_native_history(env):
    from datetime import datetime, timedelta
    client, _ = env
    task_id = put(client, payload()).json['task_id']
    now = datetime.now()
    for index in range(4):
        run = models.TaskResult(task_id=task_id)
        models.db.session.add(run); models.db.session.flush()
        models.db.session.add(models.TaskJobResult(task_result_id=run.id, node=1, server='synthetic', status_code=200, status='ok', result='job'+str(index)))
        models.db.session.add(JobFixture(project='retail_shake', spider='synthetic', job='job'+str(index), status='1' if index==3 else '2',
                                        start=now-timedelta(seconds=300 if index==3 else 1200),
                                        finish=None if index==3 else now-timedelta(seconds=600), update_time=now, items=120, pages=12))
    models.db.session.commit()
    url='/collections/v1/'+ 'a'*32 + '/progress/?company_id=10&revision=2'
    assert client.get(url).status_code == 403
    result=client.get(url, headers={'X-Piwigo-Token':'synthetic-token'}).json
    assert result['progress']['state']=='running' and result['progress']['percent']==50
    assert result['progress']['items']==120
    assert client.get(url.replace('company_id=10','company_id=20'), headers={'X-Piwigo-Token':'synthetic-token'}).json['progress']=={'state':'not_scheduled'}
    assert client.get(url.replace('revision=2','revision=4'), headers={'X-Piwigo-Token':'synthetic-token'}).json['progress']=={'state':'not_scheduled'}


def test_native_frequency_drift_cannot_bypass_collection_intent(env):
    client, _ = env
    task_id = put(client, payload()).json['task_id']
    task = models.Task.query.get(task_id)
    task.day_of_week = '*'
    models.db.session.commit()
    assert not api.task_allowed(task_id)
    response = put(client, payload())
    assert response.status_code == 409 and response.json['error'] == 'native_task_drift'
    response = client.post('/collections/v1/' + 'a' * 32 + '/check/', json=payload(),
                           headers={'X-Piwigo-Token': 'synthetic-token'})
    assert response.status_code == 409 and response.json['error'] == 'native_task_drift'


def test_real_php_reservation_native_task_and_revocation(env, monkeypatch):
    import os
    import subprocess
    if os.environ.get('COLLECTION_PHP_CONTRACT') != '1':
        pytest.skip('Set COLLECTION_PHP_CONTRACT=1 with isolated PHP fixture mounted in piwigo-dev')
    client, scheduler = env
    from flask import current_app
    current_app.config['TEST_SOURCE_ROWS']['4:store'] = dict(
        project='retail_shake', version='fixture1', spider='leclerc_drive_fr_stores_pagelist', node=1,
        retailer_company_id=226, client_company_ids=[10], fulfillment_scopes=['1P'],
        store_argument='store_ids', allowed_store_ids=['161301-161301'], fixed_arguments={'full_catalog':'true'})
    process = subprocess.Popen(['docker','exec','-i','-e','PHP_INI_SCAN_DIR=/tmp/collections-test-ini','-w','/app',
                                'piwigo-dev','php','tests/support/collection_bridge_contract.php'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    def rpc(data):
        process.stdin.write(json.dumps(data)+'\n'); process.stdin.flush()
        line=process.stdout.readline()
        assert line, 'PHP fixture ended unexpectedly'
        return json.loads(line)
    try:
        reservation=rpc({'action':'reserve'})
        data=reservation['payload']; collection=reservation['collection_id']
        import hashlib
        digest=hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        assert digest==reservation['digest'], 'PHP/Python canonical payload contract'
        def real_gate(collection, data):
            digest=hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
            return rpc(dict(action='gate',collection_id=collection,company_id=data['company_id'],revision=data['revision'],digest=digest))=={'allowed':True}
        monkeypatch.setattr(api,'approved',real_gate)
        monkeypatch.setattr(api,'collection_today',lambda:'2099-03-01')
        response=put(client,data,collection)
        assert response.status_code==200
        task_id=response.json['task_id']
        assert api.task_allowed(task_id)
        assert put(client,data,collection).json['task_id']==task_id
        released=rpc({'action':'release'})
        assert not api.task_allowed(task_id), 'PHP release blocks before timer synchronization'
        assert put(client,released['payload'],collection).json['state']=='paused'
        assert scheduler.get_job(str(task_id)).next_run_time is None
        assert models.Task.query.count()==1
    finally:
        process.stdin.close()
        process.wait(timeout=10)
        assert process.returncode==0, process.stderr.read()


def test_explicit_schedule_roundtrip_and_revision(env):
    client, scheduler = env
    schedule = dict(year='*', month='*', day='*', week='*', day_of_week='1,5', hour='8', minute='10', second='15', timezone='Europe/Paris')
    data = payload(schedule=schedule)
    api.validate(data)
    response = put(client, data)
    assert response.status_code == 200
    task_id = response.json['task_id']
    task = models.Task.query.get(task_id)
    assert all(str(getattr(task,k)) == v for k,v in schedule.items())
    data['revision'] += 1
    data['schedule']['minute'] = '25'
    response = put(client, data)
    assert response.status_code == 200
    assert str(models.Task.query.get(response.json['task_id']).minute) == '25'
    assert scheduler.get_job(str(task_id)) is None or scheduler.get_job(str(task_id)).next_run_time is None
    data['revision'] += 1
    data['schedule']['hour'] = '30'
    assert put(client, data).status_code >= 400


def test_contract_period_preserves_exact_cutoff(env):
    client, scheduler = env
    data=payload(period={'start_date':'2099-01-01T12:00:00+00:00','end_date':'2099-02-28T11:59:59+00:00'})
    api.validate(data)
    response=put(client,data)
    assert response.status_code == 200
    task=models.Task.query.get(response.json['task_id'])
    assert task.start_date == data['period']['start_date']
    assert task.end_date == data['period']['end_date']
    _,_,cron=api.schedule_spec(data, 'a' * 32)
    trigger=api.CronTrigger(**cron)
    assert trigger.end_date.isoformat() == data['period']['end_date']


def test_database_source_transport(monkeypatch):
    app = Flask(__name__)
    app.config.update(COLLECTIONS_GATE_URL='https://gate.example', COLLECTIONS_GATE_TOKEN='synthetic')
    calls = []
    route = {'project': 'synthetic'}
    class Response:
        status_code = 200
        def json(self): return {'allowed': True, 'route': route}
    def post(url, **kwargs):
        calls.append(kwargs)
        return Response()
    monkeypatch.setattr(api.requests, 'post', post)
    with app.app_context():
        assert api.resolve_source('a'*32, payload()) == route
        assert calls[0]['json']['resolve_source'] is True
        assert calls[0]['allow_redirects'] is False
        monkeypatch.setattr(Response, 'json', lambda self: {'allowed': False})
        with pytest.raises(ValueError): api.resolve_source('a'*32, payload())
        app.config['COLLECTIONS_ROUTES'] = {'1:national': route}
        with pytest.raises(ValueError): api.resolve_source('a'*32, payload())


def test_changed_or_disabled_database_source_blocks_dispatch(env, monkeypatch):
    from flask import current_app
    client, scheduler = env
    result = put(client, payload())
    task_id = result.json['task_id']
    assert api.task_allowed(task_id)
    current_app.config['TEST_SOURCE_ROWS']['1:national']['spider'] = 'changed_source'
    assert not api.task_allowed(task_id)
    current_app.config['TEST_SOURCE_ROWS']['1:national']['spider'] = 'synthetic'
    assert api.task_allowed(task_id)
    def unavailable(*args):
        raise ValueError('Database source disabled')
    monkeypatch.setattr(api, 'resolve_source', unavailable)
    assert not api.task_allowed(task_id)
    assert models.Task.query.count() == 1


def test_zero_quality_revision_without_quality_commitment(env):
    client, scheduler = env
    data = dict(payload(), quality_revision=0)
    result = client.post('/collections/v1/' + 'a' * 32 + '/check/', json=data,
                         headers={'X-Piwigo-Token': 'synthetic-token'})
    assert result.status_code == 200
    assert models.Task.query.count() == 0 and not scheduler.get_jobs()
    result = put(client, data)
    assert result.status_code == 200
    assert models.Task.query.count() == 1
    for value in [-1, True, '0', 2147483648]:
        assert put(client, dict(data, quality_revision=value)).status_code == 400
    for key in ['company_id', 'revision', 'domain_id', 'retailer_company_id']:
        assert put(client, dict(data, **{key: 0})).status_code == 400


def test_collection_tasks_always_follow_latest_version(env):
    client, scheduler = env
    result = put(client, payload())
    assert result.status_code == 200
    task = models.Task.query.get(result.json['task_id'])
    assert task.version == 'default: the latest version'
    next_run = scheduler.get_job(str(task.id)).next_run_time
    from flask import current_app
    current_app.config['TEST_SOURCE_ROWS']['1:national']['version'] = 'new_release'
    assert put(client, payload()).status_code == 200
    assert models.Task.query.count() == 1
    assert scheduler.get_job(str(task.id)).next_run_time == next_run
    task.version = 'old_release'
    models.db.session.commit()
    assert put(client, payload()).json['error'] == 'native_task_drift'
