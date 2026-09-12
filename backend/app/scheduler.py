from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .database import SessionLocal
from . import models
from .executor import execute_workflow_run

scheduler = BackgroundScheduler()


def _trigger_scheduled_run(workflow_id: str):
    db = SessionLocal()
    try:
        workflow = db.query(models.Workflow).get(workflow_id)
        if not workflow or workflow.is_paused:
            return
        run = models.WorkflowRun(workflow_id=workflow.id, triggered_by="schedule")
        db.add(run)
        db.commit()
        execute_workflow_run(db, run.id)
    finally:
        db.close()


def _evaluate_scheduled_alert(alert_id: str):
    from .alerts import evaluate_alert
    db = SessionLocal()
    try:
        alert = db.query(models.Alert).get(alert_id)
        if not alert or alert.is_paused:
            return
        evaluate_alert(db, alert)
    finally:
        db.close()


def _run_scheduled_pipeline(pipeline_id: str):
    from .main import run_pipeline_now
    db = SessionLocal()
    try:
        pipeline = db.query(models.Pipeline).get(pipeline_id)
        if not pipeline or pipeline.is_paused:
            return
        run_pipeline_now(db, pipeline)
    finally:
        db.close()


def sync_jobs():
    """Re-reads all workflows, alerts, and pipelines and (re)registers their cron
    schedules. Call this on startup and whenever any of them is created/updated."""
    scheduler.remove_all_jobs()
    db = SessionLocal()
    try:
        workflows = db.query(models.Workflow).filter(models.Workflow.schedule_cron.isnot(None)).all()
        for wf in workflows:
            if wf.is_paused:
                continue
            try:
                trigger = CronTrigger.from_crontab(wf.schedule_cron)
                scheduler.add_job(_trigger_scheduled_run, trigger, args=[wf.id], id=f"workflow:{wf.id}", replace_existing=True)
            except Exception:
                continue  # invalid cron string - skip rather than crash the whole scheduler

        alerts = db.query(models.Alert).filter(models.Alert.schedule_cron.isnot(None)).all()
        for a in alerts:
            if a.is_paused:
                continue
            try:
                trigger = CronTrigger.from_crontab(a.schedule_cron)
                scheduler.add_job(_evaluate_scheduled_alert, trigger, args=[a.id], id=f"alert:{a.id}", replace_existing=True)
            except Exception:
                continue

        pipelines = db.query(models.Pipeline).filter(models.Pipeline.schedule_cron.isnot(None)).all()
        for p in pipelines:
            if p.is_paused:
                continue
            try:
                trigger = CronTrigger.from_crontab(p.schedule_cron)
                scheduler.add_job(_run_scheduled_pipeline, trigger, args=[p.id], id=f"pipeline:{p.id}", replace_existing=True)
            except Exception:
                continue
    finally:
        db.close()


def start():
    sync_jobs()
    scheduler.start()
