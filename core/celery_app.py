from celery.schedules import crontab
from app.config import settings
import logging
import celery
from app.apps.crontab import CELERY_BEAT_SCHEDULE

celery_app = celery.Celery("worker", broker=settings.CELERY_BROKER_URL)

celery_app.conf.task_routes = {"app.worker.*": "main-queue"}

celery_app.conf.beat_schedule = CELERY_BEAT_SCHEDULE
celery_app.conf.timezone = 'UTC'


@celery.signals.after_setup_logger.connect
def on_after_setup_logger(**kwargs):
    logger = logging.getLogger('celery.task')
    logger.propagate = True
        
    handler = logging.StreamHandler()
    handler.setFormatter(celery.app.log.TaskFormatter("[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] [TASK:%(task_name)s TID:%(task_id)s] [APP:" + settings.APP_NAME +  " PID:%(process)d TID:%(thread)d] - %(message)s"))
    logger.addHandler(handler)
    
    error_logger = logging.getLogger("uvicorn.error")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] [APP:" + settings.APP_NAME +  " PID:%(process)d TID:%(thread)d] - %(message)s"))
    error_logger.addHandler(handler)