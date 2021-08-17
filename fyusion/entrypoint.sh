#!/usr/bin/env bash

# User-provided configuration must always be respected.
cd /celery-redis/	
celery -A test_celery worker --loglevel=info &
sleep 10 &
python -m test_celery.run_tasks

