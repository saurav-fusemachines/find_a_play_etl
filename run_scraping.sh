#!/bin/bash

# source /home/ubuntu/linkedin_jobs/.venv/bin/activate
cd /home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group
/usr/bin/python3 /home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/pipeline.py >> /home/fm-pc-lt-342/Documents/Fusemachines/Broadway_Licensing_Group/cron_logs/log_$(date +\%Y-\%m-\%d).log 2>&1
# deactivate
