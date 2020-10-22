#!/bin/bash
echo "scrapper started"

today=`date +%Y_%m_%d_%H_%M_%S`


PROJECT_ROOT=/apps/scrapping/realestate_scrapping
export PYTHONPATH=$PYTHONPATH:$PROJECT_ROOT

LOG_DIR=/apps/scrapping/logs

#  First of all, list all files older than 30 days of logs
# find $LOG_DIR -name "*.log" -type f -mtime +30
find $LOG_DIR -name "*.log" -type f -mtime +30 -exec rm -f {} \;


# Start scrapper
cd $PROJECT_ROOT
python3 app.py  >> $LOG_DIR/app_${today}.log 2>&1 &

echo "app started"