#! /bin/bash

#############################################################################
#############################################################################
# NAME:     test.sh
# AUTHOR:   tschmitt@schmittworks.com
# DATE:     20170322

# DESCRIPTION: This is a wrapper that sets the environment and launches test.conf.json


# USAGE: ./test.sh


# DATE            WHO                           DESCRIPTION                    
# -----------     -------------------------     --------------------------------------------------------
# 20170322        tschmitt@schmittworks.com     Created

#############################################################################
#############################################################################

# Set environment
# Location of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create directory
LOG_DATE=`date +%Y%m%d_%H%M`
LOG_PATH=/tmp/test/$LOG_DATE
mkdir -p $LOG_PATH

DISABLE=901


# Launch test
python -u -B ../src/run_job.py -d 0 --no_success_email -p $DIR -c test.conf.json -l $LOG_PATH --extras_file test_extras.json -E "{\"test_a\": \"test from -E\"}" -D $DISABLE 2>&1 | tee $LOG_PATH/test.log

# Exit with the job_control results
exit ${PIPESTATUS[0]}
