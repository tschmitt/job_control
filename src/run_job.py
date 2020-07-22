"""
SYNOPSIS

run_job.py [-p, --path] [-c, --config] [-d, --delay] [-D, --disabled] [-e, --email] [-r, --running_delay] [-s, --simulate] [-v, --verbose]
                [-h, --help] [--version]
    

DESCRIPTION

    This script will execute job steps in a synchronous or asynchronous manner.
    A JSON configuration file is utilized to define the job steps.
    Steps may be local OS commands or email.
    Logs for each step will be created in:
        %path/logs/
    A persistence pickle file is also created at the completion of the job.
        This is for future functionality.
        
    Parameters:
        -p, --path
                The directory where the configuration file is located.
                    (Default = ./)
        -l, --log_path
                The directory where the log files will be written.
                    (Default = --path/logs/)                    
        -c, --config
                Configuration file name.
                Convention: <name>.conf.json
        -d, --delay
                Length of sleep delay (seconds) between main loop iterations.
                    (Default = 1)
        -D, --disabled
                Comma delimited list of steps to disable                    
        -e, --email
                Email_to address for job notices
        -E, --extras
                Additional parameters in JSON format. These are passed through to the Job.
        --extras_file
                Additional parameters stored in JSON file. These are merged with --extras.
        -r, --running_delay
                Print summary of running steps every n seconds
                    (Default = 900 / Value must be at least 60)
        -s, --simulate
                Simulates the execution of a job. No steps will be executed, but the entire job flow will be tested.
                    (Default = False)
        -v, --verbose
                Prints extra output.
                    (Default = True)
        --no_success_email
                Prevents the summary email from being sent for successful jobs. A failure email is always sent.

EXAMPLES

    python2.7 -u run_job.py -c test.conf.json
        Runs a job in the current directory with all defaults.
        
    python2.7 -u run_job.py -p /home/jobs/  -c test.conf.json -d 10 -v -s
        Runs a job in a defined directory with custom settings.
     

EXIT job.statuses

    0    No errors
    1    Job control internal error
    2    Keyboard Interrupt (Ctrl-C)
    3    Step failure
    4    Email failure
    


AUTHOR

    Terry Schmitt <tschmitt@schmittworks.com>

CHANGES

    20120719    tschmitt@schmittworks.com       Added --log_path parameter to allow for customizable log file location.
                                                Added --no_success_email parameter to prevent the summary email for successful jobs.
                                                A failure email will always be sent.
    20130521    tschmitt@schmittworks.com       Added --disable parameter
                                                Added --running_delay parameter
                                                Fixed bug in print_results() call in sigint_handler
    20150720    tschmitt@schmittworks.com       Misc cleanup
                                                Enforcing MAIL_FROM. This is now required and is NOT backward compatible.
    20180530    tschmitt@schmittworks.com       Added error handling for send_summary_mail()
                                                Removed default for --email
                                                Added better timestamp formatting for logging.
                                                Added --extras_file parameter and handling.
    20191031    cameron.ezell@clearcapital.com  Updated to make compatible with Python 3


VERSION

    1.8.000
    
"""
from __future__ import print_function
from builtins import str

import json
import optparse
import os
import traceback
import signal
import socket
import sys
import time
from copy import deepcopy
from datetime import datetime
from job_control import jobs

def main ():

    global options, args, job
    
    MAIL_TO = options.email
       
    #Create the job instance
    job = jobs.Job(options.path, options.log_path, options.config, options.simulate, options.json_extras, options.disabled)
        
    print(job.start_time.strftime("%Y-%m-%d %H:%M:%S"), 'JOB START')
    if options.simulate:
        print('*** SIMULATE MODE - No steps will be executed ***')

    #Register SIGINT (2)
    signal.signal(signal.SIGINT, sigint_handler)

    #Main job control loop. Runs until all steps are complete or a failure occurs
    while job.runnables() or job.running():
        #Find runnable steps and queue them
        job.queue_runnables()
    
        #Process queue and spawn runnable processes
        job.process_queue(True)
            
        #Check processes looking for completed steps
        job.monitor_processes()
        
        #Periodic summary display of running steps
        job.print_running_summary(options.running_delay)
        
        #Sleep between iterations to prevent cpu abuse.
        time.sleep(options.delay)
        
    job.stop_time = datetime.today()
    job.duration = job.stop_time-job.start_time
    job.save()
    print(job.start_time.strftime("%Y-%m-%d %H:%M:%S"), 'JOB COMPLETE')
    job.print_results(True, False)

    #Send the email summary on failure or if desired
    if not job.is_success() or (options.send_success_email and job.is_success()):
        try:
            job.send_summary_mail()
        except Exception as e:
            print('ERROR, EMAIL EXCEPTION')
            print(str(e))
            return 4
    
    #Job result
    if job.is_success():
        return 0
    else:
        return 3

def sigint_handler(signal, frame):
    '''
        Shut down job if Ctrl-c or kill -2 is received
    '''
    print('***** The job was canceled via SIGINT *****')
    job.cancel()
    job.print_results(True, False)
    job.send_summary_mail()
    print('***** The job was canceled via SIGINT *****')
    sys.exit(2)

#not currently being used, but will need it, so leave it   
def log_it(dir, msg, mode):
    now = datetime.today()
    message = now.strftime("%Y-%m-%d %H:%M:%S") + ' ' + msg + '\n'
    path = os.path.join(dir, 'log.log')
    logfile = open(path, mode)
    try:
        logfile.write(message)
    finally:
        logfile.close()  
    
if __name__ == '__main__':
    try:
        start_time = time.time()
        parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(),
                usage=globals()['__doc__'], version='1.7.001')
        parser.add_option ('-p', '--path', action='store', help='File path',
                default='./')
        parser.add_option ('-l', '--log_path', action='store', help='Log file path')
        parser.add_option ('-c', '--config', action='store',
                help='Job configuration file')
        parser.add_option ('-d', '--delay', action='store',
                help='Sleep seconds between iterations', type='int', default=1)
        parser.add_option ('-D', '--disabled', action='store',
                help='Comma delimited list of steps to disable')
        parser.add_option ('-e', '--email', action='store',
                help='Email_to address for job notices')
        parser.add_option ('-E', '--extras', action='store',
                help='Additional parameters in JSON format')
        parser.add_option ('--extras_file', action='store',
                help='Additional parameters stored in JSON file')
        parser.add_option ('-r', '--running_delay', action='store',
                help='Print summary of running steps every n seconds', type='int', default=900)
        parser.add_option ('-s', '--simulate', action='store_true',
                help='Simulate a job execution', default=False)
        parser.add_option ('--no_success_email', action='store_false',
                help='Sends a summary email on success', default=True, dest='send_success_email')        
        parser.add_option ('-v', '--verbose', action='store_true',
                default=True, help='verbose output')
        (options, args) = parser.parse_args()

        #Runtime setting of variables. These are passed through to the Job.
        #From command line
        if options.extras:
            options.json_extras = json.loads(options.extras)
        else:
            options.json_extras = {}

        #From JSON file
        #Command line and JSON file are merged with command line winning if duplicate 
        if options.extras_file:
            extras_file_path = os.path.join(options.path, options.extras_file)
            f = open(extras_file_path, 'r')
            #Load entire json object
            extras_contents = f.read()
            f.close()
            extras_contents = json.loads(extras_contents)
            merged = deepcopy(extras_contents)
            merged.update(options.json_extras)
            options.json_extras = merged


        #Validation
        #Test for log_path
        if not options.log_path:
            options.log_path = os.path.join(options.path, 'logs')
        #Check for valid path
        if not os.path.isdir(options.path):
            parser.error('option -p %s is not a valid directory' % options.path)
        #Check for config file existence
        if not os.path.isfile(os.path.join(options.path, options.config)):
            parser.error('option -c %s file does not exist' %
                                (os.path.join(options.path, options.config)))  
        #Check that running_delay is at least 60 seconds
        if options.running_delay < 60:
            parser.error('option -r %s is less than 60 seconds' % options.running_delay)
        #Create the log directory if required
        if not os.path.isdir(options.log_path):
            os.makedirs(options.log_path)

        print('START TIME:', time.asctime())
        results = main()        
        print('END TIME:', time.asctime())
        print('ELAPSED TIME:', end=' ')
        elapsed_time = time.time() - start_time
        if elapsed_time >= 60:
            print(round(elapsed_time / 60.0, 2), 'min')
        else:
            print(int(elapsed_time), 'sec')
        
        sys.exit(results)

    except SystemExit as e: # sys.exit()
        raise e
    except Exception as e:
        print('ERROR, UNEXPECTED EXCEPTION')
        print(str(e))
        traceback.print_exc()
        os._exit(1)
