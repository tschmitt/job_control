#!/usr/bin/env python

"""
SYNOPSIS

    run_job.py [-p, --path] [-c, --config] [-s, --sleep] [-v, --verbose]
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
        -p, --path    The directory where the configuration files is located.
                        The logs directory will be created here/
                        (Default = ./)
        -c, --config  Configuration file name.
        -s, --sleep   Length of sleep delay (seconds) between main loop iterations.
                        (Default = 1 )
        -v, --verbose Prints extra output.
                        (Default = False)

EXAMPLES

    run_job.py -c job1.conf.json
        Runs a job in the current directory with all defaults.
        
    run_job.py -p /home/jobs/  -c job2.conf.json -s 10 -v
        Runs a job in a defined directory with custom settings.
     

EXIT job.statuses

    0    No errors
    1    Job control internal error
    2    Keyboard Interrupt (Ctrl-C)
    3    Step failure
    


AUTHOR

    Terry Schmitt <tschmitt@schmittworks.com>

VERSION

    1.0.000
    
"""


import optparse
import os
import traceback
import sys
import time
from datetime import datetime

from job_control import jobs


def main ():

    global options, args, job
    
    MAIL_TO = 'tschmitt@schmittworks.com'
    MAIL_FROM = 'job_control@schmittworks.com'
        
    #Create the job instance
    job = jobs.Job(options.path, options.config)
        
    print job.start_time, 'JOB START'
    
    #Main job control loop. Runs until all steps are complete or a failure occurs
    while job.runnables() or job.running():
        #Find runnable steps and queue them
        job.queue_runnables()
    
        #Process queue and spawn runnable processes
        job.process_queue(True)
            
        #Check processes looking for completed steps
        job.monitor_processes(True)
        
        #Sleep between iterations to prevent cpu abuse.
        #This does not need to be an event loop.
        time.sleep(options.sleep)
        
    job.stop_time = datetime.today()
    job.duration = job.stop_time-job.start_time
    job.save()
    print job.start_time, 'JOB COMPLETE'
    job.print_results(True)
    
    if job.is_success():
        return 0
    else:
        #Send an email
        mail_subject = 'Job %s failed' % (job.config_file)
        mail_args = {
                     'mail_to': MAIL_TO,
                     'mail_from': MAIL_FROM,
                     'mail_subject': mail_subject,
                     'mail_body': mail_subject
                     }
        job.send_mail(**mail_args)
        return 3

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
                usage=globals()['__doc__'], version='1.0.000')
        parser.add_option ('-p', '--path', action='store', help='file path',
                default='./')
        parser.add_option ('-c', '--config', action='store',
                help='job configuration file')
        parser.add_option ('-s', '--sleep', action='store',
                help='Sleep seconds between iterations', type='int', default=1)
        parser.add_option ('-v', '--verbose', action='store_true',
                default=True, help='verbose output')
        (options, args) = parser.parse_args()

        #Validation
        #Check for valid path
        if not os.path.isdir(options.path):
            parser.error('option -p %s is not a valid directory' % (options.path))
        #Check for config file existence
        if not os.path.isfile(os.path.join(options.path, options.config)):
            parser.error('option -c %s file does not exist' %
                                (os.path.join(options.path, options.config)))        

        print 'START TIME:', time.asctime()
        results = main()        
        print 'END TIME:', time.asctime()
        print 'ELAPSED TIME:',
        elapsed_time = time.time() - start_time
        if elapsed_time >= 60:
            print round(elapsed_time / 60.0, 2), 'min'
        else:
            print int(elapsed_time), 'sec'
        
        sys.exit(results)
    except KeyboardInterrupt, e: # Ctrl-C
        print '***** The job was canceled via Ctrl-C *****'
        job.cancel()
        job.print_results(True)
        print '***** The job was canceled via Ctrl-C *****'
        sys.exit(2)
    except SystemExit, e: # sys.exit()
        raise e
    except Exception, e:
        print 'ERROR, UNEXPECTED EXCEPTION'
        print str(e)
        traceback.print_exc()
        os._exit(1)
