"""
SYNOPSIS

    jobs module
    

DESCRIPTION

    This contains the job class, which is used in the execution of a job.
    The job class also includes all the step functionality, which may be moved
        to a steps module and step class at a later date.
        
AUTHOR

    Terry Schmitt <tschmitt@schmittworks.com>

CHANGES

    20120719    tschmitt@schmittworks.com           Added --log_path parameter to allow for customizable log file location.
                                                    Fixed bug where summary printed twice.
    20120731    tschmitt@schmittworks.com           Added 'sleep' internal step.
    20130521    tschmitt@schmittworks.com           Added --disable parameter to allow disabling of steps at runtime.
    20140212    tschmitt@schmittworks.com           Added 'completed list:' to summary display.
    20150728    tschmitt@schmittworks.com           Added mail_to_fail and smtp_relay variables
                                                    Email recipient variables can now accept a comma delimited string to send to multiple recipients
    20180222    sara.rasmussen@clearcapital.com     Add job step name to outputs with PID
    20180419    tschmitt@schmittworks.com           Add optional step parameter, resultcode_allowed, to allow successful resultcodes other than 0
    20180530    tschmitt@schmittworks.com           Added mail_from_domain, which is appended to HOSTNAME and used as default mail_from.
                                                    Removed default mail_to. 
                                                    Added several timestamp formatting options for logging.
    20180705    tschmitt@schmittworks.com           Fixed error when calling print_running_summary.
    20191031    cameron.ezell@clearcapital.com      Updated to make compatible with Python 3
    20211106    tschmitt@schmittworks.com           Added JSON log output
                                                    Colorized the output a bit
                                                    

VERSION

    1.9.000
    
"""
from __future__ import print_function
from builtins import object
import json
import os
import pickle
import shlex
import signal
import smtplib
import socket
import time
import sys
from collections import deque
from copy import deepcopy
from datetime import date
from datetime import datetime
try:
    from email.mime.multipart import MIMEMultipart
except:
    from email.MIMEMultipart import MIMEMultipart
try:
    from email.mime.text import MIMEText
except:
    from email.MIMEText import MIMEText

from string import Template
from subprocess import Popen, STDOUT

#Not used but may use later for converting datetime
#def encode_custom(obj):
#    if isinstance(obj, datetime):
#        return obj.strftime('%Y-%m-%dT%H:%M:%S')
#    else:
#        return json.JSONEncoder.default(obj)

ANSI_RESET="\033[0m"
ANSI_GREEN="\033[92m"
ANSI_RED="\033[31m"

class Error(Exception):
    '''
        Base class for module exceptions
    '''
    pass

class InvalidTypeError(Error):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
        
class Job(object):
    '''
    This class:
        Reads in steps from a configuration file.
        Spawns processes and monitors.
        Persists the status.
    '''

    def __init__(self, path, log_path, config_file, simulate, json_extras, disabled):
        '''
        Constructor
        '''
        #Parameters
        self.path = path
        self.config_file = config_file
        self.simulate = simulate
        self.json_extras = json_extras
        self.disabled = disabled
        
        #Build paths
        self.config_path = os.path.join(self.path, self.config_file)
        self.log_path = log_path
        self.data_path = os.path.join(self.log_path, '%s.%s' %
                                     (self.config_file.split('.')[0], 'pkl'))
        self.log_path_json = os.path.join(self.log_path, '%s.%s' %
                                     (self.config_file.split('.')[0], 'log.json'))
        
        #Defaults
        self.start_time = datetime.today()
        self.status_display_start = time.time()
        self.stop_time = None
        self.duration = 0  
        self.queue = deque([]) #Runnable steps
        self.processes = {} #Running and completed steps
        self.completed = [] #Sucessful steps
        self.failed = [] #Failed steps      
        self.disabled_steps = []      
        
        #Constants
        self.HOSTNAME_FQDN = socket.getfqdn()
        self.HOSTNAME = self.HOSTNAME_FQDN.split('.')[0]
        self.STEP_STATUS_TEMPLATE = {'status': 'waiting', 'resultcode': None, 'pid': None,'queued_time':  None, 'start_time': None, 'stop_time': None, 'duration': 0, 'simulate': self.simulate}
        self.STATUSES = {
                     'waiting':  {'name': 'waiting', 'runnable': True, 'running': False, 'description': 'Step has not been queued yet'},
                     'queued':   {'name': 'queued', 'runnable': True, 'running': False, 'description': 'Step is in the queue and ready to run'},
                     'running':  {'name': 'running', 'runnable': False, 'running': True, 'description': 'Step is currently running'},
                     'complete': {'name': 'complete', 'runnable': False, 'running': False, 'description': 'Step completed without error'},
                     'disabled': {'name': 'disabled', 'runnable': False, 'running': False, 'description': 'Step has been disabled and will not be run'},
                     'failed':   {'name': 'failed', 'runnable': False, 'running': False, 'description': 'Step failed'},
                     'canceled': {'name': 'canceled', 'runnable': False, 'running': False, 'description': 'Step was canceled without being started. Most likely due to a dependency that failed. This step may be rerun'},
                     'aborted':  {'name': 'aborted', 'runnable': False, 'running': False, 'description': 'Step was aborted while it was running. Most likely due to Ctrl-C or a unhandled exception. This step will likely require research before rerunning'}
                         }
        self.STEP_TYPES = {
                    'os': {'name': 'os', 'description': 'Spawns a new asynchronous process and execute the os level command with parameters'},
                    'internal': {'name': 'internal', 'description': 'Pre configured steps for routine functionality'}
                      }
        self.TASK_TYPES = {
                    'send_mail': {'name': 'send_mail', 'description': 'Sends an email. This is a blocking event'},
                    'sleep': {'name': 'sleep', 'description': 'Sleeps for x seconds'}
                      }        
        self.CONFIG_DEFAULTS = {
                            'concurrency': self.detect_cpus(),
                            'config_file': self.config_file,
                            'date': self.start_time.strftime('%Y_%m_%d'),
                            'date_time': self.start_time.strftime('%Y%m%d_%H%M%S'),
                            'date_time_2': self.start_time.strftime('%Y%m%d-%H%M%S'),
                            'date_time_3': self.start_time.strftime('%Y%m%d%H%M%S'),
                            'date_time_4': self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'date_time_friendly': self.start_time.strftime('%c'),
                            'hostname': self.HOSTNAME,
                            'hostname_fqdn': self.HOSTNAME_FQDN,
                            'mail_from_domain': '',
                            'mail_from': '',
                            'mail_to': '',
                            'mail_to_fail': '',
                            'smtp_relay': 'localhost'
                                }
        
        #Read and load the job configuration file
        self.raw_config = self.load_config()
        self.load_json(self.raw_config)
        
        #Set defaults
        self.merge_default_to_config()
        self.CONCURRENCY = self.config['variables']['concurrency']
        if not self.config['variables']['mail_from']:
            self.MAIL_FROM = self.HOSTNAME + '@' + self.config['variables']['mail_from_domain']
        else:
            self.MAIL_FROM = self.config['variables']['mail_from']
        self.MAIL_TO = self.config['variables']['mail_to']
        self.MAIL_TO_FAIL = self.config['variables']['mail_to_fail']
        self.SMTP_RELAY = self.config['variables']['smtp_relay']
        
        #Replace variables in configuration
        t = Template(self.raw_config)
        self.raw_config = t.safe_substitute(self.config['variables'])
        
        #Load the job JSON configuration
        self.load_json(self.raw_config)

        #Create steps from json
        self.steps = self.config['steps']

        #Set up disabled_steps
        if self.disabled:
            self.disabled_steps = self.disabled.replace(' ', '').split(',')

        #Preprocessing
        for step, value in self.steps.items():
            #Set up the default job_status for each step
            self.steps[step]['job_status'] = deepcopy(self.STEP_STATUS_TEMPLATE)
            
            #Check for ALL dependency and set
            if self.steps[step]['dependencies'] == 'ALL':
                self.steps[step]['dependencies'] = self.get_all_dependencies(step)
                    
            #Build args
            #Need to encode the task value for Python < 2.7.3. json returns unicode, but shlex cannot handle unicode.
            pyVersion = float(str(sys.version_info[0]) + "." + str(sys.version_info[1]))
            if pyVersion < 2.7:
                self.steps[step]['args'] = shlex.split(self.steps[step]['task'].encode('utf-8'))
            else:
                self.steps[step]['args'] = shlex.split(self.steps[step]['task'])
            if 'detail' in self.steps[step]:
                self.steps[step]['args'].append(self.steps[step]['detail'])
                
            #Check for disabled steps
            if not self.steps[step]['enabled']:
                self.steps[step]['job_status']['simulate'] = True

            #Disable any steps via disabled_steps parameter
            if step in self.disabled_steps:
                self.steps[step]['job_status']['simulate'] = True

            #Set up successful resultcodes
            if 'resultcode_allowed' not in self.steps[step]:
                self.steps[step]['resultcode_allowed'] = [0]
            else:
                self.steps[step]['resultcode_allowed'] = self.steps[step]['resultcode_allowed']

    def cancel_children(self, step):
        '''
            Cancels all dependents of a step
        '''
        
        for child_step in sorted(self.get_decendents(step)):
            #If runnable
            if self.steps[child_step]['job_status']['status'] in ('waiting','queued'):
                #Update status to canceled
                self.steps[child_step]['job_status']['status'] = 'canceled'
                
                #Remove from queue if required
                # Don't think we need this, but leaving for now...
                #if child_step in self.queue:
                #    del self.queue[self.queue.index(child_step)]
                    
    def get_children(self, step):
        '''
            Returns the immediate children step keys for a given step
        '''
        
        children = []
        for child in sorted(self.steps.keys()):
            if step in self.steps[child]['dependencies']:
                children.append(child)
        return children
        
    def get_decendents(self, step):
        '''
            Return all decendents step keys for a given step
            
            1. put the parent in child_queue to bootstrap the process
            2. while child_queue
                2.a. pop left
                2.b. crawl the step
                2.c. add any new children to decendents and child_queue            
        '''
        decendents = []
        child_queue = deque([])

        #Bootstrap with the supplied step
        child_queue.append(step)
        
        #Recurse through all decendents
        while child_queue:
            #Grab the next child to investigate
            candidate = child_queue.popleft()
            
            #Grab the children of this step
            children = self.get_children(candidate)

            #Append to decendents and queue up the new children for investigation.
            #We only want unique values in child_queue.
            decendents = list(set(decendents + children))

            child_queue.extend(children)
            child_queue = deque(list(set(child_queue)))
            
        return decendents
                    
    def cancel(self):
        '''
            Cancels the job and running steps
        '''
        
        for step in self.steps:
            #If running
            if self.STATUSES[self.steps[step]['job_status']['status']]['running']:
                self.abort_step(step)             
    
    def abort_step(self, step):
        '''
            Aborts the step and all children
        '''
        
        if self.steps[step]['type'] == 'os':
            #Get the pid
            pid = self.processes[step]['process'].pid
            #Rudely kill it
            os.kill(pid, signal.SIGKILL)
            #Close the out file
            self.processes[step]['out'].close()        
        #Set step status
        self.steps[step]['job_status']['status'] = 'aborted'
        #Cancel dependents
        self.cancel_children(step)
                      
    def complete_step(self, step, results):
        '''
            Appends to completed and updates the status.
            Cancels children if a failed step.
        '''
        
        if results in self.steps[step]['resultcode_allowed']:
            self.completed.append(step)
            self.steps[step]['job_status']['status'] = 'complete'
        else:
            self.failed.append(step)
            self.steps[step]['job_status']['status'] = 'failed'
            #cancel any dependent steps
            self.cancel_children(step)
            
        #Update stats
        self.steps[step]['job_status']['resultcode'] = results  
        self.steps[step]['job_status']['stop_time'] = datetime.today()
        self.steps[step]['job_status']['duration'] = self.steps[step]['job_status']['stop_time']-self.steps[step]['job_status']['start_time']
        
    def dependencies_met(self, step):
        '''
            Checks that the dependency steps are complete
        '''
        
        if self.steps[step]['dependencies'] is None:
            #No dependencies
            return True
        else:
            completed = set(self.completed)
            dependencies = set(self.steps[step]['dependencies'])

            if len(dependencies) == len(completed.intersection(dependencies)):
                return True
            else:
                return False
                            
    def get_all_dependencies(self, step):
        '''
            Return all steps except step
        '''
        
        keys = list(self.steps.keys())
        keys.remove(step)
        return keys
        
    def get_aborted_steps(self):
        '''
            Retrieves aborted steps
        '''
        
        steps = []
        for step in self.steps:
            if self.steps[step]['job_status']['status'] == 'aborted':
                steps.append(step)
        return steps
    
    def get_canceled_steps(self):
        '''
            Retrieves canceled steps
        '''
        
        steps = []
        for step in self.steps:
            if self.steps[step]['job_status']['status'] == 'canceled':
                steps.append(step)
        return steps    
    
    
    def get_running_steps(self):
        '''
            Iterates through all the steps to find all running steps
        '''
        
        running_steps = {}
        for step in self.steps:
            #If running
            if self.STATUSES[self.steps[step]['job_status']['status']]['running']:
                running_steps[step] = self.steps[step]

        return running_steps
    
    def is_success(self):
        if len(self.steps) == len(self.completed):
            return True
        else:
            return False
        
    def load_config(self):
        '''
            Reads the JSON Job configuration file.
        '''
        
        try:
            f = open(self.config_path, 'r')
            #Load entire json object
            config_contents = f.read()
            f.close()
            return config_contents

        except IOError:
            print('%s : %s' % ('Could not read config file', self.config_path))
            raise
        except Exception:
            raise
            
    def load_json(self, config_template):
        '''
            Loads the JSON Job configuration file.
        '''
        
        try:
            #Load entire json object
            self.config = json.loads(config_template)

        except:
            print('%s : %s' % ('Could not parse json', self.config_path))
            raise
        
    def merge_default_to_config(self):
        '''
            Merges default variables with json.
            JSON config file contents overwrites CONFIG_DEFAULTS.
            Command line --json overwrites both JSON and CONFIG_DEFAULTS
        '''
        
        #Set default variables
        merged = deepcopy(self.CONFIG_DEFAULTS)
        merged.update(self.config['variables'])
        merged.update(self.json_extras)
        self.config['variables'] = merged
        
    def monitor_processes(self):
        '''
            Iterates over processes and cleans up after success or fail of
                each process.
        '''

        for step in self.processes:
            #If complete
            if step not in self.completed and step not in self.failed:
                step_complete = False
                if not self.simulate and not self.steps[step]['job_status']['simulate']:
                    #OK, this is a real running step, let's check it
                    results = self.processes[step]['process'].poll()

                    if results is not None: #step is finished
                        #Close the out file
                        self.processes[step]['out'].close()
                        
                        if results in self.steps[step]['resultcode_allowed']:
                            result_str = 'COMPLETE'
                            color = ANSI_GREEN
                        else:
                            result_str = 'FAILED'
                            color = ANSI_RED

                        #Append to completed and update status
                        self.complete_step(step, results)
                        step_complete = True
                else:
                    result_str = 'COMPLETE'
                    color = ANSI_GREEN
                    results = 0
                    self.complete_step(step, results)
                    step_complete = True

                if step_complete:
                    if self.simulate or self.steps[step]['job_status']['simulate']:
                        sim_msg = '(simulated)'
                    else:
                        sim_msg = ''
                    print('%s%s STEP %s: %s resultcode: %s duration: %s %s%s' % (color, self.format_date(datetime.today()), result_str, step, results, self.steps[step]['job_status']['duration'], sim_msg, ANSI_RESET))
                    
    def format_date(self, datetime):
        '''
            Returns a formatted datetime for logging
        '''
        return datetime.strftime("%Y-%m-%d %H:%M:%S")

    def print_results(self, verbose, terminal):
        '''
            Prints out a summary of the job results
        '''
        summary = []
        summary_verbose = []
        SEP = '*******************************************'
        
        if verbose:
            summary_verbose.append(SEP)
            summary_verbose.append('JOB DETAIL')
            summary_verbose.append(SEP)
            summary_verbose.append('Completed Steps:')
            if not self.completed:
                summary_verbose.append('     None')
            for step in self.completed:
                if self.simulate or self.steps[step]['job_status']['simulate']:
                    sim_msg = '(simulated)'
                else:
                    sim_msg = ''

                summary_verbose.append('Step: %s' % step)
                summary_verbose.append('     name:       %s' % self.steps[step]['name'])
                summary_verbose.append('     status :    %s %s' % (self.STATUSES[self.steps[step]['job_status']['status']]['name'], sim_msg))
                summary_verbose.append('     resultcode: %s (allowed: %s)' % (self.steps[step]['job_status']['resultcode'], self.steps[step]['resultcode_allowed']))
                summary_verbose.append('     start:      %s' % self.format_date(self.steps[step]['job_status']['start_time']))
                summary_verbose.append('     stop:       %s' % self.format_date(self.steps[step]['job_status']['stop_time']))
                summary_verbose.append('     duration:   %s' % self.steps[step]['job_status']['duration'])
            summary_verbose.append(SEP)
            summary_verbose.append('Failed Steps:')
            if not self.failed:
                summary_verbose.append('     None')
            for step in self.failed:
                summary_verbose.append('Step: %s' % step)
                summary_verbose.append('     name:       %s' % self.steps[step]['name'])
                summary_verbose.append('     status :    %s' % self.STATUSES[self.steps[step]['job_status']['status']]['name'])
                summary_verbose.append('     resultcode: %s (allowed: %s)' % (self.steps[step]['job_status']['resultcode'], self.steps[step]['resultcode_allowed']))
                summary_verbose.append('     start:      %s' % self.format_date(self.steps[step]['job_status']['start_time']))
                summary_verbose.append('     stop:       %s' % self.format_date(self.steps[step]['job_status']['stop_time']))
                summary_verbose.append('     duration:   %s' % self.steps[step]['job_status']['duration'])
            summary_verbose.append(SEP)
            summary_verbose.append('Canceled Steps:')
            if not self.get_canceled_steps():
                summary_verbose.append('     None')
            for step in self.get_canceled_steps():
                summary_verbose.append('Step: %s' % step)
                summary_verbose.append('     name:       %s' % self.steps[step]['name'])
                summary_verbose.append('     status :    %s' % self.STATUSES[self.steps[step]['job_status']['status']]['name'])
                summary_verbose.append('     resultcode: %s (allowed: %s)' % (self.steps[step]['job_status']['resultcode'], self.steps[step]['resultcode_allowed']))
                summary_verbose.append('     start:      %s' % self.steps[step]['job_status']['start_time'])
                summary_verbose.append('     stop:       %s' % self.steps[step]['job_status']['stop_time'])
                summary_verbose.append('     duration:   %s' % self.steps[step]['job_status']['duration'])
            summary_verbose.append(SEP)
            summary_verbose.append('Aborted Steps:')
            if not self.get_aborted_steps():
                summary_verbose.append('     None')
            for step in self.get_aborted_steps():
                summary_verbose.append('Step: %s' % step)
                summary_verbose.append('     name:       %s' % self.steps[step]['name'])
                summary_verbose.append('     status :    %s' % self.STATUSES[self.steps[step]['job_status']['status']]['name'])
                summary_verbose.append('     resultcode: %s (allowed: %s)' % (self.steps[step]['job_status']['resultcode'], self.steps[step]['resultcode_allowed']))
                summary_verbose.append('     start:      %s' % self.steps[step]['job_status']['start_time'])
                summary_verbose.append('     stop:       %s' % self.steps[step]['job_status']['stop_time'])
                summary_verbose.append('     duration:   %s' % self.steps[step]['job_status']['duration'])
            summary_verbose.append(SEP)
            
        summary.append(SEP)
        summary.append('JOB SUMMARY')
        summary.append(SEP)
        summary.append('Job:')
        summary.append('    config file      %s' % self.config_path)
        summary.append('    log path         %s' % self.log_path)
        summary.append('    log path_json    %s' % self.log_path_json)
        summary.append('    start:           %s' % self.format_date(self.start_time))
        summary.append('    stop:            %s' % self.format_date(self.stop_time))
        summary.append('    duration:        %s' % self.duration)
        summary.append('    steps total:     %s' % len(self.steps))
        if terminal:
            if len(self.completed) < len(self.steps):
                color = ANSI_RED
            else:
                color = ANSI_GREEN
            summary.append('    %ssteps completed: %s%s' % (color, len(self.completed), ANSI_RESET))
            if len(self.failed):
                color = ANSI_RED
            else:
                color = ANSI_GREEN
            summary.append('    %ssteps failed:    %s%s' % (color, len(self.failed), ANSI_RESET))
            if len(self.get_canceled_steps()):
                color = ANSI_RED
            else:
                color = ANSI_GREEN
            summary.append('    %ssteps canceled:  %s%s' % (color, len(self.get_canceled_steps()), ANSI_RESET))
            if len(self.get_aborted_steps()):
                color = ANSI_RED
            else:
                color = ANSI_GREEN
            summary.append('    %ssteps aborted:   %s%s' % (color, len(self.get_aborted_steps()), ANSI_RESET))
        else:
            summary.append('    steps completed: %s' % (len(self.completed)))
            summary.append('    steps failed:    %s' % (len(self.failed)))
            summary.append('    steps canceled:  %s' % (len(self.get_canceled_steps())))
            summary.append('    steps aborted:   %s' % (len(self.get_aborted_steps())))
        summary.append('    completed steps: %s' % ",".join(sorted(self.completed)))
        summary.append(SEP)
        
        #If terminal mode, print the results, otherwise just return the data
        if terminal:
            for line in summary_verbose:
                print(line)
                
            for line in summary:
                print(line)            
            
        return {'summary': summary, 'summary_verbose': summary_verbose}

    def print_running_summary(self, periodic_display_interval):
        '''
            Logs a summary of running steps periodically.
            This will allow you to see the current status for long running jobs.
        '''

        #If elapsed time is over the threshold, then print summary
        if time.time() - self.status_display_start > periodic_display_interval:
            cur_running_msg = ''
            steps = self.get_running_steps()
            
            # Reset the time
            self.status_display_start = time.time()
            
            #Gather steps
            for step in steps:
                cur_running_msg = cur_running_msg + '%s: %s (pid: %s) (name: %s)' % (step, datetime.today() - steps[step]['job_status']['start_time'], self.steps[step]['job_status']['pid'], self.steps[step]['name'])  + '\n'
            
            # Display currently running
            print('%s CURRENTLY RUNNING STEPS (%s) ***********************\n%s' % (self.format_date(datetime.today()), len(steps), cur_running_msg))
            
    def process_queue(self, verbose):
        '''
            Launch each step in the queue
        '''
        
        #Launch if step in queue and currently running processes does not exceed the concurrency
        while len(self.queue) and (len(self.get_running_steps()) < int(self.CONCURRENCY)):
            #Remove from queue
            step = self.queue.popleft()
            
            #Update status
            self.steps[step]['job_status']['status'] = 'running'
            self.steps[step]['job_status']['start_time'] = datetime.today()

            if self.steps[step]['type'] == 'os':
                #Launches a new process
                self.processes[step] = {}
                if not self.simulate and not self.steps[step]['job_status']['simulate']:
                    #Create log file
                    outfile = '%s-%s.%s' % (self.config_file.split('.')[0], step, 'out')
                    self.processes[step]['out'] = open(os.path.join(self.log_path, outfile), 'wb')
                    #Launch step
                    self.processes[step]['process'] = Popen(self.steps[step]['args'], stdout=self.processes[step]['out'], stderr=STDOUT)
                    #Get the pid
                    self.steps[step]['job_status']['pid'] = self.processes[step]['process'].pid

                if verbose:
                    print('%s STEP %s: %s (pid: %s) (name: %s)' % (self.format_date(datetime.today()), 'SPAWNED', step, self.steps[step]['job_status']['pid'], self.steps[step]['name']))
            elif self.steps[step]['type'] == 'internal':
                if self.steps[step]['task'] == 'send_mail':
                    if verbose:
                        print('%s STEP %s: %s' % (self.format_date(datetime.today()), 'EXECUTED', step))
                    #Send email
                    if not self.simulate and not self.steps[step]['job_status']['simulate']:
                        results = self.send_mail(**self.steps[step]['args'][1])
                    else:
                        results = 0

                    #If the returned dictionary has members, set to 1
                    if results:
                        results = 1
                    else:
                        results = 0
                    self.complete_step(step, results)
                    if self.simulate or self.steps[step]['job_status']['simulate']:
                        sim_msg = '(simulated)'
                    else:
                        sim_msg = ''
                    if verbose:
                        print('%s STEP COMPLETE: %s resultcode: %s duration: %s %s' % (self.format_date(datetime.today()), step, results, self.steps[step]['job_status']['duration'], sim_msg))
                if self.steps[step]['task'] == 'sleep':
                    if verbose:
                        print('%s STEP %s: %s (Sleeping for %s seconds)' % (self.format_date(datetime.today()), 'EXECUTED', step, self.steps[step]['args'][1]['seconds']))
                    #Sleep for x seconds
                    if not self.simulate and not self.steps[step]['job_status']['simulate']:
                        time.sleep(float(self.steps[step]['args'][1]['seconds']))
                    results = 0

                    self.complete_step(step, results)
                    if self.simulate or self.steps[step]['job_status']['simulate']:
                        sim_msg = '(simulated)'
                    else:
                        sim_msg = ''
                    if verbose:
                        print('%s STEP COMPLETE: %s resultcode: %s duration: %s %s' % (self.format_date(datetime.today()), step, results, self.steps[step]['job_status']['duration'], sim_msg))                        
            else:
                raise InvalidTypeError(self.steps[step]['type'])
                    
    def queue_runnables(self):
        '''
            Places runnable steps in the queue
        '''
        
        for step in sorted(self.steps.keys()):
            #If waiting and not already queued and its dependencies are met
            if self.steps[step]['job_status']['status'] == 'waiting' and step not in self.queue and self.dependencies_met(step):
                #Add to queue
                self.queue.append(step)
                #Update status
                self.steps[step]['job_status']['status'] = 'queued'
                self.steps[step]['job_status']['queued_time'] = datetime.today()
                        
    def runnables(self):
        '''
            Iterates through all the steps to find the first runnable step then
                short circuits.
        '''
        
        for step in self.steps:
            #If runnable
            if self.STATUSES[self.steps[step]['job_status']['status']]['runnable']:
                return True
        return False
    
    def running(self):
        '''
            Iterates through all the steps to find the first running step then
                short circuits.
        '''
        
        for step in self.steps:
            #If running
            if self.STATUSES[self.steps[step]['job_status']['status']]['running']:
                return True
        return False    
                   
    def save_config(self):
        '''
            This writes the configuration to a JSON file.
            This will probably not be used much, as it was primarily written
                to write out the first file as a sample.
        '''
       
        conf = {
                'steps': self.steps               
                }

        try:
            f = open(self.config_path, 'w')
            try:
                json.dump(conf, f, indent=4)
            except:
                f.close()
                print('%s : %s' % ('Could not write config file', self.config_path))
                raise            
        except IOError:
            print('%s : %s' % ('Could not open config file', self.config_path))
            raise

    def save(self):
        '''
            Write the current status to persistent storage.
        '''
        
        data = {
                'steps': self.steps,
                'start_time': self.start_time,
                'stop_time': self.stop_time,
                'duration': self.duration,
                'queue': self.queue,
                'processes': None,
                'completed': self.completed,
                'failed': self.failed                
                }

        try:
            f = open(self.data_path, 'wb')
            try:
                pickle.dump(data, f, 0)
            except:
                print('%s : %s' % ('Could not write data file', self.data_path))
                raise                  
        except IOError:
            print('%s : %s' % ('Could not open data file', self.data_path))
            raise
        
    def save_log(self):
        '''
            Write the job status to a JSON file.
        '''
        log_data = {
                
                'hostname_fqdn': self.HOSTNAME_FQDN,
                'job_start_time': self.start_time,
                'job_stop_time': self.stop_time,
                'job_duration': self.duration,
                'step_count_total': len(self.steps),
                'step_count_completed': len(self.completed),
                'step_count_failed': len(self.failed),
                'step_count_canceled': len(self.get_canceled_steps()),
                'step_count_aborted': len(self.get_aborted_steps()),
                'steps_completed': self.completed,
                'steps_failed': self.failed,
                'steps_canceled': self.get_canceled_steps(),
                'steps': self.steps
                }
        with open(self.log_path_json, "w") as log_file:
            json.dump(log_data, log_file, indent=4, sort_keys=True, default=str)

    def send_mail(self, mail_to, mail_from, mail_subject, mail_body):
        COMMASPACE = ', '
        recipients = mail_to.split(",")
        msg = 'From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s' % (mail_from, COMMASPACE.join(recipients), mail_subject, mail_body)
        session = smtplib.SMTP(self.SMTP_RELAY)
        results = session.sendmail(mail_from, recipients, msg)
        session.quit()
        return results
        
    def send_summary_mail(self):
        '''
            Send an email with the job summary
        '''
        COMMASPACE = ', '
        if self.is_success():
            status = 'SUCCESS'
        else:
            status = 'FAILURE'

        recipients = self.MAIL_TO.split(",")

        #Send to failure email if it is different than the notice email address.
        if self.MAIL_TO_FAIL and not self.is_success() and self.MAIL_TO != self.MAIL_TO_FAIL:
            recipients.append(self.MAIL_TO_FAIL)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = '%s : Job %s completed with %s' % (self.HOSTNAME, self.config_file, status)
        msg['From'] = self.MAIL_FROM
        msg['To'] = COMMASPACE.join(recipients)

        results = self.print_results(True, False)
        mail_body = ''
           
        for line in results['summary']:
            mail_body = mail_body + line + '\n'
        for line in results['summary_verbose']:
            mail_body = mail_body + line + '\n'
        
        html = '''
        <html>
        <head></head>
        <body>
        <pre style="font-size:large">%s</pre>
        </body>
        </html>
        ''' % mail_body
        part1 = MIMEText(mail_body, 'plain')
        part2 = MIMEText(html, 'html')
        
        msg.attach(part1)
        msg.attach(part2)


        session = smtplib.SMTP(self.SMTP_RELAY)
        session.sendmail(self.MAIL_FROM, recipients, msg.as_string())
        session.quit()
        
        #self.send_mail(self.MAIL_TO, self.MAIL_FROM, mail_subject, mail_body)
        
    def detect_cpus(self):
        """
         Detects the number of CPUs on a system. Cribbed from pp.
         """
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
           if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
               # Linux & Unix:
               ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
               if isinstance(ncpus, int) and ncpus > 0:
                   return ncpus
           else: # OSX:
               return int(os.popen("sysctl -n hw.ncpu").read())
        # Windows:
        if "NUMBER_OF_PROCESSORS" in os.environ:
               ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
               if ncpus > 0:
                   return ncpus
        return 1 # Default
