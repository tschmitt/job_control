"""
SYNOPSIS

    jobs module
    

DESCRIPTION

    This contains the job class, which is used in the execution of a job.
    The job class also includes all the step functionality, which may be moved
        to a steps module and step class at a later date.
        
AUTHOR

    Terry Schmitt <tschmitt@schmittworks.com>

VERSION

    1.0.000
    
"""

import os
import pickle
import shlex
import signal
import json
import smtplib
from string import Template
from subprocess import Popen, STDOUT

from datetime import datetime
from copy import deepcopy
from collections import deque

#Not used but may use later for converting datetime
def encode_custom(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        return json.JSONEncoder.default(obj) 

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


    def __init__(self, path, config_file):
        '''
        Constructor
        '''
        self.path = path
        self.config_file = config_file
        #Build paths
        self.config_path = os.path.join(self.path, self.config_file)
        self.data_path = os.path.join(self.path, '%s.%s' %
                                     (self.config_file.split('.')[0], 'pkl'))
        self.logpath = os.path.join(self.path, 'logs')
        if not os.path.isdir(self.logpath):
            os.makedirs(self.logpath)
        
        #Constants
        self.STEP_STATUS_TEMPL = {'status': 'waiting', 'resultcode': None, 'queued_time':  None, 'start_time': None, 'stop_time': None, 'duration': 0}
        self.STATUSES = {
                     'waiting':  {'name': 'waiting', 'runnable': True, 'running': False, 'description': 'Step has not been queued yet'},
                     'queued':   {'name': 'queued', 'runnable': True, 'running': False, 'description': 'Step is in the queue and ready to run'},
                     'running':  {'name': 'running', 'runnable': False, 'running': True, 'description': 'Step is currently running'},
                     'complete': {'name': 'complete', 'runnable': False, 'running': False, 'description': 'Step completed without error'},
                     'disabled': {'name': 'disabled', 'runnable': False, 'running': False, 'description': 'Step has been disabled and will not be run'},
                     'failed':   {'name': 'failed', 'runnable': False, 'running': False, 'description': 'Step failed'},
                     'canceled': {'name': 'canceled', 'runnable': False, 'running': False, 'description': 'Step was canceled without being started. Most likely due to a dependency that failed. This step may be rerun'},
                     'aborted':  {'name': 'aborted', 'runnable': False, 'running': False, 'description': 'Step was aborted while it was running. Most likely due to Ctrl-C or a undandled exception. This step will likely require research before rerunning'}
                         }
        self.STEP_TYPES = {
                    'os': {'name': 'os', 'description': 'Spawns a new ayncronous process and execute the os level command with parameters'},
                    'internal': {'name': 'internal', 'description': 'Pre configured steps for routine functionality'}
                      }
        self.TASK_TYPES = {
                    'send_mail': {'name': 'send_mail', 'description': 'Sends an email. This is a blocking event'}
                      }        
        self.REPLACE_KEYS = ['task', 'name']
        
        #Load the job configuration
        self.load_config()

        #Preprocessing
        for step in self.steps:
            #Set up the default job_status for each step
            self.steps[step]['job_status'] = deepcopy(self.STEP_STATUS_TEMPL)
            #Check for ALL dependency and set
            if self.steps[step]['dependencies'] == 'ALL':
                self.steps[step]['dependencies'] = self.get_all_dependencies(step)
            #Replace variables in config
            d =  self.config['variables']
            #check these values for variable replacement
            for item in self.REPLACE_KEYS:
                #sanity check since these keys may not exist for each step
                if item in self.steps[step]:
                    t = Template(self.steps[step][item])
                    self.steps[step][item] = t.substitute(d)
            #Build args
            self.steps[step]['args'] = shlex.split(self.steps[step]['task'])
            if 'detail' in self.steps[step]:
                self.steps[step]['args'].append(self.steps[step]['detail'])
                
        #Defaults
        self.start_time = datetime.today()
        self.stop_time = None
        self.duration = 0  
        self.queue = deque([]) #Runnable steps
        self.processes = {} #Running and completed steps
        self.completed = [] #Sucessful steps
        self.failed = [] #Failed steps

    def cancel_children(self, step):
        '''
            Cancels the dependents of a step
        '''
        for step in self.steps:
            #If runnable
            if self.steps[step]['job_status']['status'] in ('waiting','queued'):
                #Update status to canceled
                self.steps[step]['job_status']['status'] = 'canceled'
                #Remove from queue if required
                if step in self.queue:
                    del self.queue[self.queue.index(step)]
                    
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
                      
                
    def dependencies_met(self, step):
        '''
            Checks that the dependency steps are complete
        '''
        if self.steps[step]['dependencies'] == None:
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
        
        keys = self.steps.keys()
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
    
    def is_success(self):
        if len(self.steps) == len(self.completed):
            return True
        else:
            return False
        
    def load_config(self):
        '''
            Loads the JSON Job configuration file
        '''
        
        try:
            f = open(self.config_path, 'r')
            try:
                self.config = json.load(f)
                self.steps = self.config['steps']
            except:
                f.close()
                print '%s : %s' % ('Could not parse config file', self.config_path)
                raise
        except IOError:
            print '%s : %s' % ('Could not open config file', self.config_path)
            raise
        except Exception:
            raise
        
    def monitor_processes(self, verbose):
        '''
            Iterates over processes and cleans up after success or fail of
                each process.
        '''
        for step in self.processes:
            #If complete
            if step not in self.completed and step not in self.failed:
                results = self.processes[step]['process'].poll()
                if results != None: #step is finished
                    #Close the out file
                    self.processes[step]['out'].close()
                    if results == 0:
                        result_str = 'COMPLETE'
                    else:
                        result_str = 'FAILED'
                    
                    #Append to completed and update status
                    self.complete_step(step, results)
                    
                    print '%s STEP %s: %s resultcode: %s duration: %s' % (datetime.today(), result_str, step, results, self.steps[step]['job_status']['duration'])
    def complete_step(self, step, results):
        '''
            Appends to completed and updates the status
        '''
        if results == 0:
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
            
    def print_results(self, verbose):
        '''
            Prints out a summary of the job results
        '''
        if verbose:
            print '*******************************************'
            print 'JOB DETAIL'
            print '*******************************************'
            print 'Completed Steps:'
            if not self.completed:
                print '     None'            
            for step in self.completed:
                print 'Step:', step
                print '     name:      ', self.steps[step]['name']
                print '     status :   ', self.STATUSES[self.steps[step]['job_status']['status']]['name']
                print '     resultcode:', self.steps[step]['job_status']['resultcode']
                print '     start:     ', self.steps[step]['job_status']['start_time']
                print '     stop:      ', self.steps[step]['job_status']['stop_time']
                print '     duration:  ', self.steps[step]['job_status']['duration']
            print '*******************************************'
            print 'Failed Steps:'
            if not self.failed:
                print '     None'
            for step in self.failed:
                print 'Step:', step
                print '     name:      ', self.steps[step]['name']
                print '     status :   ', self.STATUSES[self.steps[step]['job_status']['status']]['name']
                print '     resultcode:', self.steps[step]['job_status']['resultcode']
                print '     start:     ', self.steps[step]['job_status']['start_time']
                print '     stop:      ', self.steps[step]['job_status']['stop_time']
                print '     duration:  ', self.steps[step]['job_status']['duration']    
            print '*******************************************'
            print 'Canceled Steps:'
            if not self.get_canceled_steps():
                print '     None'            
            for step in self.get_canceled_steps():
                print 'Step:', step
                print '     name:      ', self.steps[step]['name']
                print '     status :   ', self.STATUSES[self.steps[step]['job_status']['status']]['name']
                print '     resultcode:', self.steps[step]['job_status']['resultcode']
                print '     start:     ', self.steps[step]['job_status']['start_time']
                print '     stop:      ', self.steps[step]['job_status']['stop_time']
                print '     duration:  ', self.steps[step]['job_status']['duration']    
            print '*******************************************'    
            print 'Aborted Steps:'
            if not self.get_aborted_steps():
                print '     None'            
            for step in self.get_aborted_steps():
                print 'Step:', step
                print '     name:      ', self.steps[step]['name']
                print '     status :   ', self.STATUSES[self.steps[step]['job_status']['status']]['name']
                print '     resultcode:', self.steps[step]['job_status']['resultcode']
                print '     start:     ', self.steps[step]['job_status']['start_time']
                print '     stop:      ', self.steps[step]['job_status']['stop_time']
                print '     duration:  ', self.steps[step]['job_status']['duration']    
            print '*******************************************'   
        print '*******************************************'
        print 'JOB SUMMARY'
        print '*******************************************'
        print 'Job:'
        print '    config file     ', self.config_path
        print '    start:          ', self.start_time
        print '    stop:           ', self.stop_time
        print '    duration:       ', self.duration
        print '    steps total:    ', len(self.steps)
        print '    steps completed:', len(self.completed)
        print '    steps failed:   ', len(self.failed)
        print '    steps canceled: ', len(self.get_canceled_steps())
        print '    steps aborted:  ', len(self.get_aborted_steps())        
        print '*******************************************'
    def process_queue(self, verbose):
        '''
            Launch each step in the queue
        '''
        while len(self.queue):
            #Remove from queue
            step = self.queue.popleft()
            
            #Update status
            self.steps[step]['job_status']['status'] = 'running'
            self.steps[step]['job_status']['start_time'] = datetime.today()   
            if self.steps[step]['type'] == 'os':
                #Launches a new process
                self.processes[step] = {}
                #Create log file
                outfile = '%s-%s.%s' % (self.config_file.split('.')[0], step, 'out')        
                self.processes[step]['out'] = open(os.path.join(self.logpath, outfile), 'wb')
                #Launch step
                self.processes[step]['process'] = Popen(self.steps[step]['args'], stdout=self.processes[step]['out'], stderr=STDOUT)
                if verbose:
                    print '%s STEP %s: %s' % (datetime.today(), 'SPAWNED', step)
            elif self.steps[step]['type'] == 'internal':
                if self.steps[step]['task'] == 'send_mail':
                    if verbose:
                        print '%s STEP %s: %s' % (datetime.today(), 'EXECUTED', step)
                    #Send email
                    results = self.send_mail(**self.steps[step]['args'][1])
                    #If the returned dictinary has members, set to 1
                    if results:
                        results = 1
                    else:
                        results = 0
                    self.complete_step(step, results)
                    if verbose:
                        print '%s STEP COMPLETE: %s resultcode: %s duration: %s' % (datetime.today(), step, results, self.steps[step]['job_status']['duration'])               
            else:
                raise InvalidTypeError(self.steps[step]['type'])
                    
    def queue_runnables(self):
        '''
            Places runnable steps in the queue
        '''
        for step in self.steps:
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
            This will probably not be used much, as it was primkarily written
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
                print '%s : %s' % ('Could not write config file', self.config_path)
                raise            
        except IOError:
            print '%s : %s' % ('Could not open config file', self.config_path)
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
            f = open(self.data_path, 'w')
            try:
                pickle.dump(data, f, 0)
            except:
                print '%s : %s' % ('Could not write data file', self.data_path)
                raise                  
        except IOError:
            print '%s : %s' % ('Could not open data file', self.data_path)
            raise
        
    def send_mail(self, mail_to, mail_from, mail_subject, mail_body):
        msg = 'From: %s\r\nTo: %s\r\nSubject: %s: %s\r\n\r\n%s' % (mail_from, mail_to, os.uname()[1].split('.')[0], mail_subject, mail_body)
        session = smtplib.SMTP('localhost')
        results = session.sendmail(mail_from, mail_to, msg)
        session.quit()
        return results
            