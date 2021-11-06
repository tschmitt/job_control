# Job Control

Job Control is a Python utility that will execute a simple or complex job flow on a Linux server. It takes care of logging, notification, and concurrency allowing you to focus on job flow.

The goal is to have a simple, but flexible job controller that can handle complex job flow with a simple JSON configuration. No database is required, which provides transporatability.

Anyone that can configure a JSON file can use job_control. No Python knowledge is required.

Requirements

- Python 2.6 - 3.8
- [[future](https://pypi.org/project/future/)] for Python < 3

## Features

- JSON configuration file
- Configurable path
- Simulate mode
  - Individual steps
  - Entire job
- Configurable flow
  - Serial
  - Parallel
  - Serial/parallel
- Configurable concurrency
  - Default to the number of cores
- Variable replacement at runtime
- Run-time variable overwriting
  - Command line JSON snippet
  - JSON file
- Various system variables available
- Remote execution via ssh
- Configurable email addresses
  - Notices
  - Failures
- Configurable SMTP relay server
  - Defaults to localhost
- Step types
  - Operating System commands
  - Internal
    - Email
    - Sleep
- Graceful job canceling
- Selective step enabling/disabling
- Runtime step disabling
- Logging
  - Detailed job-level logging
  - Step running summary
  - Step output logging
  - Configurable log path
  - Job summary
- Email summary on SUCCESS/FAILURE
  - success email is optional
- Configurable queue monitoring interval
  - Default is 1 second
- Execute sub jobs as a step

## Job Configuration

The job and steps are defined in a JSON format configuration file.

Any variable may be defined in the variables object and system variables may be overwritten. These variables are then utilized to change job behavior or for variable replacement in the steps.

You can create a very reusable configuration file by passing in parameters at runtime.

Variable replacement is similar to Linux variables and uses "\$" to prefix the variable name. "\$\$" may be used to escape a dollar sign. Variable replacement is performed at job initiation. No changes are allowed after job start.

### System variables

- concurrency - Defaults to the number of cores in the server
- config_file - The name of the configuration file
- date - ('%Y_%m_%d')
- date_time - '%Y%m%d_%H%M%S'
- date_time_2 - '%Y%m%d-%H%M%S'
- date_time_3 - '%Y%m%d%H%M%S'
- date_time_4 - '%Y-%m-%d %H:%M:%S'
- date_time_friendly - 'Tue Jun  5 11:01:10 2012'
- hostname - Short hostname
- hostname_fqdn - Fully qualified hostname
- mail_from - Default: hostname@mail_from_domain
- mail_from_domain - Is appended to HOSTNAME and used as default mail_from.
- mail_to - Required. Email address for general notices. Single address or comma delimited list of email addresses.
- mail_to_fail - Required. Email_to address for job failure. This email is sent, in addition to - the notice email, if it is different than the notice email address and is useful for sending text messages for alerting.
- smtp_relay - Default: localhost

### Step configuration

- key - This is a string unique identifier. It is common to use numerical values to logical group steps.
- type - The step type
  - os - Any operating system command
  - internal
    - task
      - send_mail - Sends an email. See JSON example for usage.
      - sleep - Sleeps for x seconds. See JSON example for usage.
- dependencies - Array of steps that must successfully complete before this step can execute.
- name - A friendly name for this step. This will appear in logs etc.
- task - The actual work to be performed. This can be any operating system command or a special - keyword for internal step types.
- enabled - (true | false) - Any step may be selectively disabled for a job execution. This is most useful during a re-run of a failed job. Any successful steps should be set to "false" and the job re-executed. The entire job flow will be processed, but the disabled steps will simply show as "simulated" and behave like they completed successfully.
- resultcode_allowed - Default: 0. Pass in an array of allowed successful result codes. This is useful for edge cases where a non-zero may be considered a success.
- This example configuration displays most of the options:
  - comment - This is simply to comment the JSON file and is ignored by job control.
  - The "database" variable is set for variable replacement as "$database"
  - "concurrency" is over-written to "2". Only a maximum of two processes will execute in parallel, even if more parallel steps are defined in the configuration file.
  - mail_to and mail_from are over-written as appropriate for this job. This is a comma-delimited - list of email addresses).
  - Single and multiple dependencies are set for various steps.
  - Step 200 uses the special keyword "ALL" dependency. This step will only execute on successful completion of all other steps.

  > Only a single step per job is allowed to use the special keyword "ALL". The job will never complete if multiple steps use this word.

- Step success is established by the Linux result code.
  - zero = success
  - non-zero = failure
  - resultcode_allowed can override this

  > More complex OS command may involve nested quotes and special characters. These can be tricky, as proper escaping may be required using "\". shlex.split(s) may assist in creating a proper string. See [http://docs.python.org/library/subprocess.html#popen-constructor](http://docs.python.org/library/subprocess.html#popen-constructor) . Another awesome tool is: [https://www.freeformatter.com/json-escape.html](https://www.freeformatter.com/json-escape.html) . This web page will save your sanity.

### Example config

``` JSON
{
    "variables": {
        "database": "test_db",
        "concurrency": "2",
        "mail_to": "someone@somewhere.com",
        "mail_to_fail": "someone_text@somewhere.com",
        "mail_from": "job_control_test@somewhere.com"
    },
    "steps": {
        "103": {"enabled": true,  "dependencies": [],      "name": "step 103", "task": "python ./test.step1.py $database", "type": "os", "comment": "test"},
        "104": {"enabled": true,  "dependencies": ["103"], "name": "step 104", "task": "python ./test.step1.py", "type": "os"},
        "105": {"enabled": true,  "dependencies": ["104"], "name": "step 105", "task": "ls -l", "type": "os"},
        "106": {"enabled": true,  "dependencies": ["105"], "name": "step 106", "task": "python ./test.step2.py", "type": "os"},
        "107": {"enabled": true,  "dependencies": [],      "name": "step 107", "task": "python ./test.step2.py", "type": "os"},
        "108": {"enabled": true,  "dependencies": [],      "name": "step 108", "task": "python ./test.step2.py", "type": "os"},
        "109": {"enabled": false, "dependencies": [],      "name": "step 109", "task": "python ./test.step2.py", "type": "os"},
        "110": {"enabled": true,  "dependencies": [],      "name": "step 110", "task": "python ./test.step2.py", "type": "os"},
        "111": {"enabled": true,  "dependencies": [],      "name": "step 111", "task": "python ./test.step1.py", "type": "os"},
        "112": {"enabled": true,  "dependencies": [],      "name": "step 112", "task": "python ./test.step2.py", "type": "os"},
        "113": {"enabled": true,  "dependencies": ["111","112"], "name": "step 113", "task": "python ./test.step2.py", "type": "os"},
        "900": {"enabled": true,
            "task": "send_mail",
            "name": "Send email for step 107",
            "dependencies": ["107"],
            "detail": {"mail_to": "$mail_to", "mail_from": "$mail_from", "mail_subject": "Yay! Step 107 just completed on $hostname", "mail_body": ""},
            "type": "internal"
            },
        "901": {"enabled": true, "task": "sleep",       "name": "Sleep for a while", "dependencies": ["900"], "detail": {"seconds": "10"}, "type": "internal"},
        "200": {"enabled": true, "dependencies": "ALL", "name": "step 200", "task": "python ./test.step2.py", "type": "os"}
    }
}
```

## Usage

```txt
Parameters:
        -p, --path
                The directory where the configuration file is located.
                    (Default = ./)
        -l, --log_path
                The directory where the log files will be written.
                    (Default = --path/logs)
        -c, --config
                Configuration file name.
                Convention: <name>.conf.json
        -d, --delay
                Length of sleep delay (seconds) between main loop iterations.
                    (Default = 1 )
        -D, --disabled
                Comma delimited list of steps to disable
        -e, --email
                Email_to address for job failure
        -E, --Extras
                Additional parameters in JSON format. These are variables that are passed through to the Job.
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
```

### Examples

Runs a job in the current directory with all defaults.

```bash
python3 -u -B run_job.py -c job1.conf.json
```

Runs a job in the current directory with all defaults and logs the job execution.

```bash
python3 -u -B /home/someuser/scripts/job_control/run_job.py -p /tmp/jobs/refresh -c refresh.conf.json 2>&1 | tee /tmp/jobs/refresh/logs/refresh.`date +%Y%m%d`.log
```

Runs a job in a defined directory with custom settings.

```bash
# Example 1
python3 -u -B run_job.py -p /home/jobs/  -c job2.conf.json -d 10 -v -s --no_success_email -E '{"test_var": "hello"}'
# Example 2
python3 -u -B run_job.py -p /home/jobs/  -c job2.conf.json -d 10 -v -s --no_success_email -E "{\"test-var\": \"$TEST_VAR\"}"
```

Passing shell variables into a job

```bash
# Create directory
LOG_DATE=`date +%Y%m%d_%H%M`
LOG_PATH=/tmp/serve_refresh/logs/$LOG_DATE
mkdir -p $LOG_PATH

# Launch refresh
python3 -u -B /home/someuser/scripts/prod/job_control/run_job.py -p /home/someuser/scripts/refresh -c refresh.conf.json -l $LOG_PATH -E "{\"log_ts\": \"$LOG_DATE\"}" > $LOG_PATH/refresh.log
```

> The following parameters are important.

- The -u parameter when launching Python is important to obtain un-buffered STDOUT.
- The -B parameter when launching Python is important to NOT write .pyc files in the shared file path.

### Variables

A Job has several locations where variables may be set.

- Built-in defaults - Described above.
- JSON configuration file - Described above.
- JSON command line - Allows overwriting of built-ins on configuration file. This is a great way to save production settings in the config file and overwrite for development.It also provides a method to pass in variables at runtime.

Cascade

- JSON job configuration file overwrites Built-ins
- JSON --extras_file overwrites job configuration file
- JSON command line --extras overwrites --extras_file and job configuration file

### Logs

Each step will generate an individual log file containing STDOUT and STDERR from the process.

Logs will be created in a subdirectory /logs in the path specified by -p (--path) or in the directory specified by -l (--log_path). Appropriate permissions are required to create the path.

Output of the job can be logged using standard Linux utilities such as tee or by using redirection.
