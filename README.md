Uses association/frequent itemset mining to generate and score ABAC policies from AWS CloudTrail logs. 

Dependencies
----
* Python 3.6 (Some versions of MacOS and Python appear to have problems with the pool.starmap method used in this code not properly exiting so Windows or Linux should be used for running the python code)
* MongoDB 3.6
* Elasticsearch 6.3
* https://github.com/MWSanders/CloudTrailIngestor these instructions assume the CloudTrailIngestor project has already been run to ingest AWS CloudTrail logs into MongoDB

Basic concepts
----
The mining process operates in a sliding window manner to descritize time into manageable chunks.  Experiments suggest that 30 days is a fair observation period size for mining patterns from audit logs, and 1 day is a fair operation period time used for scoring policies.  For this program, each observation period window is represnted by a single job which contains all the information necessary to mine a policy from the observation time window.

A job queueing architecture is used to distribute the work of mining and scoring across multiple machines.  These instrucitons will focus on running the code on a single machine for simplicity.  When running in a distributed environment, there is only one Mongo instance shared between the worker nodes, but each worker node will run its own Elasticsearch instance.

The MongoDB instance is used to store the job queue information, audit log events (with indexes for quick searching), and the results of mined poliices.

The Elasticsearch instance is used to store the universe of attributes and their values, this helps speed up the process of scoring policies.

Getting Started 
----
1. Run pip install -r requirement.txt to install dependencies. 
1. Modify src/config/config.py Elasticsearch and Mongo settings, defaults assume they are both running on a localhost.
1. Run src/model/EnvLogUniverseGenerator.py  This program builds the parameter universe referred to as xi\` in the paper.  Preindexing the parameter universe of attributes and values helps speed up calculating the policy scores.  However, defining the parameter universe is a complicated step.  The default field values used in EnvLogUniverseGenerator.py were created based on a separate feature selection process of a private dataset.  Both automated methods and manually looking through the AWS documentation was performed to identify dependencies between the parameters.  Important settings in building the param universe:
    * `r_valid_fields` the list of selected attributes/features to be used from the logs  
    * `dependent_field_lists` a 2d array of dependent fields.  Certain attribute values are only valid when present with other attribute values, for example, the eventName, such as DeleteInstance, is only valid with with ec2 eventSource.  So there is a dependency between these fields that would be represented as ['eventName', 'eventSource'] within the `dependent_field_lists` 2d array.
    * `fields_to_bin` extracted from the `field_subsets` tuple describes which fields to bin assuming there is logic to bin them within src/model/EventNormalizerNg.py
    * `pop_binned_fields` removes the original fields from the log after they have been binned and `add_missing_fields` is needed to clean up log entries for processing later.  For best results, both of these should be true. 
1. Modify src/job/job_generator.py which generates the self-contained jobs which each worker node runs.  job_generator.py will generate multiple jobs based on the start and end time of the data to mine and the observation period size.  Each of the generated jobs represents a single window in the sliding window paradigm that runs across the data set.  Settings enclosed within lists are used to create runs with multiple parameters, for example `'itemset_freq':  [0.05, 0.1]` will generate jobs for all test window periods with both `itemset_freq: 0.05` and `itemset_freq: 0.1`.  Using many values across multiple job config parameters can quickly lead to a large number of jobs being created.
The most important settings in job_generator.py are:
    * calendar_start: Start of the log mining period.  Sometimes it is useful to run smaller tests than all available log data so the the job generating code does not determine this automatically.  It can easily be found but running `db.getCollection('events').find({}).sort({eventTime:1}).limit(1)` in Mongo to get the earliest `eventTime` in the log data.
    * calendar_end: Run `db.getCollection('events').find({}).sort({eventTime:-1}).limit(1)` to get the latest `eventTime` in the log data.
    * abac_params.generation_param_info_id: Unique identifier corresponding to the parameter universe generated by the EnvLogUniverseGenerator.
    * abac_params.itemset_freq:  The itemset frequency limit, also referred to as minimal support, when sent to the FP-growth algorithm used to identify frequent itemsets.  Setting this too low will result in many poor quality patterns being identified, setting it too high may result in missing the highest quality rule for the job window.
    * abac_params.metric.coverage_rate_method: The coverage rate method explored in Figure 4 of the paper, uncovered_all_logs_count is |Luncov|, the best perfroming method.
    * abac_params.metric.type: Candidate evaluation metric to use (compared in Figure 3 of the paper), arithmetic_mean corresponds to C-Score.
    * abac_params.metric.beta: Although called beta in the code, this value corresponds to the omega value in the C-Score (and other metrics) which is the weighting between CoverageRate and OverPrivilegeRate.
    * obs_days: Size of the observation period windows, in days 
    * opr_days: Size of the operation period windows, in days        

    A unique `config hash` value generated based on these parameters and appended to the job name.  This provides a way of separating the jobs, policies, and scoring results when queing up multiple experiments with different input parameters.
1. Run src/job/job_generator.py which will populate the `abac_job_queue` collection in Mongo with job tasks for worker nodes to execute.
1. Run src/job/job_executor.py which will pick a single status:NEW job from the `abac_job_queue` and run it, this is useful for debugging purposes and will write log output to the console.  When operating in a distributed environment with multiple workers, run src/main.py to run a worker node that will continually poll that `abac_job_queue` for NEW jobs and write the output to a log file based on the hostname.  
    * `abac_job_queue` contains jobs to run, each job corresponds to a time window in the sliding window.
    * `abac_policies` after the mining process is complete, the resulting policy is stored in this collection.
    * `abac_scores` after storing a policy, the job_executor will call src/eval/EnvPolicyEvaluator.py to score that policy, scoring results stored in this collection.
1. Run src/job/ScoreAggregator.py which will average all scores associated with each config hash and write the result to the `abac_aggregate_scores` collection.  This provides a final summary result includes True Positive, False Positive, True Negative, False Negative, Precision, Recall, etc. associated with the chosen parameters for an experiment (itemset_freq, beta, parameter universe, etc.)

Reference
----
Mining Least Privilege Attribute Based Access Control Policies.   
By Matthew Sanders and Chuan Yue.  
Annual Computer Security Applications Conference (ACSAC), 2019.  
