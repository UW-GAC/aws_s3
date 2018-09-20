## aws_s3 ##

This project contains python scripts and files associated with TOPMed's AWS S3 and AWS SQS.  The project includes the following files:
1. `sqs_examples.py` - examples using boto3 to access sqs and s3
2. `syncs3.py` - a script that polls sqs for messages to sync data from s3 to local folder
3. `syncs3.service` - linux systemd service to create a daemon process executing syncs3.py
4. `upload_tree_s3.py` - python script to upload a directory tree on the local computer to s3
5. `download_tree_s3.py` - python script to download a tree from s3 to local root folder
6. `sqs_tool.py` - a script for sending a test message to sqs or for purging the queue
7. `awscontext.py` - a python module defining a python class for managing the various context options when uploading data
8. `awscontext.json` - a configuration file specifying contexts for various AWS environments and accounts
9. `sqsmsg.py` - a python module defining the structure of the messages sent to the SQS message queue when an upload has been completed
### upload_tree_s3.py ###
Because this script is used more frequently and by various users, a more detailed description is presented here.

This python script utilizes boto3 (an python API to AWS services) to upload files and their associated directory tree(s) to an S3 bucket.  By default, when the upload is complete a message is sent to an AWS SQS (Simple Queue Service) queue where an application on the AWS docker image is notified and the changed files are downloaded to the NFS volume.

There are at least three different AWS accounts where S3 maybe uploaded and each AWS account has different security keys for permission to access the S3 service.  Each AWS account may have different S3 buckets and different here are different buckets and different SQS message queues. To support this diverse AWS environment, the script uses a "context" configuration file as well a command line options.  The provided context configuration file is `awscontext.json`.  There are contexts defined for the UW AWS account; the NHLBI development account; and the NHBLI data account.  Each context includes defaults for:
1. Name and URL address of the message queue
2. Bucket name
3. AWS credential profile name

There are configuration command line options to `upload_tree_s3.py` for specifying:
1. The file name of the context configuration file
2. Profile name (over-riding the configuration file)
3. Bucket name (over-riding the configuration file)
4. Message queue name (over-riding the configuration file)

There are additional options to `upload_tree_s3.py` for copying or uploading to S3 including:
1. Include only specified files (e.g., '*.py')
2. Exclude specified files (e.g., '*.vcf')
3. Include only specified folders (e.g., '*.config')
4. Exclude specified folders (e.g., '*.log, *.results, *.data')
5. Recursively copy subfolders
6. Copy only changed files

A brief summary of all the options are available by specifying `--help` option.  Another useful option is `--test` (or `-T`) to output morewhat will be uploaded without actually executing the upload. See the examples below for additional help.

### Examples ###
1. <i>Example 1</i> - Upload all the files in the cwd (where the cwd is a directory under /projects).  Subdirectories are not uploaded.
```{r}
cd /projects/analysts/kuraisa
upload_tree_s3.py
```
2. <i>Example 2</i> - Without actually executing the upload print a summary for recursively uploading all new or changed files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -T
```
3. <i>Example 3</i> - Upload example 2.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline
```
4. <i>Example 4</i> - Without actually executing the upload print a summary for recursively uploading all new or changed R files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -i "*.R" -T
```
5. <i>Example 5</i> - Without actually executing the upload print a summary for recursively uploading all new or changed excluding *.py files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -e "*.py" --test
```
6. <i>Example 6</i> - List all the messages in the SQS queue
```{r}
sqs_tool.py -l
```
7. <i>Example 7</i> - Send a default message to the SQS queue
```{r}
sqs_tool.py -s
```
7. <i>Example 7</i> - Purge all the messages in the SQS queue
```{r}
sqs_tool.py -P
```
8. <i>Example 8</i> - Upload all the files in the cwd (where the cwd is a directory under /projects) but do not send a message to sqs to synchronize s3 to nfs on aws.  Subdirectories are not uploaded.
```{r}
cd /projects/analysts/kuraisa
upload_tree_s3.py -N
```
9. <i>Example 9</i> - Recursively upload from the cwd (where the cwd is a directory under /projects) but do not include the results, log, plots or data directories and do not include an files starting with "fail"
```{r}
cd /projects/analysts/kuraisa
python2.7 ~/aws_s3/upload_tree_s3.py -E results,log,plots,data -e "fail*"
```
10. <i>Example 9</i> - Recursively upload from the cwd (where the cwd is a directory under /projects) but do not include the results, log, plots or data directories and do not include an files starting with "fail".  Specify a different config file.
```{r}
cd /projects/analysts/kuraisa
python2.7 ~/aws_s3/upload_tree_s3.py -E results,log,plots,data -e "fail*" -C test.cfg
```
11. <i>Example 9</i> - Recursively upload new files from a specified folder and its subfolders to a specifically named bucket and using a specified credentials profile (which must exist in ~/.aws/credentials)
```{r}
cd /projects/analysts/kuraisa
python2.7 ~/aws_s3/upload_tree_s3.py -s /project/topmed/simdata -r -c -profile uw -b uw-simdata
```
