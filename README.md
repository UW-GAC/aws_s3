## aws_s3 ##

This project contains python scripts and files associated with AWS S3 and AWS SQS.  The project includes the following files:
1. `sqs_examples.py` - examples using boto3 to access sqs
2. `update_projects.py` - a script to upload data in /projects to s3 via aws cli (s3 sync and cp) and sends a message to sqs when upload completes
3. `check_download_s3.py` - a script running on the topmed docker instance that checks for messages in sqs and downloads from s3 if a message is found
4. `update_s3_base_projects.bash` - a bash script running from pearson for uploading the base set of files with /projects folder (e.g., freeze5b gds files)
5. `sqs_tool.py` - a script for sending a test message to sqs or for purging the queue
