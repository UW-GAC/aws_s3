## aws_s3 ##

This project contains python scripts and files associated with TOPMed's AWS S3 and AWS SQS.  The project includes the following files:
1. `sqs_examples.py` - examples using boto3 to access sqs and s3
2. `syncs3.py` - a script that polls sqs for messages to sync data from s3 to local folder
3. `syncs3.service` - linux systemd service to create a daemon process executing syncs3.py
4. `upload_tree_s3.py` - python script to upload a directory tree on the local computer to s3
5. `download_tree_s3.py` - python script to download a tree from s3 to local root folder
5. `sqs_tool.py` - a script for sending a test message to sqs or for purging the queue

### Examples ###
1. <i>Example 1</i> - Upload all the files in the cwd (where the cwd is a directory under /projects).  Subdirectories are not uploaded.
```{r}
cd /projects/analysts/kuraisa
upload_tree_s3.py
```
2. <i>Example 2</i> - Without actually executing the upload print a summary for recursively uploading all new or changed files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -S
```
3. <i>Example 3</i> - Upload example 2.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline
```
4. <i>Example 4</i> - Without actually executing the upload print a summary for recursively uploading all new or changed R files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -i "*.R" -S
```
5. <i>Example 5</i> - Without actually executing the upload print a summary for recursively uploading all new or changed excluding *.py files in the specified folder and its subfolders.
```{r}
upload_tree_s3.py -r -c -s /projects/topmed/analysts/kuraisa/analysis_pipeline -e "*.py" -S
```
6. <i>Example 6</i> - List all the messages in the SQS queue
```{r}
sqs_tool.py -l
```
7. <i>Example 7</i> - Send a default message to the SQS queue
```{r}
sqs_tool.py -l
```
7. <i>Example 7</i> - Purge all the messages in the SQS queue
```{r}
sqs_tool.py -P
```
