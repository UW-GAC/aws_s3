import boto3
client = boto3.client('sqs')
s3msg = client.receive_message(
   QueueUrl='https://sqs.us-west-2.amazonaws.com/988956399400/s3_test',
   AttributeNames=['All'],
   MessageAttributeNames=['All'])
rh2='AQEBruNeRqMRs14Je39r3Pp9noyl6SUM/cXXQNFEf8B4VAt1q25/k2HCAv3XSZ+zZvEyOqfaB5Q85GtWma0iI0aFwEtA/IGiIATkNinCJ67b8RmcNHjUCmxQEcfUfJ4mZ2tbFYjABTfq/pctpt3jwuQJxPF+6kVUFEBppA+9nJ3ggz6L8MLortoXgUxqhDg1tw592jUIY+ZVjZ3+88ZzIkhVM1X67q3Fm+VavJOhYh4+91KNWSox0W85fwNEtobGu53GcRuQkQ9Siu6YCBF6G8bvQbqRSbA/YLPzhdiNpCJXkcstxmYGzSk6HJ305pF34ZXgktFEYWNgLu/1aOi9g9HtB5AvEYjBLFA494XqZCkX5jKyi44NEgX91CEKBvxuZTyKQegh8mywoJ/kCJMrRJ2TVA=='
qurl='https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
res=client.delete_message(QueueUrl=qurl,ReceiptHandle=rh2)

res=client.purge_queue(QueueUrl=qurl)
type(s3msg['Messages'][0]['Body'])
body=ast.literal_eval(s3msg['Messages'][0]['Body'])
body['object']
body['Records'][0]
body['Records'][0]['s3']
body['Records'][0]['s3']['object']
s3msg['Messages'][0]['ReceptHandle']
s3msg['Messages'][0]['ReceiptHandle']
import readline
nhi=readline.get_current_history_length()


// send message
import boto3
client=boto3.client("sqs")
qurl='https://sqs.us-west-2.amazonaws.com/988956399400/s3_test'
response=client.send_message(MessageBody='update')
response=client.send_message(QueueUrl=qurl,MessageBody='update')
response
