import boto.sqs
import json
from boto.sqs.message import Message
import uuid
from boto.sqs.queue import Queue

with open("config.json") as fh:
    config = json.load(fh)

conn = boto.sqs.connect_to_region(
    config["task_sqs"]["region"],
    aws_access_key_id=config["aws"]["access_key"],
    aws_secret_access_key=config["aws"]["secret_key"]
)

#assert config["task_sqs"]["queue"]
#queue = Queue(connection=conn, url=conn.get_queue(config["task_sqs"]["queue"]))
#queue = conn.get_queue(config["task_sqs"]["queue"])
#assert queue



job_id = str(uuid.uuid1())
msg_body = json.dumps({
    "task_type": "map",
    "job_id": job_id,
    "map_index": 0,
    "account": "181239768896",
    "year": 2016,
    "month": 4,
    "bucket": 0,
    "reducers": 1,
    "output_dir": "s3://%s/%s%s" % (config["s3"]["bucket"], config["s3"]["prefix"], job_id),
    "func": "function map(obj,yield){ yield(obj.userAgent); }"
})

conn.send_message(
    Queue(url=config["task_sqs"]["url"]),
    msg_body
)