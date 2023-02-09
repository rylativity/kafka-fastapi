from fastapi import FastAPI, Response, status
from fastapi.logger import logger as log
from fastapi.responses import RedirectResponse
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
from kafka.admin import NewTopic

import json
import logging
import os

gunicorn_logger = logging.getLogger('guincorn.error')
log.handlers = gunicorn_logger.handlers

if __name__ != "main":
    log.setLevel(gunicorn_logger.level)
else:
    log.setLevel(logging.DEBUG)

KAFKA_ENV_VAR_PREFIX = "KAFKA_"
kafka_conf = {}
var:str
for var in os.environ.keys():
    if var.upper().startswith(KAFKA_ENV_VAR_PREFIX):

        kafka_var_name = var.replace(KAFKA_ENV_VAR_PREFIX, "", 1).lower()

        kafka_conf[kafka_var_name] = os.environ.get(var)
producer = KafkaProducer(**kafka_conf)
admin_client = KafkaAdminClient(**kafka_conf)


app = FastAPI(debug=True)

@app.get("/")
def root():
    return RedirectResponse("/docs")

@app.get("/topics")
def list_topics():

    return admin_client.list_topics()

@app.post("/topics/{topic}")
def create_topic(name:str,response: Response, num_partitions:int = 1, replication_factor:int = 1):

    topic_object = NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor)

    admin_client.create_topics(new_topics=[topic_object])

    response.status_code = status.HTTP_201_CREATED

    return {"status_code":response.status_code, "message":f"Topic {name} created successfully"}

@app.delete("/topics/{topic}")
def delete_topic(name:str,response: Response):

    admin_client.delete_topics(topics=[name])

    response.status_code = status.HTTP_200_OK

    return {"status_code":response.status_code, "message":f"Topic {name} deleted successfully"}

@app.post("/produce/{topic}")
def produce_message(message: dict, topic: str, response: Response):

    message_value = json.dumps(message).encode("utf8")

    producer.send(topic=topic, value=message_value)

    response.status_code = status.HTTP_201_CREATED

    return message

@app.get("/messages/{topic}")
def get_messages(topic: str, response: Response, limit:int=10):

    consumer = KafkaConsumer(topic, auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=1000, **kafka_conf)

    messages = []
    for message in consumer:
        messages.append(json.loads(message.value.decode("utf8")))

        if len(messages) == limit:
            break

    return messages
