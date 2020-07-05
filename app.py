#!/usr/bin/env python
import pika
import time
from flask import Flask, json, request
from threading import Lock


def create_mq_channel():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
    pika.ConnectionParameters('192.168.178.30',5672,'/',credentials,frame_max=10000))
    
    channel = connection.channel()

    channel.queue_declare(queue='observations.queue', durable=True,arguments={'x-message-ttl' : 20000})

    return channel

app = Flask(__name__)
channel = create_mq_channel()
lock = Lock()

# validate web server from meraki
@app.route('/', methods=['GET'])
def get_validator():
    return "820c23d07e25bf53b55fb557901109dd7da0d9c5"
# 18621

# Accept CMX JSON POST
@app.route('/', methods=['POST'])
def get_cmxJSON():

    if not request.json or not 'data' in request.json:
        return("invalid data",400)

    cmxdata = request.json
    #pprint(cmxdata, indent=1)
    
    # Verify secret
    if cmxdata['secret'] != '19820205':
        print("secret invalid:", cmxdata['secret'])
        return("invalid secret",403)


    # Verify version
    if cmxdata['version'] != '3.0':
        print("invalid version")
        return("invalid version",400)

    # Add message to queue


    with lock:
        channel.basic_publish(exchange='',
                        routing_key='observations.queue',
                        body=json.dumps(cmxdata['data']),
                        properties=pika.BasicProperties(app_id='example-publisher',
                                            content_type='application/json'))




    # Return success message
    return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 


@app.route('/health')
def status():
    # Return success message
    return json.dumps({'name': 'meravki-v3-receiver', 'status': 'online', 'pid': 123})

