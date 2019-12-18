from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import globalVariables
import numpy as np
import base64
import json
import os
import cv2
import pkg_resources
import requests


def faceplusplus_api(img_base64):
    __data = {"api_key": globalVariables.API_KEY,
              "api_secret": globalVariables.API_SECRET,
              "image_base64": img_base64}
    response = requests.post(url=globalVariables.URL, data=__data)
    faces = response.json()["faces"]
    data = base64.b64decode(img_base64)
    nparr = np.fromstring(data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    for face in faces:
        coordinate = face["face_rectangle"]
        (x, y, w, h) = (coordinate['top'], coordinate['left'], coordinate['width'], coordinate['height'])
        cv2.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 3)
    return img


haar_xml = pkg_resources.resource_filename('cv2', 'data/haarcascade_frontalface_default.xml')
face_cascade = cv2.CascadeClassifier(haar_xml)


def __analysis(img_np):
    gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, 1.1, 4)
    for (x, y, w, h) in faces:
        cv2.rectangle(img_np, (x, y), (x + w, y + h), (255, 0, 0), 3)


def __process_video_data(list):
    print("Incoming data...")
    videoId = list[0]["videoId"]
    row = list[0]["row"]
    col = list[0]["col"]
    filename = "output/" + videoId + "-" + str(list[0]["timestamp"]) + ".avi"
    fps = globalVariables.GLOBAL_FPS
    fourcc = cv2.VideoWriter_fourcc(*"MJPG")
    video_writer = cv2.VideoWriter(filename, fourcc, fps, (row, col))
    for json in list:
        code = json["data"]
        data = base64.b64decode(code)
        nparr = np.fromstring(data, np.uint8)
        img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        __analysis(img_np)
        video_writer.write(img_np)

    video_writer.release()
    print("Save video: " + filename)


def __map_func(c):
    list_in_order = c.collect()
    __process_video_data(list_in_order)


def process(topic):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar pyspark-shell'
    brokers = globalVariables.GLOBAL_BROKER
    conf = SparkConf().setMaster("local[2]").setAppName("VideoProcess")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, globalVariables.GLOBAL_BATCH_DURATION)
    stream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers})

    processed = stream.map(lambda x: x[1]) \
        .window(globalVariables.GLOBAL_WINDOW_DURATION, globalVariables.GLOBAL_SLIDE_DURATION) \
        .map(json.loads) \
        .map(lambda x: (x['timestamp'], x)) \
        .transform(lambda rdd: rdd.sortByKey(ascending=True)) \
        .map(lambda x: x[1])

    processed.foreachRDD(__map_func)

    print("Consuming data...")

    ssc.start()
    ssc.awaitTermination()

