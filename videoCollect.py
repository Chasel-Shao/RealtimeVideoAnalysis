import cv2
import base64
import json
import time
import kafkaUtil
import threading
import numpy as np
import globalVariables


def __fetch_live_video(stream_addr, videoId, func):
    cap = cv2.VideoCapture(stream_addr)
    row = globalVariables.GLOBAL_ROW
    col = globalVariables.GLOBAL_COL
    interval = 1 / globalVariables.GLOBAL_FPS
    while True:
        time.sleep(interval)
        ret, frame = cap.read()
        if not ret:
            time.sleep(1)
            cap = cv2.VideoCapture(stream_addr)
            continue
        col = int(frame.shape[0] * row / frame.shape[1])
        frame = cv2.resize(frame, (row, col), interpolation=cv2.INTER_CUBIC)
        _, buffer = cv2.imencode('.jpg', frame)
        encodedJPG = base64.b64encode(buffer).decode('utf-8')
        t = int(round(time.time() * 1000))
        jsonObject = json.dumps({"videoId": videoId,
                                 "row": row,
                                 "col": col,
                                 "data": encodedJPG,
                                 "timestamp": t,
                                 "type": "jpg"})
        if callable(func):
            func(jsonObject)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()


def playVideo(stream_addr):
    cap = cv2.VideoCapture(stream_addr)
    row = 256
    col = 256
    while True:
        time.sleep(0.05)
        ret, frame = cap.read()
        if not ret:
            time.sleep(1)
            cap = cv2.VideoCapture(stream_addr)
            continue
        frame = cv2.resize(frame, (row, col), interpolation=cv2.INTER_CUBIC)
        _, buffer = cv2.imencode('.jpg', frame)
        # encode data
        encodedJPG = base64.b64encode(buffer).decode('utf-8')
        # decode data
        data = base64.b64decode(encodedJPG)
        nparr = np.fromstring(data, np.uint8)
        img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        cv2.imshow('video', img_np)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()


def collect(topic, videoId):
    producer = kafkaUtil.connect_kafka_producer()

    def func(json):
        kafkaUtil.publish_message(producer, topic, json)

    stream_addr = globalVariables.GLOBAL_STREAM_ADDRESS
    t = threading.Thread(target=__fetch_live_video, args=(stream_addr, videoId, func))
    t.start()
    print("Producing data")





