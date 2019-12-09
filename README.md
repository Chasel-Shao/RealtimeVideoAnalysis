## Realtime Video Analysis

This repo shows my bigdata project about real-time video pipeline. The code is written in Python. 



## Frameworks

In this project, I use various frameworks that includes the Kafka, Zookeeper, Spark, Opencv. Kafka is used for data transportation, Zoo manages the cluster, Spark for distributed computation, Opencv for image processing.



## Functions

The live video comes from a link of RTMP, and through the processing of the live video, a dozen frames of images are sent out through kafka. Spark receiving the streaming data, and computes the data and combines it with OpenCV to restore the data to short video. According to the size of window, the live video is clipped into short video of the same duration and stored in the output directory.











