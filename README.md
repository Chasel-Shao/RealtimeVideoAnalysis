## Realtime Video Analysis

This repo shows my big data project about real-time video pipeline. The code is written in Python. 



## Frameworks

In this project, It contains various frameworks that includes the Kafka, Zookeeper, Spark, OpenCV. Kafka is used for data transportation, Zookeeper manages the cluster, Spark for distributed computation, OpenCV for image processing.



## Features

The live video comes from a link of RTMP, and through the processing of the live video, a dozen frames of images are sent out through kafka. Spark receiving the streaming data, and computes the data and combines it with OpenCV to restore the data to short video. According to the size of window, the live video is clipped into short video of the same duration and stored in the output directory.











