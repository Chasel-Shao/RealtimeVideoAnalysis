import videoCollect
import videoProcess


topic = 'videoframe'
videoId = 'tvShow'
videoCollect.collect(topic, videoId)
videoProcess.process(topic)
# Test live streaming
videoCollect.playVideo('rtmp://aistranger.net:1935/hls')

