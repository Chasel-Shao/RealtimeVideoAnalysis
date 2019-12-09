import videoCollect
import videoProcess


topic = 'videoframe'
videoId = 'tvShow'
videoCollect.collect(topic, videoId)
videoProcess.process(topic)

