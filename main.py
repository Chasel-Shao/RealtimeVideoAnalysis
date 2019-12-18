import videoCollect
import videoProcess


topic = 'videoframe'
videoId = 'tvShow'

# 1. Collect Data
videoCollect.collect(topic, videoId)

# 2. Process Data
videoProcess.process(topic)


