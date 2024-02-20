from api import get_video, get_comments
from utils import constants

from kafka import KafkaProducer
# from confluent_kafka import Producer
import json
import argparse

producer = KafkaProducer(bootstrap_servers=['localhost:29092'], max_block_ms=5000)
video_topic = constants.video_topic_key
comment_topic = constants.comment_topic_key

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--save_sample", type=bool, default=True)
    args = parser.parse_args()

    video_ids = ["qpOt62UE6pU"]

    for video_id in video_ids:
        video = get_video(video_id)
        comments = get_comments(video_id)

        if args.save_sample:
            with open('sample_data/sample_video_fields.json', 'w') as f:
                json.dump(video, f, indent=4)

            with open('sample_data/sample_comment_fields.json', 'w') as f:
                json.dump(comments, f, indent=4)

        # producer.send(topic=video_topic, value=json.dumps(video).encode('utf-8'), key=video_id.encode('utf-8'))
        # producer.flush()

        # for comment in comments:
        #     producer.send(topic=comment_topic, value=json.dumps(comment).encode('utf-8'), key=comment['id'].encode('utf-8'))
        #     producer.flush()



