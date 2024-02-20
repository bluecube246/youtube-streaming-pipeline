from googleapiclient.discovery import build
from collections.abc import MutableMapping
import json



youtube = build(
    'youtube',
    'v3',
    developerKey=api_key
)


def flatten(dictionary, parent_key='', separator='_'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def get_video(video_id):
    request = youtube.videos().list(
        part="id, snippet, statistics, contentDetails",
        id=video_id,
        fields="kind,items(id, snippet(publishedAt, channelId, channelTitle, title, description), statistics, contentDetails(duration)),"
               "pageInfo"
    ).execute()

    return request

# def get_comment(videoId):


def get_comments(videoId, max_results=100):

    page_token = None
    comments = []

    while True:
        request = youtube.commentThreads().list(
            part="id,snippet",  # replies
            order="relevance",  # relevance
            videoId=videoId,
            maxResults=max_results,
            pageToken=page_token,
            fields="items(id,snippet(channelId,videoId,topLevelComment(snippet(textOriginal))))"
        ).execute()

        comments.extend(request['items'])

        break
        # if 'nextPageToken' not in request:
        #     break
        # else:
        #     page_token = request['nextPageToken']

    return comments


def search(channel_id, max_results, published_after=None, published_before=None, page_token=None):
    recent_video = None

    while True:
        request = youtube.search().list(
            part="id, snippet",
            maxResults=max_results,
            channelId=channel_id,
            order="date",
            publishedAfter=published_after,
            publishedBefore=published_before,
            pageToken=page_token,
            type="video"
        ).execute()

        print(request)

        for video in request['items']:

            if recent_video is None:
                recent_video = video

            flatten_search = flatten(video)
            flatten_video = flatten(get_video(video["id"]["videoId"])['items'][0])

            search_json = json.dumps(flatten_search, default=str, ensure_ascii=False)
            video_json = json.dumps(flatten_video, default=str, ensure_ascii=False)


        if 'nextPageToken' not in request:
            break
        else:
            page_token = request['nextPageToken']

    return recent_video


if __name__ == '__main__':
    print(get_video("8I30t5uHqAk"))

