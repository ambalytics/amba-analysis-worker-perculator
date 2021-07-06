import datetime
import json
import time
import logging


# class Author(object):
#
#     url = None
#     original_tweet_url = None
#     original_tweet_author = None
#     alternative_id = None
#
#
# class Subject(object):
#     # tweet data eg
#     pid = None
#     url = None
#     title = None
#     issued = None
#     author = None
#     data = None
#
#
# class Object(object):
#
#     pid = None
#     url = None
#     method = None
#     verification = None
#     data = None


class Event(object):

    data = {
        "obj_id": None,
        "occurred_at": None,
        "subj_id": None,
        "id": None,
        "subj": {
            "pid": None,
            "url": None,
            "title": None,
            "issued": None,
            "author": {
                "url": None
             },
            "original-tweet-url": None,
            "original-tweet-author": None,
            "alternative-id": None,
        },
        "source_id": None,
        "obj": {
            "pid": None,
        },
        "timestamp": None,
        "relation_type": None
    }

    def set(self, key, value):
        self.data[key] = value

    def get(self, keys):
        return self.data[keys]
        # t = self.data
        # # todo check if key exist
        # for key in keys:
        #     t = t[key]
        # return t

    def from_json(self, json_msg):
        self.data = json_msg
        # self.data = json.loads(json_msg)

    def get_json(self):
        return json.dumps(self.data)

    def __init__(self):
        # "timestamp":"2019-01-10T17:21:51Z",
        self.set('timestamp', '{0:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.datetime.now()))

    # def __init__(self, subj_id, relation_type, obj_id, occurred_at, id, source_id, source_token):
    #     self.sub_id = subj_id
    #     self.relation_type = relation_type
    #     self.obj_id = obj_id
    #     self.occurred_at = occurred_at
    #     self.id = id
    #     self.source_id = source_id
    #     self.source_tokend = source_token
    #     self.timestamp = round(time.time() * 1000)
    #     logging.warning(self)

    # def __init__(self, subj_id, relation_type, obj_id, occurred_at, id, source_id, source_token):
    #     self.json_data = json.loads(self.json_data)
    #     self.json_data['subj_id'] = subj_id
    #     self.json_data['relation_type'] = relation_type
    #     self.json_data['obj_id'] = obj_id
    #     self.json_data['occurred_at'] = occurred_at
    #     self.json_data['id'] = id
    #     self.json_data['source_id'] = source_id
    #     self.json_data['source_tokend'] = source_token
    #     self.json_data['timestamp'] = round(time.time() * 1000)
    #     logging.warning(self.json_data)

# {
#
#  "obj_id":"https://doi.org/10.1039/c8ee03134g",
#  "occurred_at":"2019-01-10T17:12:26Z",
#  "subj_id":"twitter://status?id=1083411254788739073",
#  "id":"29ffcda3-c9bc-47ca-a916-dcde1e2023fa",
#  "action":"add",
#  "subj":{
#     "pid":"twitter://status?id=1083411254788739073",
#     "url":"twitter://status?id=1083411254788739073",
#     "title":"Tweet 1083411254788739073",
#     "issued":"2019-01-10T17:12:26.000Z",
#     "author":{
#       "url":"twitter://user?screen_name=pmherder"},
#       "original-tweet-url":"twitter://status?id=1083379011089133568",
#       "original-tweet-author":"twitter://user?screen_name=TomBurdyny",
#       "alternative-id":"1083411254788739073"
#       "data"
#     },
#  "source_id":"twitter",
#  "obj":{
#    "pid":"https://doi.org/10.1039/c8ee03134g",
#     "data":
#    },
#    "timestamp":"2019-01-10T17:21:51Z",
#    "relation_type_id":"discusses"
# }
