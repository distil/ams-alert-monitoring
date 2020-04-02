import os
import configparser
import requests
import json

class slack_API():
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.dpcfg.ini'))
        self.WEBHOOK_URL = config.get('slack','WEBHOOK_URL')
    
    def send_message(self, message):
        self.data = {'text':message}
        requests.post(self.WEBHOOK_URL, json.dumps(self.data))