import requests
import json

class slack_API():
    def __init__(self):
        self.WEBHOOK_URL = 'https://hooks.slack.com/services/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'
    
    def send_message(self, message):
        self.data = {'text':message}
        requests.post(self.WEBHOOK_URL, json.dumps(self.data))