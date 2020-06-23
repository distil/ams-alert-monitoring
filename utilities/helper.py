import time
from datetime import datetime
from utilities.google_client import gsheet_link
from colorclass import Color
import logging
import psutil

# Percentage of used RAM and avail memory
def perc_used_ram():
    return psutil.virtual_memory().percent

def perc_avail_memory():
    perc = psutil.virtual_memory().available * 100 / psutil.virtual_memory().total
    return "{:.2f}".format(perc)

def write_memory_log():
    asctime = time.asctime(datetime.utcnow().timetuple())
    logging.info(f'{asctime} | perc used ram: {perc_used_ram()} | perc avail memory: {perc_avail_memory()}')

def print_to_terminal_and_log(text, color='white', level_name='info'):
    ''''color' set the color when displaying to terminal
       'level_name' the log level'''
    # take the chance to log the memory usage as well
    LOG_FILENAME = 'data/logs.log'
    LEVELS = {'debug': logging.DEBUG,
             'info': logging.INFO,
             'warning': logging.WARNING,
             'error': logging.ERROR,
             'critical': logging.CRITICAL}
    
    level = LEVELS.get(level_name, logging.NOTSET)
    logging.basicConfig(filename=LOG_FILENAME,level=level)
    asctime = time.asctime(datetime.utcnow().timetuple())

    message = Color('{auto'+ color + '}' + text + '{/auto'+ color + '}')
    print(asctime, '-', message)
    logging.info(asctime + ' - '+ text)
    write_memory_log()

    

# Helper function to write logs and console at the same time
def write_log(line):
    print(line)
    with open("log/log.txt", "a+") as logs:
        logs.write(f"{line}\r\n")
        
# Generate the looker url string
def generate_looker_url(row_dict):
    url_match_formatted = row_dict.get('ilike_url').replace('/', '%2F')
    account_id = row_dict.get('account_id')
    site_id = row_dict.get('site_id')
    domain_id = row_dict.get('domain_id')
    return f'https://analytics.distilnetworks.com/dashboards/618?access_time=168%20hours&account_id={account_id}&site_id={site_id}&domain_id={domain_id}&url_match=%25{url_match_formatted}%25'

def compose_slack_alert(row_idx, row_dict, results):
    domain = row_dict.get('domain')
    ilike_url = row_dict.get('ilike_url')
    requests = results.get('requests')
    threshold = results.get('threshold')
    multiplier = row_dict.get('multiplier')
    threshold_bucket = results.get('threshold_bucket')
    note = results.get('note')
    name = results.get('name')
    looker_url = generate_looker_url(row_dict)
    
    return f'''ALERT! :warning: 
Domain: _{domain}_ | Url: _{ilike_url}_ | Time: _{time.asctime()}_
Request count exceded the threshold:
```- Request count: {requests}
- Threshold: {threshold}
- Threshold w/multiplier: {int(int(threshold)*float(multiplier))}
- Threshold bucket: {threshold_bucket}
- Percentage increase: {round(int(requests)/int(threshold)*100-100, 2)}%
- Note: {note}
- Name: {name}```
<{gsheet_link}|Attack monitor sheet> | row: {row_idx+2}
<{looker_url}|Looker Dashboard>'''