from utilities.google_client import google_sheet_API
from utilities.athena_client import athena_API
from utilities.slack_client import slack_API
import multiprocessing as mp
import time

# Initialise the 2 APIs 
gsheetAPI = google_sheet_API()
athenaAPI = athena_API()
slackAPI = slack_API()

# Retrieve GSheet
service = gsheetAPI.retrieve_gservice()
gsheet_df = gsheetAPI.get_google_sheet(service=service)

# Retrieve SQL query
with open('main_query.sql', 'r') as f:
    sql_query = f.read()

# Helper functions
# ------------------
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
    looker_url = generate_looker_url(row_dict)
    
    return f'''ALERT! :warning: 
    Domain: _{domain}_ | Url: _{ilike_url}_ | Time: _{time.asctime()}_
    Request count exceded the threshold:
    ```- Request count: {requests}
    - Threshold: {threshold}
    - Threshold w multiplier: {int(threshold)*float(multiplier)}
    - Threshold bucket: {threshold_bucket}
    - Percentage increase: {round(int(requests)/int(threshold)*100-100, 2)}%```
    <{gsheetAPI.gsheet_link}|Attack monitor sheet> row: {row_idx}
    <{looker_url}|Looker Dashboard>'''

def process_row(row_idx, row_dict):
    # Step 1: Generate the clauses 
    for identifier_id in ['account_id', 'domain_id', 'site_id']:
        if row_dict[identifier_id] != '':
            row_dict[f'{identifier_id}_condition'] = f'and {identifier_id} = ' + '\'' + row_dict[identifier_id] + '\''
        else:
            row_dict[f'{identifier_id}_condition'] = ''

    row_dict['threshold_one'] = '\'' + row_dict['threshold_one | 0-7'] + '\''
    row_dict['threshold_two'] = '\'' + row_dict['threshold_two | 8-15'] + '\''
    row_dict['threshold_three'] = '\'' + row_dict['threshold_three | 16-23'] + '\''
    
    # Step 2: Execute the query
    results = athenaAPI.get_pandas_df(sql_query.format(**row_dict)).to_dict('records')[0]

    # Step 3: heck logic and send to slack:
    if results.get('requests')>int(results.get('threshold'))*float(row_dict.get('multiplier')):
        try:
            message = compose_slack_alert(row_idx, row_dict, results)
            slackAPI.send_message(message)
            client_message = 'Successful'
        except Exception as e:
            slackAPI.send_message(f"Error for domain {domain}, row {row_idx}, query returned error:\n ```{str(e)}```")
            client_message = str(e)
    
    # Update Google sheet
    cell_address = f'O{row_idx+2}'
    gsheetAPI.update_sheet(service, cell_address, [time.asctime(), client_message])

# MAIN LOOP 
# ------------------
if __name__ == '__main__':
    while True:
        # Retrieve GSheet
        service = gsheetAPI.retrieve_gservice()
        gsheet_df = gsheetAPI.get_google_sheet(service=service)
        write_log(f'Data from Google Sheet retrieved at {time.asctime()}')

        # Prepare a list of dicts
        row_list = [(row_idx, row_dict) for row_idx, row_dict in gsheet_df.iterrows()]

        # Multiprocessing to run the queries simultaneously, it has to stay in the main function
        # The argument is the full dictionary, I need most of the arguments
        write_log(f'Beginning multiprocessing at {time.asctime()}, running {len(row_list) * 2} queries on {mp.cpu_count()} threads')
        time_start = time.time()
        with mp.Pool(mp.cpu_count()) as pool:
            results = [pool.apply(process_row, args=row) for row in row_list]

        # Print how long it took for a full cycle
        exec_time = "{:.2f}".format(time.time() - time_start)
        write_log(f'Multiprocessing completed in {exec_time} seconds')