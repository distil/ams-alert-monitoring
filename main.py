from utilities.google_client import google_sheet_API
from utilities.athena_client import athena_API
from utilities.slack_client import slack_API
from utilities.helper import write_log, generate_looker_url, compose_slack_alert
import multiprocessing as mp
import time
from datetime import datetime

# Initialise the 2 APIs 
gsheetAPI = google_sheet_API()
athenaAPI = athena_API()
slackAPI = slack_API()

# Retrieve query
sql_query = athenaAPI.get_SQL_query()

def process_row(row_idx, row_dict, service):
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

    # Step 3: check logic and send to slack:
    requests = int(results.get('requests'))
    threshold = int(results.get('threshold'))
    multiplier = float(row_dict.get('multiplier'))

    if requests > threshold * multiplier:
        try:
            message = compose_slack_alert(row_idx, row_dict, results)
            slackAPI.send_message(message)
            client_message = f'Successful | {requests} requests | Slack alert sent at {time.asctime(datetime.utcnow().timetuple())}'
        except Exception as e:
            domain = row_dict.get('domain')
            slackAPI.send_message(f"Error for domain {domain}, row {row_idx}, query returned error:\n ```{str(e)}```")
            client_message = str(e)
    else:
        client_message = 'Successful | No alert triggered'
    
    # Update Google sheet
    cell_address = f'O{row_idx+2}'
    gsheetAPI.update_sheet(service, cell_address, [time.asctime(datetime.utcnow().timetuple()), client_message])

# MAIN LOOP 
# ------------------
if __name__ == '__main__':
    while True:
        # Retrieve GSheet
        service = gsheetAPI.retrieve_gservice()
        gsheet_df = gsheetAPI.get_google_sheet(service=service)
        write_log(f'Data from Google Sheet retrieved at {time.asctime(datetime.utcnow().timetuple())}')

        # Prepare a list of dicts
        row_list = [(row_idx, row_dict, service) for row_idx, row_dict in gsheet_df.iterrows() if row_dict.get('status') != 'pause']

        # Multiprocessing to run the queries simultaneously, it has to stay in the main function
        write_log(f'Beginning multiprocessing at {time.asctime(datetime.utcnow().timetuple())}, running {len(row_list)} queries on {mp.cpu_count()} threads')
        time_start = time.time()
        with mp.Pool(mp.cpu_count()) as pool:
            results = [pool.apply(process_row, args=row) for row in row_list]

        # Print how long it took for a full cycle
        exec_time = "{:.2f}".format(time.time() - time_start)
        write_log(f'Multiprocessing completed in {exec_time} seconds')