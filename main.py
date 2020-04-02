from utilities.google_client import google_sheet_API
from utilities.athena_client import athena_API
from utilities.slack_client import slack_API
import multiprocessing as mp
import time

# Initialise the 2 APIs 
gsheetAPI = google_sheet_API()
athenaAPI = athena_API()
slackAPI = slack_API()

# Helper function to get the percentage in the right format
def _perc_to_float(perc):
        return float(perc.strip('%'))/100

# Helper function to write logs
def write_log(line):
    with open("log.txt", "a+") as logs:
        logs.write(f"{line}\r\n")

# Parallelised function, everything happens here
# It will retrieve the data from ATHENA
# Running 2 queries, and update the results in google sheet 
def get_data(query):
    row_idx = query['row_idx']
    domain = query['domain']
    url = query['url']

    # Try to run the 2 queries
    try:
        tw_query = athenaAPI.get_pandas_df(query['tw_query'])
        lw_query = athenaAPI.get_pandas_df(query['lw_query'])
        
        # Values are returned in DataFrame format, extract the single number
        tw_count = int(tw_query.values[0][0])
        lw_count = int(lw_query.values[0][0])
        client_message = 'Successful'
    except Exception as e:        
        tw_count = ''
        lw_count = ''
        slackAPI.send_message(f"Error for domain {domain}, row {row_idx}, query returned error:\n ```{str(e)}```")
        client_message = str(e)

    # Check if the percentage increase is formatted properly
    try:
        perc_increase = _perc_to_float(query['perc_increase'])
    except ValueError as e: 
        perc_increase = 1
        slackAPI.send_message(f"Error for domain {domain}, row {row_idx}, couldn't format the percentage value:\n ```{str(e)}```")
        client_message = str(e)

    # Update google sheet
    cell_address = f'K{row_idx+2}'
    cell_value = [tw_count, lw_count, time.asctime(), client_message]
    result = gsheetAPI.update_sheet(cell_address, cell_value)
    # Log results
    write_log(f"{result['updatedRange']} - {cell_value}")

    # Check logic and send to slack:
    if tw_count >= (lw_count + lw_count * perc_increase):
        slackAPI.send_message(f'''ALERT! :warning: 
Domain: _{domain}_ | Url: _{url}_ | Time: _{time.asctime()}_
This week's requests exceded last week by the given percentage:
```- This week's count: {tw_count}
- Last week's count: {lw_count}
- Percentage threshold: {query['perc_increase']}
- Actual percentage increase: {round(tw_count/lw_count*100-100, 2)}%```
<{gsheetAPI.gsheet_link}|Attack monitor sheet>''')

# --------- #
# MAIN LOOP #
# --------- #
while True:
    # Retrieve the sheet and add the queries
    gsheet_df = gsheetAPI.retrieve_sheet_as_df()
    gsheet_df_queries = athenaAPI.add_row_queries(gsheet_df)
    write_log(f'Data from Google Sheet retrieved at {time.asctime()}')

    # Prepare a list of dicts
    queries_list = []
    for row in gsheet_df_queries.iterrows():
        queries_list.append({'row_idx'      : row[0],
                            'domain'        : row[1]['domain'],
                            'url'           : row[1]['url'],
                            'perc_increase' : row[1]['perc_increase'],
                            'tw_query'      : row[1]['tw_query'],
                            'lw_query'      : row[1]['lw_query']})

    # Multiprocessing to run the queries simultaneously
    # It has to stay in the main function
    write_log(f'Beginning multiprocessing at {time.asctime()}, running {len(gsheet_df_queries)*2} queries on {mp.cpu_count()} threads')
    time_start = time.time()
    pool = mp.Pool(mp.cpu_count())
    results = [pool.apply(get_data, args=[query]) for query in queries_list]

    # Print how long it took for a full cycle
    exec_time = "{:.2f}".format(time.time() - time_start)
    write_log(f'Multiprocessing completed in {exec_time} seconds')
