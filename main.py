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

# Helper function to write logs and console at the same time
def write_log(line):
    print(line)
    with open("log.txt", "a+") as logs:
        logs.write(f"{line}\r\n")

# Helper function to make sure all rows in the dict are valid
def check_row_validity(row):
    if row[1]['perc_increase'] == '' or row[1]['account_id'] == '' or row[1]['site_id'] == '':
        write_log(f'Skipped row {row[0] + 2} as contained empty values')
        return False
    elif row[1]['perc_increase'] == None or row[1]['account_id'] == None or row[1]['site_id'] == None:
        write_log(f'Skipped row {row[0] + 2} as contained Null values')
        return False
    else:
        return True

# Helper function to generate the looker url string
def generate_looker_url(account_id, site_id, url_match):
    url_match_formatted = url_match.replace('/', '%2F')
    return f'https://analytics.distilnetworks.com/dashboards/618?access_time=168%20hours&account_id={account_id}&site_id={site_id}&url_match=%25{url_match_formatted}%25'

# Parallelised function, everything happens here
# It will retrieve the data from Athena
# Run the 2 queries, and update the results in google sheet
def get_data(row_idx, row_dict):
    domain = row_dict.get('domain')
    account_id = row_dict.get('account_id')
    site_id = row_dict.get('site_id')
    url_match = row_dict.get('url_match')
    perc_increase = row_dict.get('perc_increase')
    
    # Try to run the 2 queries
    try:
        tw_query = athenaAPI.get_pandas_df(row_dict.get('tw_query'))
        lw_query = athenaAPI.get_pandas_df(row_dict.get('lw_query'))
        
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
        float_perc_increase = _perc_to_float(perc_increase)
    except ValueError as e: 
        float_perc_increase = 1.0
        slackAPI.send_message(f"Error for domain {domain}, row {row_idx}, couldn't format the percentage value:\n ```{str(e)}```")
        client_message = str(e)

    # Update google sheet
    cell_address = f'K{row_idx+2}'
    cell_value = [tw_count, lw_count, time.asctime(), client_message]
    result = gsheetAPI.update_sheet(cell_address, cell_value)
    
    # Log results
    write_log(f"{result['updatedRange']} - {cell_value}")

    # Looker
    looker_url = generate_looker_url(account_id
                                    , site_id
                                    , url_match)

    # Check logic and send to slack:
    if tw_count >= (lw_count + lw_count * float_perc_increase):
        slackAPI.send_message(f'''ALERT! :warning: 
Domain: _{domain}_ | Url: _{url_match}_ | Time: _{time.asctime()}_
This week's requests exceded last week by the given percentage:
```- This week's count: {tw_count}
- Last week's count: {lw_count}
- Percentage threshold: {perc_increase}
- Actual percentage increase: {round(tw_count/lw_count*100-100, 2)}%```
<{gsheetAPI.gsheet_link}|Attack monitor sheet>
<{looker_url}|Looker Dashboard>''')


# --------- #
# MAIN LOOP #
# --------- #
if __name__ == '__main__':
    while True:
        # Retrieve the sheet and add the queries
        gsheet_df = gsheetAPI.retrieve_sheet_as_df()
        gsheet_df_queries = athenaAPI.add_row_queries(gsheet_df)
        write_log(f'Data from Google Sheet retrieved at {time.asctime()}')

        # Prepare a list of dicts
        row_list = [row for row in gsheet_df_queries.iterrows() if check_row_validity(row)]

        # Multiprocessing to run the queries simultaneously, it has to stay in the main function
        # The argument is the full dictionary, I need most of the arguments
        write_log(f'Beginning multiprocessing at {time.asctime()}, running {len(row_list) * 2} queries on {mp.cpu_count()} threads')
        time_start = time.time()
        with mp.Pool(mp.cpu_count()) as pool:
            results = [pool.apply(get_data, args=row) for row in row_list]

        # Print how long it took for a full cycle
        exec_time = "{:.2f}".format(time.time() - time_start)
        write_log(f'Multiprocessing completed in {exec_time} seconds')