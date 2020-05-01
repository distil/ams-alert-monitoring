import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import os

gsheet_link = 'https://docs.google.com/spreadsheets/d/10QCryRqYBlS-kE_ExHaGaSZU2JfS88BGcBss-KYr0Wk'

# Spreadsheet is in:
# https://docs.google.com/spreadsheets/d/10QCryRqYBlS-kE_ExHaGaSZU2JfS88BGcBss-KYr0Wk

class google_sheet_API():
    # Default spreadsheet_id value is athena attack alerting
    def __init__(self):
        # If modifying these scopes, delete the file token.pickle!
        self.SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        #SPREADSHEET_ID = '1UUp2TOeC4i2DaNT1FEzM6C-QfwHvP_32gJDisLvC0Ak' # presto attack alerting
        
    def retrieve_gservice(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        return build('sheets', 'v4', credentials=creds)
    
    def get_google_sheet(self
                        , service
                        , SPREADSHEET_ID='10QCryRqYBlS-kE_ExHaGaSZU2JfS88BGcBss-KYr0Wk'
                        , RANGE_NAME = 'queries'):
        '''Call the Sheets API and retrieve the sheet in dataframe format'''
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        return pd.DataFrame(values[1:], columns=values[0])
    
    def update_sheet(self
                    , service
                    , cell
                    , value
                    , SPREADSHEET_ID='10QCryRqYBlS-kE_ExHaGaSZU2JfS88BGcBss-KYr0Wk'
                    , RANGE_NAME = 'queries'):
        '''Given a cell, a value, it will update the spreadsheet accordingly'''
        sheet = service.spreadsheets()
        body = {'values': [value]}
        result = sheet.values().update(spreadsheetId=SPREADSHEET_ID, 
                                       range=cell, 
                                       body=body, 
                                       valueInputOption='USER_ENTERED').execute()
        return result