from __future__ import print_function

import os.path
import json
from google.auth.transport.requests import Request
#from google.oauth2.credentials import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
#SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly', 
    'https://www.googleapis.com/auth/drive'] # 'https://spreadsheets.google.com/feeds',

# Google Sheet
# [zones - Google Sheets](https://docs.google.com/spreadsheets/d/1DnE1RY7exhRzc-e3kd8sX9HKRbjEvBHS4aZFXFLupeM/edit#gid=423793051)
SPREADSHEET_ID = '1DnE1RY7exhRzc-e3kd8sX9HKRbjEvBHS4aZFXFLupeM'
RANGE_NAME = 'zone_dates'

CREDENTIALS_JSON = '/Users/bbest/My Drive (ben@ecoquants.com)/projects/whalesafe/data/gfw/Benioff Ocean Initiative-454f666d1896.json'
CREDENTIALS_JSON = '/Users/bbest/Downloads/benioff-ocean-initiative-0b09860e2d00.json'
# lgnd-website-service-account: https://console.cloud.google.com/iam-admin/serviceaccounts/details/114569616080626900590;edit=true?previousPage=%2Fapis%2Fcredentials%3Fproject%3Dbenioff-ocean-initiative%26authuser%3D1&authuser=1&project=benioff-ocean-initiative
# shared Gsheet with ships4whales@benioff-ocean-initiative.iam.gserviceaccount.com as Editor
with open(CREDENTIALS_JSON, 'r') as file:
    CREDENTIALS_STR= file.read().replace('\n', '')
SHEETS_KEY = json.loads(CREDENTIALS_STR)

def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(CREDENTIALS_JSON):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(SHEETS_KEY, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or creds.invalid:
        sys.exit("CREDENTIALS_JSON not found:" + CREDENTIALS_JSON)
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file(
    #             'credentials.json', SCOPES)
    #         creds = flow.run_local_server(port=0)
    #     # Save the credentials for the next run
    #     with open(CREDENTIALS_JSON, 'w') as token:
    #         token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        # RANGE_NAME = 'zone_dates'
        result = sheet.values().get(
          spreadsheetId=SPREADSHEET_ID,
          range=RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
            return

        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))
    except HttpError as err:
        print(err)


if __name__ == '__main__':
    main()
