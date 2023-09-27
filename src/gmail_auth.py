import sys
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from config import KEYS_PATH, SCOPES


def get_auth_token(force_refresh: bool ):
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(f'{KEYS_PATH}/token.json'):
        creds = Credentials.from_authorized_user_file(f'{KEYS_PATH}/token.json', SCOPES)

    save_token = False
    if not creds:
        # If there are no (valid) credentials available, let the user log in.
        flow = InstalledAppFlow.from_client_secrets_file(
            f'{KEYS_PATH}/credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        save_token = True
    elif force_refresh or not creds.valid:
        creds.refresh(Request())
        save_token = True

    if save_token:
        # Save the credentials for the next run
        with open(f'{KEYS_PATH}/token.json', 'w') as token:
            token.write(creds.to_json())


if __name__ == "__main__":
    args = sys.argv

    if len(args) == 1:
        force_refresh = False
    else:
        force_refresh = args[1] == "refresh"

    get_auth_token(force_refresh)
