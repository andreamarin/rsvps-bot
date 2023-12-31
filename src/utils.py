import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config import KEYS_PATH, SCOPES

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)


def build_service(service_name: str):

    creds = Credentials.from_authorized_user_file(f'{KEYS_PATH}/token.json', SCOPES)

    if service_name == "gmail":
        # create gmail client
        service = build('gmail', 'v1', credentials=creds)
    elif service_name == "sheets":
        # create sheets client
        service = build('sheets', 'v4', credentials=creds)

    return service