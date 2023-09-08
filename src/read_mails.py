
import re
import pytz
import base64
import hashlib
import logging
import pandas as pd
from ast import literal_eval
from bs4 import BeautifulSoup
from deta import Deta
from datetime import datetime, timedelta

from config import COLUMNS_ORDER, SPREADSHEET_ID
from utils import build_service


LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)


def parse_message_body(mail_body: str):
    """
    Get the RSVP answers from the message's body
    """
    # get the html representation of the message
    html_body = base64.urlsafe_b64decode(mail_body.encode("utf8")).decode("utf8")
    mssg_body = BeautifulSoup(html_body, "html.parser")

    translations = {
        "phone_number": "teléfono",
        "name": "nombre"
    }

    answers_html = mssg_body.find("table").find_all("tr")[2].find_all("p")
    answer_data = {}
    for ans in answers_html:
        match = re.search(r"(?P<question>.*?) *\*? *: *(?P<answer>.*)\r", ans.text)
        data = match.groupdict()
        
        header = data["question"].lower().replace(" ", "_")
        answer = data["answer"].strip()

        if "asistirás" in header or "come" in header:
            answer_data["asistiras"] = answer
        elif "invitados" in header or "guests" in header:
            answer_data["num_invitados"] = answer
        elif "tomar" in header or "drink" in header:
            drinks = literal_eval(answer)
            answer_data.update({f"tomar_{d.lower()}": "Si" for d in drinks})
        elif header in translations:
            answer_data[translations[header]] = answer
        else:
            answer_data[header] = answer
    
    return answer_data


def process_attatchments(message_parts: list):
    """
    Get all the answers in the attatchments
    """
    attatchment_messages = []

    # message with attatchemnts
    for attatchment in message_parts:
        if attatchment["mimeType"] != "message/rfc822":
            continue

        LOGGER.info(f"Processing attatchment no {attatchment['partId']}")

        message_answers = build_message_data(attatchment["parts"][0])

        attatchment_messages.extend(message_answers)

    return attatchment_messages


def build_message_data(message: dict):
    message_raw = message.get("payload", None)
    if message_raw is None:
        message_raw = message

    if message_raw["mimeType"] == "multipart/mixed":
        LOGGER.info(f"Message has {len(message_raw['parts'])} attatchments")
        message_answers = process_attatchments(message_raw["parts"])
    
    elif message_raw["mimeType"] == "text/html":
        try:
            # get answers from the mail's body
            message_data = parse_message_body(message_raw["body"]["data"])
        except Exception:
            LOGGER.error(f"Error parsing message", exc_info=True)
            raise Exception("Couldnt parse message")
        
        # get original date from headers
        headers_df = pd.DataFrame(message_raw["headers"])
        date_str = headers_df.loc[headers_df.name == "Date"].value.iloc[0]

        # get datetime obj to str
        date_tz = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")

        # convert to MX timezone
        date_mx = date_tz.astimezone(pytz.timezone("America/Mexico_City"))
        
        # save date in local time
        message_data["fecha"] = date_mx.replace(tzinfo=None).isoformat()

        # get unique id
        id_str = f"{message_data['fecha']}-{message_data['nombre']}-{message_data['teléfono']}"
        message_data["key"] = hashlib.md5(id_str.encode("utf8")).hexdigest()

        message_answers = [message_data]

    else:
        LOGGER.warning(f"Mime type not recognized: {message_raw['mimeType']}")
        
    return message_answers


def get_message(message_id: str, gmail):
    """
    Get all the answers contained in the message
    """
    message = gmail.users().messages().get(
        userId='me',
        id=message_id,
        format="full"
    ).execute()

    LOGGER.info(f"Processing message {message_id}")

    message_answers = build_message_data(message)
    
    return message_answers


def get_new_messages(start_date: datetime, end_date: datetime, gmail):
    """
    Get all the messages received after `start_date`
    """
    # get timestamp representation
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    answers = []

    finished = False
    next_page_token = None
    while not finished:
        emails = gmail.users().messages().list(
            userId='me',
            q=f"label:boda-pau after:{start_timestamp} before:{end_timestamp}",
            pageToken=next_page_token
        ).execute()

        LOGGER.info(f"Processing {emails['resultSizeEstimate']} messages")

        if emails["resultSizeEstimate"] > 0:
            for message in emails["messages"]:
                # get the answers for all the mssgs in the thread
                message_answers = get_message(message["id"], gmail)
                answers.extend(message_answers)

        if "nextPageToken" in emails:
            next_page_token = emails["nextPageToken"]
        else:
            # there are no more pages to read
            finished = True

    return answers


def save_rsvps(rsvps: list, old_rsvps: list, db):
    """
    Save RSVPS into the Deta Base
    """

    if old_rsvps is not None:
        rsvps_df = pd.DataFrame(rsvps)
        old_keys = [d["key"] for d in old_rsvps]

        # keep only the ones that are not already in the DB
        rsvps_df = rsvps_df.loc[~rsvps_df.key.isin(old_keys)].fillna("")
        new_rsvps = rsvps_df.to_dict(orient="records")
    else:
        new_rsvps = rsvps

    LOGGER.info(f"Saving {len(new_rsvps)} new answers")

    batch_size = 25
    for start in range(0, len(new_rsvps), batch_size):
        LOGGER.info(f"Saving rsvps {start} to {start + batch_size}")

        rsvps_batch = new_rsvps[start:start + batch_size]

        db.put_many(rsvps_batch)


def get_rsvps(last_date: datetime, db):
    """
    Get all the old rsvps in the db
    """
    # get str representation
    last_date_str = last_date.isoformat()

    finished = False
    last = None
    rsvps = []
    while not finished:
        result = db.fetch({"fecha?lte": last_date_str}, last=last)
        LOGGER.info(f"Fetched {result.count} old items (start {result.last})")

        if result.last is not None:
            last = result.last
        else:
            finished = True

        rsvps.extend(result.items)

    return rsvps


def write_rsvps(rsvps: pd.DataFrame, sheet_name: str, sheets):
    """
    Write the RSVPS data into google sheets
    """
    # get the ASCII representation of the last column
    # 65 == 'A'
    end_char = 64 + rsvps.shape[1]
    
    end_column = chr(end_char)
    num_rows = rsvps.shape[0] + 1

    # build the range in A1 notation
    range = f"{sheet_name}!A1:{end_column}{num_rows}"

    # fill na with empty strings
    rsvps = rsvps.fillna("")

    # build values list
    values = [rsvps.columns.tolist()]
    values.extend(rsvps.to_numpy().tolist())

    body = {"values": values}

    try:
        result = sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
    except:
        LOGGER.error("Couldn't write to google sheets", exc_info=True)
        raise Exception("Can't write to sheets")
    else:
        LOGGER.info(f"Updated {result['updatedCells']} cells in {range}")


def read_mails():
    # get deta bases
    deta = Deta()
    rsvps_db = deta.Base("rsvps")
    logs_db = deta.Base("logs")

    gmail = build_service("gmail")

    # get last date fetched
    is_empty = False
    start_date = logs_db.get("max_date")
    if start_date is None:
        is_empty = True
        start_date = datetime(2023,1,1)
    else:
        start_date = datetime.fromisoformat(start_date["value"]) + timedelta(minutes=1)

    end_date = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(minutes=5)
    end_date = end_date.astimezone(pytz.timezone("America/Mexico_City")).replace(tzinfo=None)
    
    LOGGER.info(f"Getting new mails since {start_date} until {end_date}")

    # get all the rsvps
    new_rsvps = get_new_messages(start_date, end_date, gmail)

    LOGGER.info(f"{len(new_rsvps)} mails found")
    
    if len(new_rsvps) > 0:
        # build sheets service
        sheets = build_service("sheets")

        if not is_empty:
            old_rsvps = get_rsvps(start_date, rsvps_db)
            rsvps = new_rsvps + old_rsvps
        else:
            rsvps = new_rsvps
            old_rsvps = None

        # save in df 
        rsvps_df = pd.DataFrame(rsvps)

        # drop duplicates (counting all the columns)
        rsvps_df = rsvps_df.drop_duplicates()

        # save the last ingested date
        max_date = rsvps_df.fecha.max()

        # get the complete list of columns
        drinks_cols = [c for c in rsvps_df if "tomar" in c]
        full_columns = COLUMNS_ORDER + drinks_cols

        # get columns in the correct order
        rsvps_df = rsvps_df[full_columns]

        # order columns by created date
        rsvps_df = rsvps_df.sort_values(by="fecha", ascending=True)

        # write the full df
        write_rsvps(rsvps_df, "confirmaciones_raw", sheets)

        # drop duplicates (keep the last answer)
        rsvps_df.drop_duplicates(
            subset=["nombre", "teléfono"],
            keep="last",
            inplace=True
        )

        LOGGER.info(f"{rsvps_df.shape[0]} mails after dropping duplicates")

        # write only the final rsvps
        rsvps_df = rsvps_df.drop(columns=["key"])
        write_rsvps(rsvps_df, "confirmaciones", sheets)

        # save new rsvps to Deta Base
        save_rsvps(new_rsvps, old_rsvps, rsvps_db)
        logs_db.put(max_date, key="max_date")

    return len(new_rsvps)
