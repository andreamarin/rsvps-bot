import os

KEYS_PATH = f"{os.getcwd()}/keys"

# If modifying these scopes, delete the file token.json.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

COLUMNS_ORDER = [
    "key",
    "fecha",
    "nombre",
    "asistiras",
    "num_invitados",
    "teléfono"
]

SPREADSHEET_ID = "1yqndHugIrLMQnFzyREW2qKw0DWJ5h66ulbK3ZxLqUkk"
