from fastapi import FastAPI
from pydantic import BaseModel

from read_mails import read_mails

class ActionEvent(BaseModel):
    id: str
    trigger: str

class Action(BaseModel):
    event: ActionEvent

app = FastAPI()

@app.get("/")
def root():
    return "Hello from Space! 🚀"

@app.get("/get_mails")
def get_mails():
    num_mails = read_mails()
    return f"Found {num_mails} mails"

@app.post("/__space/v0/actions")
def actions(action: Action):
    if action.event.id == "update_rsvps":
        read_mails()