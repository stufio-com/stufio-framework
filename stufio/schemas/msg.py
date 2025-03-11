from pydantic import BaseModel
from typing import Literal


class Msg(BaseModel):
    msg: str


class ResultMsg(Msg):
    result: Literal["success", "error"]
