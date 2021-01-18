import sys
import time
from hashlib import sha1
from typing import Union

# OS helpers
##############################################################################

isMac = sys.platform.startswith("darwin")
isWin = sys.platform.startswith("win32")
isLin = not isMac and not isWin


def intTime(scale: int = 1) -> int:
    "The time in integer seconds. Pass scale=1000 to get milliseconds."
    return int(time.time() * scale)

def checksum(data: Union[bytes, str]) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return sha1(data).hexdigest()