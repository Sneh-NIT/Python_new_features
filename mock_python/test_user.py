from io import StringIO
from unittest.mock import patch
import json
from pathlib import Path

def get_user_settings():
    with open(Path('./settings.json').expanduser()) as f:
        return json.load(f)

def add_two_settings():
    settings = get_user_settings()
    return settings["opt1"] + settings["opt2"]

@patch("builtins.open")
def test_add_two_settings(mock_open):
    mock_open.return_value = StringIO('{"opt1": 10, "opt2": 7}')
    assert add_two_settings() == 17

test_add_two_settings()