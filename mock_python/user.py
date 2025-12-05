import json
from pathlib import Path
def get_user_settings():
    with open(Path('./settings.json').expanduser()) as f:
        return json.load(f)
    
def add_two_settings():
    settings = get_user_settings()
    return settings["opt1"] + settings["opt2"]

def test_add_two_settings():
    assert add_two_settings() == 17

print(test_add_two_settings())