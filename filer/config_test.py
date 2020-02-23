from . import config
import os
import pytest


def test_load_config():
    __this_file = os.path.realpath(os.path.abspath(__file__))
    default_config = os.path.join(os.path.dirname(__this_file), "config.json")
    value = config.load_config_from_path(default_config)
    assert value.roots == ["/"]
    with pytest.raises(AttributeError) as e:
        value.invalid_value
