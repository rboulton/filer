from . import config
import os
import pytest


def test_load_config():
    __this_file = os.path.realpath(os.path.abspath(__file__))
    test_config = os.path.join(
        os.path.dirname(os.path.dirname(__this_file)), "tests", "config.json"
    )
    value = config.load_config_from_path(test_config)
    assert value.roots == ["/"]
    with pytest.raises(AttributeError) as e:
        value.invalid_value
