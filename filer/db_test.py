from . import db
from . import config
import os
import shutil
import pytest
import sqlite3


__this_file = os.path.realpath(os.path.abspath(__file__))
test_config = config.load_config_from_path(
    os.path.join(os.path.dirname(os.path.dirname(__this_file)), "tests", "config.json")
)


def test_connect():
    shutil.rmtree(test_config.db_dir, ignore_errors=True)

    conn = db.connect(test_config, read_only=False)
    conn_ro = db.connect(test_config)

    with pytest.raises(sqlite3.OperationalError):
        db.init_schema(conn_ro)

    db.init_schema(conn)
