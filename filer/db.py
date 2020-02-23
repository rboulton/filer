import sqlite3
import urllib.parse
import os

DB_FILENAME = "db.sql"


def uri(path, read_only):
    uri = "file:{}".format(urllib.parse.quote(path))
    if read_only:
        return uri + "?mode=ro"
    return uri


def connect(config, read_only=True):
    db_dir = config.db_dir
    db_path = os.path.join(db_dir, DB_FILENAME)

    if not read_only:
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

    return sqlite3.connect(uri(db_path, read_only), uri=True)


def init_schema(connection):
    cursor = connection.cursor()
    for sql in (
        """
        pragma journal_mode=WAL;
        """,
        """
        create table if not exists files (
          hash text,
          path text,
          first_observed integer,
          deleted_before integer
        );
        """,
        """
        create index if not exists file_hashes on files (
          hash,
          path,
          first_observed,
          deleted_before
        );
    """,
    ):
        cursor.execute(sql)

    cursor.close()
    connection.commit()
