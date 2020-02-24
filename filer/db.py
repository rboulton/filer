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
          mtime integer,
          first_observed integer,
          deleted_before integer
        );
        """,
        """
        create index if not exists current_file_hashes on files (
          hash,
          path,
          mtime
        ) where deleted_before is null;
    """,
    ):
        cursor.execute(sql)

    cursor.close()
    connection.commit()

def get_current_file_data(connection, paths):
    cursor = connection.cursor()
    try:
        args = ", ".join(["?"] * len(paths))
        cursor.execute("""
        select hash, path, mtime
        from files
        where path in ({})
        and deleted_before is null
        """.format(args), paths)
        return cursor.fetchmany()
    finally:
        cursor.close()

def update_file_data(connection, new_hash, path, mtime, now):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            select rowid, hash, mtime, first_observed
            from files
            where path = ?
            and deleted_before is null
        """, (path, ))
        rows = cursor.fetchmany()
        if len(rows) > 0:
            assert len(rows) == 1
            rowid, old_hash, old_mtime, old_first_observed = rows[0]
            if old_hash == new_hash and old_mtime == mtime:
                print("Nothing to change")
                return

            cursor.execute("""
                replace into files (rowid, hash, path, mtime, first_observed)
                values(?, ?, ?, ?, ?)
            """, (rowid, new_hash, path, mtime, old_first_observed))
        else:
            cursor.execute("""
                insert into files (hash, path, mtime, first_observed)
                values(?, ?, ?, ?)
            """, (new_hash, path, mtime, now))
    finally:
        cursor.close()
