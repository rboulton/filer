import sqlite3
import urllib.parse
import os

DB_FILENAME = "db.sqlite"


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
          filesize integer,
          first_observed integer,
          deleted_before integer
        );
        """,
        """
        create table if not exists visits (
          path text primary key,
          revisit_time integer
        ) without rowid;
        """,
        """
        create index if not exists idx_current_file_hashes on files (
          hash,
          path,
          mtime,
          filesize
        ) where deleted_before is null;
        """,
        """
        create index if not exists idx_file_hashes on files (
          hash
        )
        """,
        """
        create index if not exists idx_revisits on visits (
          path,
          revisit_time
        ) where revisit_time is not null;
    """,
    ):
        cursor.execute(sql)

    cursor.close()
    connection.commit()


def clear_visits(connection):
    """Clear the visits table.

    This is done before starting a new walk of the full tree.

    This allows the files which aren't seen in a walk to be determined.

    """
    cursor = connection.cursor()
    try:
        cursor.execute("delete from visits;")
    finally:
        cursor.close()


def record_visit(connection, path, revisit_time=None, deleted=False):
    """Record a visit to a path.

    """
    cursor = connection.cursor()
    try:
        if deleted:
            cursor.execute(
                """
                delete from visits
                where path = ?
            """,
                (path,),
            )

        else:
            cursor.execute(
                """
                insert or replace into visits (path, revisit_time)
                values(?, ?)
            """,
                (path, revisit_time),
            )
    finally:
        cursor.close()


def due_for_revisit(connection, now):
    """Return a list of some paths which are due a revisit, or a time that some
    will become available

    """
    next_revisit_time = None
    revisits = []
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            select path, revisit_time
            from visits
            where revisit_time is not null
            order by revisit_time asc
            limit 1000
        """
        )
        items = cursor.fetchall()

        if len(items) == 0:
            return None, ()
        due = [path for path, revisit_time in items if revisit_time <= now]
        not_due_times = [
            revisit_time for path, revisit_time in items if revisit_time > now
        ]
        if len(not_due_times) > 0:
            next_revisit_time = min(not_due_times)
        else:
            next_revisit_time = None
        if len(due) > 0:
            return next_revisit_time, due
        return items[0][1], ()
    finally:
        cursor.close()
    return due, next_revisit_time


def get_current_file_data(connection, paths):
    cursor = connection.cursor()
    try:
        args = ", ".join(["?"] * len(paths))
        cursor.execute(
            """
        select hash, path, mtime
        from files
        where path in ({})
        and deleted_before is null
        """.format(
                args
            ),
            paths,
        )
        return cursor.fetchall()
    finally:
        cursor.close()


def update_file_data(connection, new_hash, filesize, path, mtime, now):
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            select rowid, hash, mtime, first_observed
            from files
            where path = ?
            and deleted_before is null
        """,
            (path,),
        )
        rows = cursor.fetchall()
        if len(rows) > 0:
            assert len(rows) == 1
            rowid, old_hash, old_mtime, old_first_observed = rows[0]
            if old_hash == new_hash and old_mtime == mtime:
                # print("Nothing to change")
                return

            cursor.execute(
                """
                replace into files (rowid, hash, filesize, path, mtime, first_observed)
                values(?, ?, ?, ?, ?, ?)
            """,
                (rowid, new_hash, filesize, path, mtime, old_first_observed),
            )
        else:
            cursor.execute(
                """
                insert into files (hash, filesize, path, mtime, first_observed)
                values(?, ?, ?, ?, ?)
            """,
                (new_hash, filesize, path, mtime, now),
            )
    finally:
        cursor.close()


def update_deleted_file_data(connection, path, now):
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
                update files
                set deleted_before = ?
                where path = ?
                and deleted_before is null
            """,
            (now, path,),
        )
    finally:
        cursor.close()
