import sqlite3
import urllib.parse
import os

DB_FILENAME = "db.sqlite"


def db_uri(path, read_only):
    """Calculate the database URI"""
    uri = "file:{}".format(urllib.parse.quote(path))
    if read_only:
        return uri + "?mode=ro"
    return uri


def connect(config, read_only=True):
    """Connect to the database"""
    db_dir = config.db_dir
    db_path = os.path.join(db_dir, DB_FILENAME)

    if not read_only:
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

    return sqlite3.connect(db_uri(db_path, read_only), uri=True)


def init_schema(connection):
    cursor = connection.cursor()
    for sql in (
        """
        pragma journal_mode=WAL;
        """,
        """
        create table if not exists dirs (
          id integer primary key autoincrement,
          path text unique,
          parent_id integer,
          first_observed integer,
          deleted_before integer,
          foreign key(parent_id) references dirs(id)
        );
        """,
        """
        create table if not exists files (
          hash text,
          path text,
          dir_id integer,
          mtime integer,
          filesize integer,
          first_observed integer,
          deleted_before integer,
          foreign key(dir_id) references dirs(id)
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
        print(sql)
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
    """Record a visit to a path."""
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


def get_unvisited_files(connection):
    """Return an iterator over paths which exist in the DB but haven't been visited yet."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            select files.path
            from files
            left outer join visits
            on files.path = visits.path
            where visits.path is null
            and files.deleted_before is null
            order by files.hash asc
        """
        )
        while True:
            items = cursor.fetchmany()
            if len(items) == 0:
                break
            for item in items:
                yield item[0]
    finally:
        cursor.close()


def get_current_file_data(connection, paths):
    print("Get current: %r" % (paths,))
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


def _update_dir_data(cursor, path, now):
    """Ensure that there's a record of a directory existing now, and return its id.

    """
    is_root = (path == '/' or path == '')
    cursor.execute(
        """
        select id, parent_id, first_observed
        from dirs
        where path = ?
        and deleted_before is null
    """,
    (path,),
    )
    rows = cursor.fetchall()
    if len(rows) > 0:
        assert len(rows) == 1
        old_dir_id, _, _ = rows[0]
        return old_dir_id
    else:
        if is_root:
            parent_id = None
        else:
            parent_path = os.path.dirname(path)
            parent_id = _update_dir_data(cursor, parent_path, now)
        cursor.execute(
            """
            insert into dirs (path, parent_id, first_observed)
            values(?, ?, ?)
        """,
            (path, parent_id, now),
        )
        return cursor.lastrowid

def update_file_data(connection, new_hash, filesize, path, mtime, now):
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            select rowid, hash, mtime, first_observed, dir_id
            from files
            where path = ?
            and deleted_before is null
        """,
            (path,),
        )
        rows = cursor.fetchall()
        if len(rows) > 0:
            assert len(rows) == 1
            rowid, old_hash, old_mtime, old_first_observed, old_dir_id = rows[0]
            dir_path = os.path.dirname(path)
            dir_id = _update_dir_data(cursor, dir_path, now)

            if old_hash == new_hash and old_mtime == mtime and dir_id == old_dir_id:
                # print("Nothing to change")
                return

            cursor.execute(
                """
                replace into files (rowid, hash, filesize, path, mtime, first_observed, dir_id)
                values(?, ?, ?, ?, ?, ?)
            """,
                (rowid, new_hash, filesize, path, mtime, old_first_observed, dir_id),
            )
        else:
            dir_path = os.path.dirname(path)
            dir_id = _update_dir_data(cursor, dir_path, now)

            cursor.execute(
                """
                insert into files (hash, filesize, path, mtime, first_observed, dir_id)
                values(?, ?, ?, ?, ?, ?)
            """,
                (new_hash, filesize, path, mtime, now, dir_id),
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
            (
                now,
                path,
            ),
        )
    finally:
        cursor.close()
