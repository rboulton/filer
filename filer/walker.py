import hashlib
import heapq
import os
import re
import stat
import subprocess
import time

from . import db


REGULAR_FILE = 1
SYMLINK = 2


class Walker:
    def __init__(self, config):
        self.config = config
        self.exclude_patterns = [
            re.compile(pattern) for pattern in config.exclude_patterns
        ]
        self.swapfiles = self.find_swapfiles()
        self.db_conn = db.connect(self.config, read_only=False)
        db.init_schema(self.db_conn)
        self.batch_size = 1000
        self.batch_timeout = 10

    def log(self, message):
        print(message)

    def find_swapfiles(self):
        result = subprocess.run(
            ["/sbin/swapon", "--show=NAME", "--noheadings"], stdout=subprocess.PIPE
        )
        if result.returncode != 0:
            return []
        return result.stdout.decode("utf8").strip().split("\n")

    def calc_hash(self, path):
        # self.log("Calculating hash of {}".format(path))
        try:
            h = hashlib.sha512()
            with open(path, "rb") as fobj:
                while True:
                    d = fobj.read(1024 * 128)
                    if len(d) == 0:
                        break
                    h.update(d)
            return h.hexdigest()
        except PermissionError as e:
            self.log("PermissionError calculating hash for {} - skipping".format(path))
            return None

    def visit_files(self, batch):
        stored_data = {
            path: (stored_hash, stored_mtime)
            for stored_hash, path, stored_mtime in db.get_current_file_data(
                self.db_conn, [path for path, _ in batch]
            )
        }
        for path, mtime in batch:
            stored = stored_data.get(path)
            now = time.time()

            old_hash = None
            if stored:
                if stored[1] == mtime:
                    # No change since last visit
                    db.record_visit(self.db_conn, path)
                    continue
                self.log("stored timestamp for {} different from new timestamp: {} {}".format(path, repr(stored[1]), mtime))
                old_hash = stored[0]

            settled_time = mtime + self.config.settle_time
            if now < settled_time:
                # Changed more recently than settle_time
                self.log("file {} changed recently - will revisit after {}s".format(path, settled_time - time.time()))
                db.record_visit(self.db_conn, path, settled_time)
                continue

            # Check mtime again before we spend time calculating the hash
            new_mtime = int(os.path.getmtime(path))
            if new_mtime != mtime:
                # Changed since we logged this as something to be visited - revisit again later.
                self.log("file {} changed since we last looked at it - will revisit after {}s".format(path, new_mtime + self.config.settle_time - time.time()))
                db.record_visit(self.db_conn, path, new_mtime + self.config.settle_time)
                continue

            new_hash = self.calc_hash(path)
            if new_hash is None:
                # Couldn't hash it - drop this file (don't record a visit to it)
                continue

            # Check mtime after hash calculated
            new_mtime = int(os.path.getmtime(path))
            if new_mtime != mtime:
                # Changed since we started calculating the hash - revisit when it might have settled
                db.record_visit(self.db_conn, path, new_mtime + self.config.settle_time)
                continue

            # print("updating {} {} {}".format(path, mtime, now))
            db.update_file_data(self.db_conn, new_hash, path, mtime, now)
            db.record_visit(self.db_conn, path)
        self.db_conn.commit()

    def visit_symlinks(self, batch):
        for path, mtime in batch:
            self.log("symlink {} mtime={}".format(path, mtime))

    def listen(self):
        """Listen for updates

        Triggers calls to visit_files() and visit_symlinks() when big enough
        batches of either have been created.

        """
        file_batch = {}
        file_batch_time = None
        symlink_batch = {}
        symlink_batch_time = None

        seen = 0
        for item in self.iter_items():
            if item is None:
               continue
            path, type, stats = item
            now = time.time()
            seen += 1
            if path is not None:
                mtime = int(stats.st_mtime)
                modified_ago = now - mtime

                if type == REGULAR_FILE:
                    file_batch[path] = mtime
                    if file_batch_time is None:
                        file_batch_time = time.time() + self.batch_timeout
                elif type == SYMLINK:
                    symlink_batch[path] = mtime
                    if symlink_batch_time is None:
                        symlink_batch_time = time.time() + self.batch_timeout

            print("File batch {}".format(len(file_batch)))
            if len(file_batch) > self.batch_size or (
                file_batch_time is not None and file_batch_time < now
            ):
                batch = file_batch
                file_batch = {}
                file_batch_time = None
                self.visit_files(sorted(batch.items(), key=lambda x: (x[1], x[0])))

            if len(symlink_batch) > self.batch_size or (
                symlink_batch_time is not None and symlink_batch_time < now
            ):
                batch = symlink_batch
                symlink_batch = {}
                symlink_batch_time = None
                self.visit_symlinks(sorted(batch.items(), key=lambda x: (x[1], x[0])))

    def check_skip_dir(self, path, dirname):
        if path in self.config.exclude_paths:
            return True
        elif dirname in self.config.exclude_directories:
            return True
        else:
            for pattern in self.exclude_patterns:
                if pattern.search(path):
                    return True
        return False

    def check_skip_file(self, path):
        if path in self.config.exclude_paths:
            return True
        if path in self.swapfiles:
            return True
        elif path in self.config.exclude_paths:
            return True
        else:
            for pattern in self.exclude_patterns:
                if pattern.search(path):
                    return True
        return False

    def iter_items(self):
        """Walks over the roots, yielding each file and symlink found.

        Applies the exclusions from the config.

        """
        def to_yield(path, stats):
            if stat.S_ISREG(stats.st_mode):
                return path, REGULAR_FILE, stats
            elif stat.S_ISLNK(stats.st_mode):
                return path, SYMLINK, stats
            return None

        def check_visit(base, dirs, files, basefd):
            skip = []
            for dirname in dirs:
                d_path = os.path.normpath(
                    os.path.realpath(os.path.join(base, dirname))
                )
                if self.check_skip_dir(d_path, dirname):
                    skip.append(dirname)

            for dirname in skip:
                self.log("Skipping {}".format(dirname))
                dirs.remove(dirname)

            for name in files:
                f_path = os.path.normpath(
                    os.path.realpath(os.path.join(base, name))
                )

                if self.check_skip_file(f_path):
                    self.log("Skipping {}".format(f_path))
                    continue

                stats = os.stat(name, dir_fd=basefd, follow_symlinks=False)
                yield to_yield(f_path, stats)

        db.clear_visits(self.db_conn)

        for root in self.config.roots:
            self.log("Checking files under {}".format(root))
            try:
                for base, dirs, files, basefd in os.fwalk(root, follow_symlinks=False):
                    for y in check_visit(base, dirs, files, basefd):
                        yield y
            except FileNotFoundError as e:
                self.log("File not found - aborting scan of root {}".format(root))

        while True:
            now = time.time()
            next_revisit_time, revisit_paths = db.due_for_revisit(self.db_conn, now)
            self.log("Next revisit time: {} ({}s), due now: {}".format(next_revisit_time, (next_revisit_time or now) - now, len(revisit_paths)))

            for path in revisit_paths:
                print("Revisit {}".format(path))
                stats = os.stat(path, follow_symlinks=False)
                yield to_yield(path, stats)
            else:
                time.sleep(1)
            yield None


if __name__ == "__main__":
    import config

    Walker(config.config).listen()
