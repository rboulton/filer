import hashlib
import heapq
import os
import re
import stat
import subprocess
import time

import db


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
        self.revisit_queue = []  # arranged as a heap

    def find_swapfiles(self):
        result = subprocess.run(
            ["/sbin/swapon", "--show=NAME", "--noheadings"], stdout=subprocess.PIPE
        )
        if result.returncode != 0:
            return []
        return result.stdout.decode("utf8").strip().split("\n")

    def schedule_revisit(self, path, visit_time):
        heapq.heappush(self.revisit_queue, (visit_time, path))

    def calc_hash(self, path):
        print(path)
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
            print("PermissionError calculating hash - skipping")
            return None

    def visit_files(self, batch):
        stored_data = {
            path: (stored_hash, stored_mtime)
            for stored_hash, path, stored_mtime in db.get_current_file_data(self.db_conn, [
                path for path, stored_mtime in batch
            ])
        }
        for path, mtime in batch:
            stored = stored_data.get(path)
            old_hash = None
            if stored:
                if stored[1] == mtime:
                    continue
                old_hash = stored[0]

            new_hash = self.calc_hash(path)
            if new_hash is None:
                # Couldn't hash it - drop this file
                continue

            new_mtime = int(os.path.getmtime(path))
            if new_mtime != mtime:
                # Changed time since we started calculating the hash
                schedule_revisit(path, new_mtime + self.config.settle_time)
            else:
                now = time.time()
                db.update_file_data(self.db_conn, new_hash, path, mtime, now)
        self.db_conn.commit()

    def visit_symlinks(self, batch):
        for path, mtime in batch:
            print("S", path, mtime)

    def listen(self):
        """Listen for updates

        Triggers calls to visit_files() and visit_symlinks() when big enough
        batches of either have been created.

        """
        file_batch = []
        file_batch_time = None
        symlink_batch = []
        symlink_batch_time = None

        seen = 0
        for path, type, stats in self.iter_items():
            now = time.time()
            seen += 1
            print(seen)
            if path is not None:
                mtime = int(stats.st_mtime)
                modified_ago = now - mtime

                if modified_ago < self.config.settle_time:
                    self.schedule_revisit(path, mtime + self.config.settle_time)
                elif type == REGULAR_FILE:
                    file_batch.append((path, mtime))
                    if file_batch_time is None:
                        file_batch_time = time.time() + self.batch_timeout
                elif type == SYMLINK:
                    symlink_batch.append((path, mtime))
                    if symlink_batch_time is None:
                        symlink_batch_time = time.time() + self.batch_timeout

            print(len(file_batch), file_batch_time, now)
            if len(file_batch) > self.batch_size or (
                file_batch_time is not None and file_batch_time < now
            ):
                batch = file_batch
                file_batch = []
                file_batch_time = None
                self.visit_files(batch)

            if len(symlink_batch) > self.batch_size or (
                symlink_batch_time is not None and symlink_batch_time < now
            ):
                batch = symlink_batch
                symlink_batch = []
                symlink_batch_time = None
                self.visit_symlinks(batch)

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

        for root in self.config.roots:
            for base, dirs, files, basefd in os.fwalk(root, follow_symlinks=False):
                skip = []
                for dirname in dirs:
                    d_path = os.path.normpath(
                        os.path.realpath(os.path.join(base, dirname))
                    )
                    if self.check_skip_dir(d_path, dirname):
                        skip.append(dirname)

                for dirname in skip:
                    print("Skipping {}".format(dirname))
                    dirs.remove(dirname)

                for name in files:
                    f_path = os.path.normpath(
                        os.path.realpath(os.path.join(base, name))
                    )

                    if self.check_skip_file(f_path):
                        print("Skipping {}".format(f_path))
                        continue

                    stats = os.stat(name, dir_fd=basefd, follow_symlinks=False)
                    y = to_yield(f_path, stats)
                    if y:
                        yield y

        while True:
            now = time.time()
            if len(self.revisit_queue) > 0:
                revisit_time = self.revisit_queue[0][0]
                if now > revisit_time:
                    _, path = heapq.heappop(self.revisit_queue)
                    stats = os.stat(path, follow_symlinks=False)
                    y = to_yield(path, stats)
                    if y:
                        yield y
                    continue

            time.sleep(1)
            yield None, None, None


if __name__ == "__main__":
    import config

    Walker(config.config).listen()
