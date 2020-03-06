import hashlib
import heapq
import os
import re
import stat
import subprocess
import time
import pyinotify
import asyncio

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
        self.batch_timeout = 5
        self.watch_manager = pyinotify.WatchManager()
        self.watch_mask = pyinotify.ALL_EVENTS
        self.watch_mask = (
            pyinotify.IN_ATTRIB
            | pyinotify.IN_ATTRIB
            | pyinotify.IN_CREATE
            | pyinotify.IN_DELETE
            | pyinotify.IN_DELETE_SELF
            | pyinotify.IN_MODIFY
            | pyinotify.IN_MOVE_SELF
            | pyinotify.IN_MOVED_FROM
            | pyinotify.IN_MOVED_TO
            | pyinotify.IN_DONT_FOLLOW
            | pyinotify.IN_EXCL_UNLINK
        )
        self.loop = asyncio.get_event_loop()

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
        revisits_queued = False

        stored_data = {
            path: (stored_hash, stored_mtime)
            for stored_hash, path, stored_mtime in db.get_current_file_data(
                self.db_conn, [path for path, _ in batch]
            )
        }
        deletes = set()

        for path, mtime in batch:
            if mtime is None:
                deletes.add(path)
                continue

            now = time.time()

            old_hash = None
            stored = stored_data.get(path)
            if stored:
                if stored[1] == mtime:
                    # No change since last visit
                    db.record_visit(self.db_conn, path)
                    continue
                self.log(
                    "stored timestamp for {} different from new timestamp: {} {}".format(
                        path, repr(stored[1]), mtime
                    )
                )
                old_hash = stored[0]

            settled_time = mtime + self.config.settle_time
            if now < settled_time:
                # Changed more recently than settle_time
                self.log(
                    "file {} changed recently - will revisit after {}s".format(
                        path, settled_time - time.time()
                    )
                )
                db.record_visit(self.db_conn, path, settled_time)
                revisits_queued = True
                continue

            # Check mtime again before we spend time calculating the hash
            try:
                new_mtime = int(os.path.getmtime(path))
            except FileNotFoundError:
                deletes.add(path)
                continue

            if new_mtime != mtime:
                # Changed since we logged this as something to be visited - revisit again later.
                self.log(
                    "file {} changed since we last looked at it - will revisit after {}s".format(
                        path, new_mtime + self.config.settle_time - time.time()
                    )
                )
                db.record_visit(self.db_conn, path, new_mtime + self.config.settle_time)
                revisits_queued = True
                continue

            new_hash = self.calc_hash(path)
            if new_hash is None:
                # Couldn't hash it - drop this file (don't record a visit to it)
                deletes.add(path)
                continue

            # Check mtime after hash calculated
            try:
                new_mtime = int(os.path.getmtime(path))
            except FileNotFoundError:
                deletes.add(path)
                continue

            if new_mtime != mtime:
                # Changed since we started calculating the hash - revisit when it might have settled
                db.record_visit(self.db_conn, path, new_mtime + self.config.settle_time)
                revisits_queued = True
                continue

            # print("updating {} {} {}".format(path, mtime, now))
            db.update_file_data(self.db_conn, new_hash, path, mtime, now)
            db.record_visit(self.db_conn, path)

        for path in deletes:
            # Check file still doesn't exist
            # Note - it's possible the file gets created between this check
            # and the write to the db - this doesn't matter, because we'll
            # get a file update notification if this happens, and guarantee
            # to process that after the db has been updated, so there's no
            # race condition here.
            try:
                new_mtime = int(os.path.getmtime(path))
            except FileNotFoundError:
                new_mtime = None
            if new_mtime:
                db.record_visit(self.db_conn, path, new_mtime + self.config.settle_time)
                revisits_queued = True
            else:
                db.record_visit(self.db_conn, path, deleted=True)
                db.update_deleted_file_data(self.db_conn, path, time.time())

        self.db_conn.commit()
        return revisits_queued

    def visit_symlinks(self, batch):
        for path, mtime in batch:
            self.log("symlink {} mtime={}".format(path, mtime))

    def listen(self):
        """Listen for updates

        Triggers calls to visit_files() and visit_symlinks() when big enough
        batches of either have been created.

        """
        self.init_delete_batch_processing()
        self.init_file_batch_processing()
        self.init_symlink_batch_processing()

        self.loop.create_task(self.start_watching_roots())

        self.revisit_cond = asyncio.Condition()
        self.loop.create_task(self.start_polling_revisits())

        self.start_polling_changes()
        self.loop.run_forever()
        self.stop_polling_changes()

    async def process_change(self, path, stats):
        if path is None:
            return
        if stats is None:
            await self.add_to_delete_batch(path)
            return

        mtime = int(stats.st_mtime)

        if stat.S_ISREG(stats.st_mode):
            await self.add_to_file_batch(path, mtime)
        elif stat.S_ISLNK(stats.st_mode):
            await self.add_to_symlink_batch(path, mtime)
        else:
            print("Unexpected change stats: {}".format(str(stats)))

    def init_delete_batch_processing(self):
        self.delete_batch = {}
        self.delete_batch_time = None
        self.delete_batch_cond = asyncio.Condition()
        self.loop.create_task(self.start_polling_delete_batches())

    async def add_to_delete_batch(self, path):
        self.delete_batch[path] = None
        if self.delete_batch_time is None:
            self.delete_batch_time = time.time() + self.batch_timeout
        if len(self.delete_batch) > self.batch_size:
            await self.process_delete_batch()
        async with self.delete_batch_cond:
            self.delete_batch_cond.notify_all()

    async def start_polling_delete_batches(self):
        while True:
            while self.delete_batch_time is not None:
                wait_time = self.delete_batch_time - time.time()
                if wait_time <= 0:
                    await self.process_delete_batch()
                else:
                    self.log(
                        "Next delete batch time: {} ({}s)".format(
                            self.delete_batch_time, wait_time
                        )
                    )
                    await asyncio.sleep(wait_time)
            self.log("No delete batch due")
            async with self.delete_batch_cond:
                await self.delete_batch_cond.wait()

    async def process_delete_batch(self):
        print("processing delete batch size: {}".format(len(self.delete_batch)))
        batch = self.delete_batch
        self.delete_batch = {}
        self.delete_batch_time = None
        revisits_queued = self.visit_files(
            sorted(batch.items())
        ) or self.visit_symlinks(sorted(batch.items()))
        if revisits_queued:
            async with self.revisit_cond:
                self.revisit_cond.notify_all()

    def init_file_batch_processing(self):
        self.file_batch = {}
        self.file_batch_time = None
        self.file_batch_cond = asyncio.Condition()
        self.loop.create_task(self.start_polling_file_batches())

    async def add_to_file_batch(self, path, mtime):
        self.file_batch[path] = mtime
        if self.file_batch_time is None:
            self.file_batch_time = time.time() + self.batch_timeout
        if len(self.file_batch) > self.batch_size:
            await self.process_file_batch()
        async with self.file_batch_cond:
            self.file_batch_cond.notify_all()

    async def start_polling_file_batches(self):
        while True:
            while self.file_batch_time is not None:
                wait_time = self.file_batch_time - time.time()
                if wait_time <= 0:
                    await self.process_file_batch()
                else:
                    self.log(
                        "Next file batch time: {} ({}s)".format(
                            self.file_batch_time, wait_time
                        )
                    )
                    await asyncio.sleep(wait_time)
            self.log("No file batch due")
            async with self.file_batch_cond:
                await self.file_batch_cond.wait()

    async def process_file_batch(self):
        print("processing file batch size: {}".format(len(self.file_batch)))
        batch = self.file_batch
        self.file_batch = {}
        self.file_batch_time = None
        revisits_queued = self.visit_files(
            sorted(batch.items(), key=lambda x: (x[1], x[0]))
        )
        if revisits_queued:
            async with self.revisit_cond:
                self.revisit_cond.notify_all()

    def init_symlink_batch_processing(self):
        self.symlink_batch = {}
        self.symlink_batch_time = None
        self.symlink_batch_cond = asyncio.Condition()
        self.loop.create_task(self.start_polling_symlink_batches())

    async def add_to_symlink_batch(self, path, mtime):
        self.symlink_batch[path] = mtime
        if self.symlink_batch_time is None:
            self.symlink_batch_time = time.time() + self.batch_timeout
        if len(self.symlink_batch) > self.batch_size:
            await self.process_symlink_batch()
        async with self.symlink_batch_cond:
            self.symlink_batch_cond.notify_all()

    async def start_polling_symlink_batches(self):
        while True:
            while self.symlink_batch_time is not None:
                wait_time = self.symlink_batch_time - time.time()
                if wait_time <= 0:
                    await self.process_symlink_batch()
                else:
                    self.log(
                        "Next symlink batch time: {} ({}s)".format(
                            self.symlink_batch_time, wait_time
                        )
                    )
                    await asyncio.sleep(wait_time)
            self.log("No symlink batch due")
            async with self.symlink_batch_cond:
                await self.symlink_batch_cond.wait()

    async def process_symlink_batch(self):
        print("processing symlink batch size: {}".format(len(self.symlink_batch)))
        batch = self.symlink_batch
        self.symlink_batch = {}
        self.symlink_batch_time = None
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

    async def start_watching_roots(self):
        """Walks over the roots, setting up watches and processing the items found.

        Doesn't follow symlinks.

        Applies the exclusions from the config.

        """
        db.clear_visits(self.db_conn)
        for root in self.config.roots:
            await self.watch_tree(root)

    async def watch_tree(self, root):
        self.log("Checking files under {}".format(root))

        async def watch_dir(base, dirs, files, basefd):
            skip = []
            for dirname in dirs:
                d_path = os.path.normpath(os.path.realpath(os.path.join(base, dirname)))
                if self.check_skip_dir(d_path, dirname):
                    skip.append(dirname)
                else:
                    self.watch_manager.add_watch(d_path, self.watch_mask)
                print("D", end="", flush=True)

            for dirname in skip:
                self.log("Skipping {}".format(dirname))
                dirs.remove(dirname)

            for name in files:
                f_path = os.path.normpath(os.path.realpath(os.path.join(base, name)))

                if self.check_skip_file(f_path):
                    self.log("Skipping {}".format(f_path))
                    continue

                try:
                    stats = os.stat(name, dir_fd=basefd, follow_symlinks=False)
                except FileNotFoundError:
                    stats = None
                await self.process_change(f_path, stats)

                print(".", end="", flush=True)

        try:
            d_path = os.path.normpath(os.path.realpath(root))
            if os.path.isdir(d_path):
                if not self.check_skip_dir(d_path, os.path.basename(d_path)):
                    self.watch_manager.add_watch(d_path, self.watch_mask)

            for base, dirs, files, basefd in os.fwalk(root, follow_symlinks=False):
                await watch_dir(base, dirs, files, basefd)
        except FileNotFoundError as e:
            self.log("File not found - aborting scan of root {}".format(root))

    async def start_polling_revisits(self):
        """Start task that triggers revisiting of paths that hadn't settled
        when we last checked.

        """
        while True:
            now = time.time()
            next_revisit_time, revisit_paths = db.due_for_revisit(self.db_conn, now)
            self.log(
                "Next revisit time: {} ({}s), due now: {}".format(
                    next_revisit_time,
                    (next_revisit_time or now) - now,
                    len(revisit_paths),
                )
            )

            for path in revisit_paths:
                try:
                    stats = os.stat(path, follow_symlinks=False)
                except FileNotFoundError:
                    stats = None
                await self.process_change(path, stats)
            else:
                if next_revisit_time is None:
                    async with self.revisit_cond:
                        await self.revisit_cond.wait()
                else:
                    await asyncio.sleep(1)

    def start_polling_changes(self):
        def process_inotify_event(event):
            async def task():
                print("EVENT: {}".format(str(event)))
                try:
                    stats = os.stat(event.pathname, follow_symlinks=False)
                except FileNotFoundError:
                    stats = None
                await self.process_change(event.pathname, stats)

            self.loop.create_task(task())

        self.notifier = pyinotify.AsyncioNotifier(
            self.watch_manager, self.loop, default_proc_fun=process_inotify_event
        )

    def stop_polling_changes(self):
        self.notifier.stop()


if __name__ == "__main__":
    import config

    Walker(config.config).listen()
