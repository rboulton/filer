from collections import namedtuple
import json
import os
import sys

__this_file = os.path.realpath(os.path.abspath(__file__))
__home_dir = os.path.expanduser("~")


CONFIG_PATHS = [
    os.path.expanduser("local_filer_config.json"),
    os.path.expanduser("~/.filer_config.json"),
    os.path.expanduser("~/.config/filer/config.json"),
    os.path.expanduser("/etc/filer/config.json"),
    os.path.join(os.path.dirname(__this_file), "config.json"),
]

Config = namedtuple(
    "Config",
    ["roots", "exclude_paths", "exclude_directories", "exclude_patterns", "db_dir"],
)


def load_config_from_path(path):
    with open(path, "rb") as fobj:
        data = json.load(fobj)

    datadir = data.pop("datadir", None)
    roots = data.pop("roots", "/")

    excludes = data.pop("exclude", {})
    exclude_paths = excludes.pop("paths", [])
    exclude_directories = excludes.pop("directories", [])
    exclude_patterns = excludes.pop("patterns", [])

    db_config = data.pop("db", {})
    db_dir = os.path.expanduser(db_config.pop("path", "~/.filer"))
    if not os.path.isabs(db_dir):
        raise ValueError(
            "Database directory must be an absolute path: got {}".format(db_dir)
        )

    if len(data) != 0:
        print(
            "Warning: unknown config items: {}".format(repr(data.keys())),
            file=sys.stderr,
        )
    if len(excludes) != 0:
        print(
            "Warning: unknown exclude items: {}".format(repr(excludes.keys())),
            file=sys.stderr,
        )
    if len(db_config) != 0:
        print(
            "Warning: unknown exclude items: {}".format(repr(db_config.keys())),
            file=sys.stderr,
        )

    return Config(roots, exclude_paths, exclude_directories, exclude_patterns, db_dir)


def load_config():
    for path in CONFIG_PATHS:
        path = os.path.abspath(path)
        if not os.path.isfile(path):
            continue
        return load_config_from_path(path)
    else:
        print(
            "No configuration file found: checked {}".format(", ".join(CONFIG_PATHS)),
            file=sys.stderr,
        )


config = load_config()