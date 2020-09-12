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
    [
        "config_path",
        "roots",
        "exclude_paths",
        "exclude_directories",
        "exclude_patterns",
        "db_dir",
        "settle_time",
    ],
)


def check_list_of_strings(data, name):
    for item in data:
        if not isinstance(item, str):
            raise ValueError(
                "Expected {} to be a list of strings, got {}".format(name, repr(data))
            )


def load_config_from_path(path):
    with open(path, "rb") as fobj:
        data = json.load(fobj)

    datadir = data.pop("datadir", None)
    roots = data.pop(
        "roots",
        [
            "/",
        ],
    )
    check_list_of_strings(roots, "roots")

    excludes = data.pop("exclude", {})
    exclude_paths = excludes.pop("paths", [])
    check_list_of_strings(exclude_paths, "exclude.paths")
    exclude_directories = excludes.pop("directories", [])
    check_list_of_strings(exclude_directories, "exclude.directories")
    exclude_patterns = excludes.pop("patterns", [])
    check_list_of_strings(exclude_patterns, "exclude.patterns")

    db_config = data.pop("db", {})
    db_dir = os.path.abspath(os.path.expanduser(db_config.pop("dir", "~/.filer")))

    times = data.pop("times", {})
    settle_time = max(float(times.pop("settle", 30.0)), 0.0)

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
    if len(times) != 0:
        print(
            "Warning: unknown times items: {}".format(repr(times.keys())),
            file=sys.stderr,
        )

    return Config(
        path,
        roots,
        exclude_paths,
        exclude_directories,
        exclude_patterns,
        db_dir,
        settle_time,
    )


def load_config():
    for path in CONFIG_PATHS:
        path = os.path.realpath(os.path.abspath(path))
        if not os.path.isfile(path):
            continue
        return load_config_from_path(path)
    else:
        print(
            "No configuration file found: checked {}".format(", ".join(CONFIG_PATHS)),
            file=sys.stderr,
        )


def config_paths():
    return [os.path.abspath(path) for path in CONFIG_PATHS]


config = load_config()
