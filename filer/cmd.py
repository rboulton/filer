import argparse
from . import config
from .walker import Walker


def run():
    parser = argparse.ArgumentParser(description="Track files in filesystem.")
    parser.add_argument(
        "--config-paths",
        action="store_true",
        help="Display the locations of config files which are checked",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Display the configuration that will be used",
    )

    args = parser.parse_args()

    if args.config_paths:
        print(
            "Configuration paths checked:\n\n  {}\n".format(
                "\n  ".join(config.config_paths())
            )
        )
        return

    if args.show_config:
        import pprint

        print("Configuration:\n")
        for key, value in config.config._asdict().items():
            print(key, end=" = ")
            pprint.pprint(value)
        print()
        return

    walker = Walker(config.config)
    walker.listen()
