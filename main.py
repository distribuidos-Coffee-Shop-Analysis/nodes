#!/usr/bin/env python3

from server.filter_node import FilterNode
import logging


def main():
    filter_node = FilterNode()

    try:
        filter_node.run()
    except KeyboardInterrupt:
        logging.info(
            "action: filter_node_shutdown | result: in_progress | msg: received keyboard interrupt"
        )
    except Exception as e:
        logging.error(f"action: filter_node_main | result: fail | error: {e}")


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


if __name__ == "__main__":
    main()
