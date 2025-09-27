#!/usr/bin/env python3

from server.filter_node import FilterNode
from common.config import get_config
import logging


def main():
    config = get_config()
    filter_config = config.get_filter_config()
    logging_level = config.get_logging_level()

    initialize_log(logging_level)

    logging.info(
        f"action: filter_node_config | result: success | "
        f"input_queue: {filter_config['input_queue']} | "
        f"output_queues: {filter_config['output_queues']}"
    )

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
