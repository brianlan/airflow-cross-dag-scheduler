from pathlib import Path
import asyncio
import signal
import yaml
import argparse

from scheduler.watcher.base_watcher import create_watcher
from scheduler.helpers.base import read_cookie_session

parser = argparse.ArgumentParser()
parser.add_argument("--batch-config", type=Path, required=True, help="path to batch config file")
parser.add_argument("--cookie-session-path", type=Path, required=True, help="path to the session ")
parser.add_argument("--api-url", default="http://127.0.0.1:8080")


async def main():
    # get args
    args = parser.parse_args()

    # read batch config
    batch_id = args.batch_config.stem
    cookies = {"session": read_cookie_session(args.cookie_session_path)}
    with open(args.batch_config, "r") as f:
        cfg = yaml.safe_load(f)
    
    # create Watchers
    watchers = [create_watcher(args.api_url, batch_id, cookies, wc) for wc in cfg["watchers"]]

    # Launch all nodes
    asyncio_tasks = [asyncio.create_task(node.run()) for node in watchers]

    await asyncio.gather(*asyncio_tasks)


def stop_loop(signum, frame):
    print("SIGTERM received, stopping...")
    loop = asyncio.get_event_loop()
    loop.stop()


if __name__ == "__main__":
    # Setup signal handling
    signal.signal(signal.SIGTERM, stop_loop)

    # Run the main coroutine
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
