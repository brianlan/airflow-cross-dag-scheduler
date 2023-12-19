import asyncio
import signal


class MasterNode:
    def __init__(self, data):
        self.data = data

    async def run(self):
        while True:
            # Process the data
            await asyncio.sleep(1)  # Simulate processing delay


class DownstreamNode:
    def __init__(self, api_url):
        self.api_url = api_url

    async def run(self):
        while True:
            # Query the REST API and trigger action
            await asyncio.sleep(1)  # Simulate query delay


async def main():
    # Predefined data for the Master Node
    data = ["data1", "data2", "data3"]
    master_node = MasterNode(data)

    # API URL for Downstream Nodes
    api_url = "https://example.com/api"

    # Create Downstream Nodes
    downstream_nodes = [DownstreamNode(api_url) for _ in range(5)]

    # Launch all nodes
    tasks = [asyncio.create_task(master_node.run())] + [asyncio.create_task(node.run()) for node in downstream_nodes]

    await asyncio.gather(*tasks)


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
