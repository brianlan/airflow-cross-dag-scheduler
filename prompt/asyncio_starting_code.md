I want to use python's asyncio to implement a daemon application with the following requirements:
1. this daemon application launches several nodes in the beginning
2. there're 2 kinds of nodes:
  - one kind is called master node (can only be one instance of this kind), which consumes a predefined list of data.
  - the other kind is called downstream node, which regularly queries through a RESTAPI to get the singal whether it can trigger an action
3. once started, these nodes will run forever to check their upstreams until the application receives a SIGTERM.