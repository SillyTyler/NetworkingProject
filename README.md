# NetworkingProject — basic version

537 total lines of Python, across six files. Python 3.11 or newer. no extra packages (yet).
Team members: Tyler Summerville,

## Run the demo on Windows Terminal

```
py -3 demo.py
```

One command creates a 21 MiB binary file, starts six peers in order, checks that all
six exit, and compares their SHA-256 hashes. Successful output ends with PASS.
Data and logs go into `demo_run`

Each run deletes and recreates the generated `demo_run` folder beside the scripts.
## The six modules

| File | Purpose |
|---|---|
| demo.py | Create data, launch peers, verify files |
| peerProcess.py | Start a single peer |
| config.py | Read Common.cfg and PeerInfo.cfg |
| protocol.py | Handshake, messages, bitfields |
| storage.py | Read and write file pieces |
| peer.py | Connections, requests, choking timers, logs, completion |

`asyncio` runs the connections and timers on one event loop. Each neighbor has its
own state. A global reservation map prevents duplicate simultaneous piece requests.
Received pieces are written at their correct file offset, then announced with HAVE.
Preferred neighbors are chosen by download rate, complete peers choose randomly.
An additional neighbor is randomly unchoked on the optimistic interval.
Connections remain open while final completion notifications drain before shutdown.

