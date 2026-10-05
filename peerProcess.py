# start peer
import asyncio
import sys
from pathlib import Path
from peer import Peer

# start script
if __name__ == '__main__':
    # check args
    if len(sys.argv) not in (2, 3):
        sys.exit('Usage: python peerProcess.py PEER_ID [DIRECTORY]')
    # select cfg directory
    root = Path(sys.argv[2]) if len(sys.argv) == 3 else Path.cwd()
    # try operation
    try:
        asyncio.run(Peer(int(sys.argv[1]), root.resolve()).run())
    # handle manual stop
    except KeyboardInterrupt:
        print('Peer stopped.')
    # handle conn or file error
    except (OSError, ValueError) as error:
        sys.exit(f'Error: {error}')
