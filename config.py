# read cfg
from dataclasses import dataclass

# common cfg values
@dataclass
class Common:
    preferred: int
    interval: int
    optimistic: int
    filename: str
    size: int
    piece_size: int

    # count chunks
    @property
    def count(self):
        return (self.size + self.piece_size - 1) // self.piece_size

    # get chunk size
    def length(self, index):
        # check chunk id
        if not 0 <= index < self.count:
            raise ValueError('Invalid piece index')
        return min(self.piece_size, self.size - index * self.piece_size)

# one peer cfg
@dataclass
class PeerInfo:
    id: int
    host: str
    port: int
    seed: bool

# load cfg files
def load(root):
    # read common settings
    settings = dict(line.split() for line in (root / 'Common.cfg').read_text().splitlines() if line.strip())
    c = Common(int(settings['NumberOfPreferredNeighbors']),
               int(settings['UnchokingInterval']), int(settings['OptimisticUnchokingInterval']),
               settings['FileName'], int(settings['FileSize']), int(settings['PieceSize']))
    # check cfg numbers
    if c.preferred < 0 or min(c.interval, c.optimistic, c.size, c.piece_size) <= 0:
        raise ValueError('Invalid common settings')
    # check filename
    if '/' in c.filename or '\\' in c.filename or c.filename in ('.', '..'):
        raise ValueError('FileName must be a simple filename')
    # collect peer cfg
    peers = []
    # read peer rows
    for line in (root / 'PeerInfo.cfg').read_text().splitlines():
        # skip blank rows
        if line.strip():
            number, host, port, seed = line.split()
            p = PeerInfo(int(number), host, int(port), seed == '1')
            # check peer values
            if seed not in ('0', '1') or not 0 < p.id <= 0xffffffff or not 0 < p.port <= 65535:
                raise ValueError('Invalid peer settings')
            peers.append(p)
    # check ids and seeds
    if len({p.id for p in peers}) != len(peers) or not any(p.seed for p in peers):
        raise ValueError('Peer IDs must be unique and at least one peer must have the file')
    return c, peers
