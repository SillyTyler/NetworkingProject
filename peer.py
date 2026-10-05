# peer stuff
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from config import load
from protocol import Type, U32, encode, handshake, parse_handshake, receive
from protocol import pack_bits, unpack_bits
from storage import Pieces

# neighbor state
@dataclass
class Neighbor:
    id: int
    writer: object
    pieces: set = field(default_factory=set)
    # they want chunks
    interested: bool = False
    # we want chunks
    our_interest: object = None
    # they block us
    choked: bool = True
    # we block them
    choking: bool = True
    # chunk id and time
    pending: object = None
    # recv bytes
    downloaded: int = 0
    first: bool = True

# local peer state
class Peer:
    # set up peer state
    def __init__(self, peer_id, root, bind='0.0.0.0', request_timeout=30):
        # set local cfg
        self.id, self.root, self.bind = peer_id, root, bind
        self.config, self.infos = load(root)
        self.order = [p.id for p in self.infos]
        self.info = next((p for p in self.infos if p.id == peer_id), None)
        # check local peer id
        if self.info is None:
            raise ValueError('Peer ID is not in PeerInfo.cfg')
        # open chunk storage
        self.store = Pieces(root, self.info, self.config)
        self.neighbors = {}
        # active reqs
        self.reserved = {}
        # track complete peers
        self.known_complete = {p.id for p in self.infos if p.seed}
        self.preferred = set()
        self.optimistic = None
        self.timeout = request_timeout
        self.done = asyncio.Event()
        self.tasks = set()
        # set up peer log
        self.log = logging.getLogger(f'peer.{peer_id}')
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        handler = logging.FileHandler(root / f'log_peer_{peer_id}.log', mode='w')
        handler.setFormatter(logging.Formatter('[%(asctime)s]: %(message)s', '%Y-%m-%d %H:%M:%S'))
        self.log.addHandler(handler)
        self.handler = handler

    # write log msg
    def event(self, text):
        self.log.info('Peer %s %s', self.id, text)

    # start async task
    def task(self, coro):
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        return t

    # send msg
    def send(self, n, kind, payload=b''):
        # send msg
        n.writer.write(encode(kind, payload))

    # free pending req
    def release(self, n):
        # check pending req
        if n.pending is not None:
            index, _ = n.pending
            # free matching reservation
            if self.reserved.get(index) == n.id:
                del self.reserved[index]
            n.pending = None

    # update interest
    def interest(self, n):
        # check useful remote chunks
        wants = bool(n.pieces - self.store.have)
        # send interest change
        if wants != n.our_interest:
            self.send(n, Type.INTERESTED if wants else Type.NOT_INTERESTED)
            n.our_interest = wants

    # req missing chunk
    def request(self, n):
        # skip blocked req
        if n.choked or n.pending is not None or self.store.complete:
            return
        # find missing unreserved chunks
        choices = n.pieces - self.store.have - self.reserved.keys()
        # pick available chunk
        if choices:
            index = random.choice(tuple(choices))
            self.reserved[index] = n.id
            n.pending = (index, time.monotonic())
            self.send(n, Type.REQUEST, U32.pack(index))

    # refresh peer reqs
    def refresh(self):
        # visit neighbors
        for n in self.neighbors.values():
            self.interest(n)
            self.request(n)

    # check all peers done
    def check_complete(self):
        # mark local completion
        if self.store.complete:
            self.known_complete.add(self.id)
        # finish when all done
        if set(self.order) <= self.known_complete:
            self.done.set()

    # read chunk id
    def index(self, payload):
        i = U32.unpack(payload[:4])[0]
        self.config.length(i)
        return i

    # handle recv msg
    def handle(self, n, kind, payload):
        # read initial bits
        if kind == Type.BITFIELD:
            # reject late bitfield
            if not n.first:
                raise ValueError('Bitfield must be the first message')
            n.pieces = unpack_bits(payload, self.config.count)
            self.interest(n)
        # add remote chunk
        elif kind == Type.HAVE:
            i = self.index(payload)
            n.pieces.add(i)
            self.event(f"received the 'have' message from {n.id} for the piece {i}.")
            self.interest(n)
        # mark remote interest
        elif kind == Type.INTERESTED:
            n.interested = True
            self.event(f"received the 'interested' message from {n.id}.")
        # clear remote interest
        elif kind == Type.NOT_INTERESTED:
            n.interested = False
            self.event(f"received the 'not interested' message from {n.id}.")
        # pause remote downloads
        elif kind == Type.CHOKE:
            n.choked = True
            self.release(n)
            self.event(f'is choked by {n.id}.')
            self.refresh()
        # allow remote downloads
        elif kind == Type.UNCHOKE:
            n.choked = False
            self.event(f'is unchoked by {n.id}.')
        # handle chunk req
        elif kind == Type.REQUEST:
            i = self.index(payload)
            # upload available chunk
            if not n.choking and i in self.store.have:
                self.send(n, Type.PIECE, U32.pack(i) + self.store.read(i))
        # handle recv chunk
        elif kind == Type.PIECE:
            i = self.index(payload)
            # check recv chunk size
            if len(payload[4:]) != self.config.length(i):
                raise ValueError('Piece payload length disagrees with configuration')
            # late chunk ok
            if n.pending is not None and n.pending[0] == i:
                self.release(n)
            # save new chunk
            if self.store.put(i, payload[4:]):
                n.downloaded += len(payload) - 4
                self.event(f'has downloaded the piece {i} from {n.id}. Now the number of pieces it has is {len(self.store.have)}.')
                # announce chunk to neighbors
                for other in self.neighbors.values():
                    self.send(other, Type.HAVE, U32.pack(i))
                # mark local completion
                if self.store.complete:
                    self.event('has downloaded the complete file.')
                self.refresh()
        # update remote completion
        n.first = False
        # mark remote completion
        if len(n.pieces) == self.config.count:
            self.known_complete.add(n.id)
        self.request(n)
        self.check_complete()

    # handle one conn
    async def connection(self, reader, writer, expected=None):
        n = None
        # try operation
        try:
            # send local handshake
            writer.write(handshake(self.id))
            await writer.drain()
            # recv remote handshake
            raw = await asyncio.wait_for(reader.readexactly(32), 10)
            other = parse_handshake(raw)
            # reject unknown peer
            if other not in self.order or other == self.id:
                raise ValueError('Unknown or self peer')
            # check outgoing peer id
            if expected is not None and other != expected:
                raise ValueError('Handshake peer ID does not match destination')
            # check incoming peer order
            if expected is None and self.order.index(other) <= self.order.index(self.id):
                raise ValueError('Incoming peer violates configured connection order')
            # reject duplicate conn
            if other in self.neighbors:
                raise ValueError('Duplicate connection')
            # create neighbor state
            n = Neighbor(other, writer)
            # send bits first
            self.send(n, Type.BITFIELD, pack_bits(self.store.have, self.config.count))
            self.neighbors[other] = n
            phrase = f'makes a connection to Peer {other}.' if expected else f'is connected from Peer {other}.'
            self.event(phrase)
            # bound msg size
            maximum = max(5 + self.config.piece_size, 1 + (self.config.count + 7) // 8)
            # keep conn open
            while True:
                kind, payload = await receive(reader, maximum)
                self.handle(n, kind, payload)
                await writer.drain()
        # handle conn or file error
        except (OSError, asyncio.IncompleteReadError, ValueError, asyncio.TimeoutError) as exc:
            # keep running until done
            if not self.done.is_set():
                self.event(f'connection ended: {exc!r}.')
        # clean up resources
        finally:
            # remove closed neighbor
            if n is not None and self.neighbors.get(n.id) is n:
                self.release(n)
                del self.neighbors[n.id]
                self.preferred.discard(n.id)
                # clear lost extra peer
                if self.optimistic == n.id:
                    self.optimistic = None
                self.refresh()
            # close conn
            writer.close()
            # try operation
            try:
                await asyncio.wait_for(writer.wait_closed(), 2)
            # handle conn or file error
            except (OSError, asyncio.TimeoutError):
                pass

    # connect to earlier peer
    async def connect(self, info):
        # repeat until all done
        while not self.done.is_set():
            # try operation
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(info.host, info.port), 5)
                await self.connection(reader, writer, info.id)
            # handle conn or file error
            except (OSError, asyncio.TimeoutError):
                pass
            # keep running until done
            if not self.done.is_set():
                await asyncio.sleep(1)

    # update upload permission
    def apply_choking(self):
        # collect allowed uploads
        allowed = self.preferred | ({self.optimistic} if self.optimistic is not None else set())
        # visit neighbors
        for n in self.neighbors.values():
            choke = n.id not in allowed
            # send choke change
            if choke != n.choking:
                n.choking = choke
                self.send(n, Type.CHOKE if choke else Type.UNCHOKE)

    # pick preferred peers
    def choose_preferred(self):
        # find interested peers
        interested = [n for n in self.neighbors.values() if n.interested]
        # random ties
        random.shuffle(interested)
        # sort by recv rate
        if not self.store.complete:
            interested.sort(key=lambda n: n.downloaded, reverse=True)
        # select top peers
        selected = {n.id for n in interested[:self.config.preferred]}
        # save preferred peers
        if selected != self.preferred:
            self.preferred = selected
            self.event('has the preferred neighbors ' + ','.join(map(str, sorted(selected))) + '.')
        # visit neighbors
        for n in self.neighbors.values():
            n.downloaded = 0
        self.apply_choking()

    # pick random extra peer
    def choose_optimistic(self):
        # find choked interested peers
        choices = [n.id for n in self.neighbors.values() if n.choking and n.interested]
        selected = random.choice(choices) if choices else None
        # save extra peer
        if selected != self.optimistic:
            self.optimistic = selected
            # log selected extra peer
            if selected is not None:
                self.event(f'has the optimistically unchoked neighbor {selected}.')
        self.apply_choking()

    # run interval checks
    async def timer(self):
        # read timer clock
        now = time.monotonic()
        # set preferred deadline
        preferred_at = now + self.config.interval
        # set extra peer deadline
        optimistic_at = now + self.config.optimistic
        status_at = now + 10
        # repeat until all done
        while not self.done.is_set():
            await asyncio.sleep(0.1)
            # read timer clock
            now = time.monotonic()
            # refresh preferred peers
            if now >= preferred_at:
                self.choose_preferred()
                # set preferred deadline
                preferred_at = now + self.config.interval
            # refresh extra peer
            if now >= optimistic_at:
                self.choose_optimistic()
                # set extra peer deadline
                optimistic_at = now + self.config.optimistic
            # check pending req times
            for n in list(self.neighbors.values()):
                # check req timeout
                if n.pending is not None and now - n.pending[1] > self.timeout:
                    # retry conn
                    self.release(n)
                    n.writer.close()
            self.refresh()
            self.check_complete()
            # log missing completion info
            if self.store.complete and not self.done.is_set() and now >= status_at:
                missing = [str(i) for i in self.order if i not in self.known_complete]
                self.event('is waiting for completion evidence from peers ' + ','.join(missing) + '.')
                status_at = now + 10

    # run this peer
    async def run(self):
        # accept incoming conn
        def accept(reader, writer):
            self.task(self.connection(reader, writer))
        # start listening
        server = await asyncio.start_server(accept, self.bind, self.info.port)
        print(f'Peer {self.id} listening on {self.bind}:{self.info.port}', flush=True)
        # try operation
        try:
            # find startup position
            position = self.order.index(self.id)
            # connect to earlier peers
            for info in self.infos[:position]:
                self.task(self.connect(info))
            self.task(self.timer())
            self.check_complete()
            # wait for global completion
            await self.done.wait()
            # send last msgs
            await asyncio.gather(*(n.writer.drain() for n in self.neighbors.values()), return_exceptions=True)
            await asyncio.sleep(2)
            print(f'Peer {self.id}: all peers have the complete file.', flush=True)
        # clean up resources
        finally:
            # stop listening
            server.close()
            await server.wait_closed()
            tasks = list(self.tasks)
            # cancel async tasks
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # close storage and log
            self.store.close()
            self.handler.close()
            self.log.removeHandler(self.handler)
