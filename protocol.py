# msg stuff
import struct
from enum import IntEnum

U32 = struct.Struct('!I')
HEADER = b'P2PFILESHARINGPROJ'

# msg type ids
class Type(IntEnum):
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7

# make handshake
def handshake(peer_id):
    return HEADER + bytes(10) + U32.pack(peer_id)

# check handshake
def parse_handshake(data):
    # check handshake bytes
    if len(data) != 32 or data[:28] != HEADER + bytes(10):
        raise ValueError('Invalid handshake')
    return U32.unpack(data[28:])[0]

# pack msg
def encode(kind, payload=b''):
    return U32.pack(1 + len(payload)) + bytes([kind]) + payload

# recv msg
async def receive(reader, maximum):
    # read full msg
    size = U32.unpack(await reader.readexactly(4))[0]
    # check msg length
    if not 1 <= size <= maximum:
        raise ValueError('Invalid message length')
    # read msg body
    body = await reader.readexactly(size)
    kind, payload = Type(body[0]), body[1:]
    # check control payload
    if kind.value < 4 and payload:
        raise ValueError('Control message has a payload')
    # check index payload
    if kind in (Type.HAVE, Type.REQUEST) and len(payload) != 4:
        raise ValueError('Index must be four bytes')
    # check chunk payload
    if kind == Type.PIECE and len(payload) < 5:
        raise ValueError('Truncated piece')
    return kind, payload

# make bitfield
def pack_bits(pieces, count):
    # create empty bits
    data = bytearray((count + 7) // 8)
    # set chunk bits
    for index in pieces:
        data[index // 8] |= 1 << (7 - index % 8)
    return bytes(data)

# read bitfield
def unpack_bits(data, count):
    # count unused bits
    spare = (-count) % 8
    # check bits and padding
    if len(data) != (count + 7) // 8 or (spare and data[-1] & ((1 << spare) - 1)):
        raise ValueError('Invalid bitfield length or padding')
    return {i for i in range(count) if data[i // 8] & (1 << (7 - i % 8))}
