from binascii import hexlify, unhexlify
from hashlib import sha256

from models import TXOUT_TYPES


BASE58_CHARS = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


TX_OUTPUT_TYPE_MAPPINGS = {
    38:     TXOUT_TYPES.P2PKH,      # GRLC 'G'
    50:     TXOUT_TYPES.P2SH,       # GRLC 'M'
    58:     TXOUT_TYPES.P2SH,       # GRLC testnet 'Q'
    65:     TXOUT_TYPES.P2PKH,      # TUX 'T'
    73:     TXOUT_TYPES.P2WPKH,     # GRLC 'W'
    111:    TXOUT_TYPES.P2PKH,      # GRLC testnet 'm'
    135:    TXOUT_TYPES.P2WPKH      # GRLC testnet 'w'
}


class InvalidAddressException(Exception):
    pass

class InvalidHashException(Exception):
    pass


class Address(object):
    def __init__(self, address):
        self.address = address
        self.version, self.rawhash, self.checksum = self.decode(address)
        self.hash = hexlify(self.rawhash)
        self.assert_checksum_valid()

    def transaction_output_type(self):
        return TX_OUTPUT_TYPE_MAPPINGS[self.version]

    def assert_checksum_valid(self):
        if sha256(sha256(chr(self.version) + self.rawhash).digest()).digest()[:4] != self.checksum:
            raise InvalidAddressException(self.address)

    @staticmethod
    def decode(s):
        i = 0

        while len(s) > 0:
            i *= 58
            i += BASE58_CHARS.index(s[0])
            s = s[1:]

        raw = unhexlify(('00' * 25 + '%x' % i)[-50:])
        return ord(raw[0]), raw[1:21], raw[21:25]

    @staticmethod
    def encode(hash, version):
        if not len(hash) in [ 20, 2*20 ]:
            raise InvalidHashException(hash)

        if len(hash) == 20:
            hash = hexlify(hash)

        raw = '%02x' % version + hash
        raw += sha256(sha256(unhexlify(raw)).digest()).hexdigest()[:8]

        state = int(raw, 16)

        out = []

        while state != 0:
            out.insert(0, BASE58_CHARS[state % 58])
            state /= 58

        return b''.join(out)


