import json
import requests

from binascii import hexlify, unhexlify

from coindaemon import Daemon
from dbhelpers import DatabaseIO
from config import Configuration


class Context(Configuration):
    def __init__(self):
        self.daemon = Daemon(self.DAEMON_URL)
        self.db = DatabaseIO(self.DATABASE_URL, debug=self.DEBUG_SQL)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.db.flush()

    def find_common_ancestor(self):
        chaintip_height = self.daemon.get_current_height()
        indexer_tip = self.db.chaintip()

        if indexer_tip == None:
            return -1, -1, chaintip_height

        ancestor_height = indexer_tip.height
        chain_block_hash = self.daemon.getblockhash(ancestor_height)

        if indexer_tip.hash != unhexlify(chain_block_hash):
            ancestor_height -= 1
            while ancestor_height > 0:
                chain_block_hash = self.daemon.getblockhash(ancestor_height)
                indexer_block = self.db.block(ancestor_height)

                if indexer_block.hash == unhexlify(chain_block_hash):
                    break

                ancestor_height -= 1

        return ancestor_height, indexer_tip.height, chaintip_height

    def sync_blocks(self):
        ancestor_height, indexer_height, chain_height = self.find_common_ancestor()

        if ancestor_height != chain_height:
            if ancestor_height < indexer_height:
                self.db.orphan_blocks(ancestor_height + 1)

            for height in range(ancestor_height + 1, chain_height + 1):
                self.import_blockheight(height)

    def import_blockheight(self, height):
        self.db.import_blockinfo(self.daemon.getblockheight(height), tx_resolver=self.get_transaction_with_metadata)

    def get_transaction_with_metadata(self, txid):
        return self.daemon.load_transaction(txid), None


def run():
    with Context() as c:
        try:
            c.sync_blocks()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    run()

