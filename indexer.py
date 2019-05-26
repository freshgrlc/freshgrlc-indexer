import socket

from binascii import unhexlify
from bitcoinrpc import authproxy
from cachetools import TTLCache
from datetime import datetime
from time import sleep
from traceback import print_exc
from sys import version_info, argv

from coindaemon import Daemon
from database import DatabaseIO
from config import Configuration

if version_info[0] > 2:
    import http.client as httplib
else:
    import httplib



class Context(Configuration):
    def __init__(self, timeout=30):
        self.daemon = Daemon(self.DAEMON_URL)
        self.db = DatabaseIO(self.DATABASE_URL, timeout=timeout, utxo_cache=self.UTXO_CACHE, debug=self.DEBUG_SQL)
        self.mempoolcache = TTLCache(ttl=600, maxsize=4096)
        self.last_mutations_txid = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
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

        if ancestor_height == chain_height:
            return False

        if ancestor_height < indexer_height:
            self.db.orphan_blocks(ancestor_height + 1)

        for height in range(ancestor_height + 1, chain_height + 1):
            self.import_blockheight(height)

        return True

    def import_blockheight(self, height):
        blockhash = self.daemon.getblockhash(height)
        self.db.import_blockinfo(self.daemon.getblock(blockhash), tx_resolver=self.get_transaction)

    def query_mempool(self):
        new_txs = list(filter(lambda tx: tx not in self.mempoolcache, self.daemon.getrawmempool()))
        if len(new_txs) == 0:
            return False
        for txid in new_txs:
            if self.db.transaction_internal_id(txid) is None:
                txinfo = self.get_transaction(txid)
                self.db.import_transaction(txinfo=txinfo)
            self.mempoolcache[txid] = True
        return True

    def update_single_balance(self):
        dirty_address = self.db.next_dirty_address()
        if dirty_address is None:
            return False
        self.db.update_address_balance(dirty_address)
        return True

    def update_single_balance_background(self):
        dirty_address = self.db.next_dirty_address(check_for_id=2, random_address=True)
        if dirty_address is None:
            return False
        self.db.update_address_balance_slow(dirty_address)
        return True

    def add_single_tx_mutations(self):
        next_tx = self.db.next_tx_without_mutations_info(last_id=self.last_mutations_txid)
        if next_tx is None:
            return False
        self.db.add_tx_mutations_info(next_tx)
        self.last_mutations_txid = next_tx.id
        return True

    def get_transaction(self, txid):
        return self.daemon.load_transaction(txid)


def indexer(context):
    print('\nPerforming initial sync...\n')
    context.sync_blocks()
    print('\nSwitching to live tracking of mempool and chaintip.\n')
    while True:
        if not (context.query_mempool() or context.sync_blocks() or context.add_single_tx_mutations() or context.update_single_balance()):
            sleep(1)

def background_task(context):
    context.db.reset_slow_address_balance_updates()
    while True:
        if not context.update_single_balance_background():
            sleep(10)

def loop(func, timeout=30):
    while True:
        try:
            with Context(timeout) as c:
                try:
                    func(c)
                except KeyboardInterrupt:
                    return
        except (socket.timeout, socket.error, httplib.BadStatusLine, authproxy.JSONRPCException):
            print('Caught connection exception:')
            print_exc()
        print('Connection lost. Reconnecting in 10 seconds...')
        sleep(10)


if __name__ == '__main__':
    if not '-B' in argv and not '--background-job' in argv:
        loop(indexer)
    else:
        loop(background_task, 300)
