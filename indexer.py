import httplib
import json
import requests
import socket

from binascii import hexlify, unhexlify
from bitcoinrpc import authproxy
from cachetools import TTLCache
from datetime import datetime
from time import sleep

from coindaemon import Daemon
from dbhelpers import DatabaseIO
from config import Configuration


class LogWatcher(object):
    def __init__(self, path):
        self.path = path
        self.last_size = self.current_size()

    def current_size(self):
        with open(self.path, 'r') as f:
            f.seek(-1, 2)
            return f.tell()

    def has_new_data(self):
        newsize = self.current_size()
        if newsize < self.last_size:    # Logfile rotation
            self.last_size = 0
        return newsize != self.last_size

    def get_new_lines(self):
        with open(self.path, 'r') as f:
            f.seek(self.last_size, 0)
            new_lines = f.read().split('\n')
            self.last_size = f.tell()
        return new_lines

    def poll(self):
        ret = {}
        if self.has_new_data():
            for line in self.get_new_lines():
                parts = line.split(' ')
                if len(parts) > 6 and parts[2] == 'New' and parts[3] in ('tx', 'block') and parts[5] == 'from':
                    if not parts[4] in ret.keys():
                        ret[parts[4]] = { 'relaytime': datetime.strptime(' '.join(parts[0:2]), '%Y-%m-%d %H:%M:%S'), 'relayip': parts[6] }
        return ret

class Context(Configuration):
    def __init__(self):
        self.daemon = Daemon(self.DAEMON_URL)
        self.db = DatabaseIO(self.DATABASE_URL, utxo_cache=self.UTXO_CACHE, debug=self.DEBUG_SQL)
        self.hashcache = TTLCache(ttl=20, maxsize=256)
        self.logwatcher = None

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
        self.update_hash_cash()
        hash = self.daemon.getblockhash(height)
        runtime_info = self.hashcache[hash] if hash in self.hashcache else None
        self.db.import_blockinfo(self.daemon.getblock(hash), runtime_metadata=runtime_info, tx_resolver=self.get_transaction_with_metadata)

    def query_mempool(self):
        pass

    def init_log_watcher(self):
        self.logwatcher = LogWatcher(self.NODE_LOGFILE)

    def update_hash_cash(self):
        if self.logwatcher != None:
            for hash, info in self.logwatcher.poll().items():
                if not hash in self.hashcache:
                    self.hashcache[hash] = info

    def get_transaction_with_metadata(self, txid):
        return self.daemon.load_transaction(txid), None


def run():
    while True:
        try:
            with Context() as c:
                try:
                    print('\nPerforming initial sync...\n')
                    c.sync_blocks()
                    print('\nSwitching to live tracking of mempool and chaintip.\n')
                    c.init_log_watcher()
                    while True:
                        sleep(1)
                        c.query_mempool()
                        c.sync_blocks()

                except KeyboardInterrupt:
                    return
        except (socket.timeout, socket.error, httplib.BadStatusLine, authproxy.JSONRPCException):
            pass
        print('Connection lost. Reconnecting in 10 seconds...')
        sleep(10)

if __name__ == '__main__':
    run()

