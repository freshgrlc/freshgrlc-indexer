import socket

from binascii import unhexlify
from bitcoinrpc import authproxy
from cachetools import TTLCache
from datetime import datetime
from time import sleep
from sys import version_info

from coindaemon import Daemon
from database import DatabaseIO
from config import Configuration

if version_info[0] > 2:
    import http.client as httplib
else:
    import httplib


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
                        relaytime = datetime.strptime(' '.join(parts[0:2]), '%Y-%m-%d %H:%M:%S')
                        relayip = parts[6].rsplit(':', 1)[0].lstrip('[').rstrip(']')
                        ret[parts[4]] = {'relaytime': relaytime, 'relayip': relayip}
        return ret


class Context(Configuration):
    def __init__(self):
        self.daemon = Daemon(self.DAEMON_URL)
        self.db = DatabaseIO(self.DATABASE_URL, utxo_cache=self.UTXO_CACHE, debug=self.DEBUG_SQL)
        self.hashcache = TTLCache(ttl=20, maxsize=256)
        self.logwatcher = None
        self.mempoolcache = TTLCache(ttl=600, maxsize=4096)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.flush()

    def find_common_ancestor(self):
        chaintip_height = self.daemon.get_current_height()
        indexer_tip = self.db.chaintip()

        if indexer_tip is None:
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
        self.update_hash_cache()
        blockhash = self.daemon.getblockhash(height)
        runtime_info = self.hashcache[blockhash] if blockhash in self.hashcache else None
        self.db.import_blockinfo(self.daemon.getblock(blockhash), runtime_metadata=runtime_info, tx_resolver=self.get_transaction_with_metadata)

    def query_mempool(self):
        new_txs = filter(lambda tx: tx not in self.mempoolcache, self.daemon.getrawmempool())
        if len(new_txs) > 0:
            self.update_hash_cache()
            for txid in new_txs:
                if self.db.transaction_internal_id(txid) is None:
                    txinfo = self.get_transaction_with_metadata(txid)
                    self.db.import_transaction(txinfo=txinfo[0], tx_runtime_metadata=txinfo[1])
                self.mempoolcache[txid] = True

    def init_log_watcher(self):
        self.logwatcher = LogWatcher(self.NODE_LOGFILE)

    def update_hash_cache(self):
        if self.logwatcher is not None:
            for objhash, info in self.logwatcher.poll().items():
                if objhash not in self.hashcache:
                    self.hashcache[objhash] = info

    def get_transaction_with_metadata(self, txid):
        return self.daemon.load_transaction(txid), None if txid not in self.hashcache else self.hashcache[txid]


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
