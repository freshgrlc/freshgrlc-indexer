from binascii import hexlify
from gevent import spawn, sleep

from postprocessor import QueryDataPostProcessor
from sse import EventStream, Event


class Mempool(object):
    def __init__(self, db):
        self.db = db
        self.dirty = False
        self.pool = None
        spawn(self.getinitdata)

    def getinitdata(self):
        with self.db.new_session() as session:
            with QueryDataPostProcessor() as pp:
                self.pool = pp.process(session.mempool()).data
        self.dirty = True

    def process_block(self, block):
        lastlen = len(self.pool)
        self.pool = list(filter(lambda tx: tx['txid'] not in [hexlify(tx.txid) for tx in block.transactions], self.pool))
        if len(self.pool) != lastlen:
            self.dirty = True

    def process_new_tx(self, tx):
        if tx.confirmed:
            return
        with QueryDataPostProcessor() as pp:
            self.pool.append(pp.process(tx).data)
        self.dirty = True

    def get(self):
        self.dirty = False
        return self.pool


class IndexerEventStream(EventStream):
    def __init__(self, db):
        super(IndexerEventStream, self).__init__()
        self.db = db
        self.mempool = Mempool(db)
        spawn(self.listener)
        spawn(self.keepalive)

    def broadcast_new_blocks(self, blocks):
        with QueryDataPostProcessor() as pp:
            for block in blocks:
                self.publish(Event('newblock', pp.process(block).data, channel='blocks'))

    def broadcast_new_txs(self, txs):
        with QueryDataPostProcessor() as pp:
            for tx in reversed(txs):
                self.publish(Event('newtx', pp.process(tx).data, channel='transactions'))

    def broadcast_current_mempool(self):
        self.publish(Event('mempoolupdate', self.mempool.get(), channel='mempool'))

    def listener(self):
        last_height = self.db.chaintip().height
        last_tx_internal_id = self.db.latest_transactions(limit=1)[0].id

        while True:
            sleep(2)

            with self.db.new_session() as session:
                cur_height = session.chaintip().height
                if cur_height > last_height:
                    new_blocks = session.blocks(last_height + 1, cur_height - last_height)
                    self.broadcast_new_blocks(new_blocks)
                    for block in new_blocks:
                        self.mempool.process_block(block)

                    last_height = cur_height

                cur_tx_internal_id = session.latest_transactions(limit=1)[0].id
                if cur_tx_internal_id > last_tx_internal_id:
                    new_txs = filter(lambda tx: tx.id > last_tx_internal_id, session.latest_transactions(limit=(cur_tx_internal_id - last_tx_internal_id)))
                    self.broadcast_new_txs(new_txs)
                    for tx in new_txs:
                        self.mempool.process_new_tx(tx)

                    last_tx_internal_id = cur_tx_internal_id

                if self.mempool.dirty:
                    self.broadcast_current_mempool()

    def keepalive(self):
        while True:
            sleep(20)
            self.publish(Event('keepalive', None, channel='keepalive'))
