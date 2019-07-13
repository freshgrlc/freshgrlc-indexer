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

    def getinitdata(self, set_dirty=True):
        with self.db.new_session() as session:
            with QueryDataPostProcessor() as pp:
                self.pool = pp.process(session.mempool()).data
        if set_dirty:
            self.dirty = True

    def process_block(self, block):
        # Since the possibility of block orphaning complicates mempool sync a lot
        # it is a lot easier to just requery the database whenever a block comes in.
        lastlen = len(self.pool)
        self.getinitdata(set_dirty=False)
        self.dirty = len(self.pool) != lastlen

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
    def __init__(self, db, poll_interval=2, keepalive_interval=20):
        super(IndexerEventStream, self).__init__()
        self.poll_interval = poll_interval
        self.keepalive_interval = keepalive_interval
        self.db = db
        self.mempool = Mempool(db)
        spawn(self.listener)
        spawn(self.keepalive)

    def broadcast_new_blocks(self, blocks):
        with QueryDataPostProcessor() as pp:
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height'])
            for block in blocks:
                self.publish(Event('newblock', pp.process(block).data, channel='blocks'))

    def broadcast_new_txs(self, txs):
        with QueryDataPostProcessor() as pp:
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height'])
            for tx in reversed(txs):
                self.publish(Event('newtx', pp.process(tx).data, channel='transactions'))

    def broadcast_current_mempool(self):
        self.publish(Event('mempoolupdate', self.mempool.get(), channel='mempool'))

    def listener(self):
        last_height = self.db.chaintip().height
        last_tx_internal_id = self.db.latest_transactions(limit=1)[0].id

        while True:
            sleep(self.poll_interval)

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
            sleep(self.keepalive_interval)
            self.publish(Event('keepalive', None, channel='keepalive'))
