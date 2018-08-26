from gevent import spawn, sleep

from postprocessor import QueryDataPostProcessor
from sse import EventStream, Event

class IndexerEventStream(EventStream):
    def __init__(self, db):
        super(IndexerEventStream, self).__init__()
        self.db = db
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

    def listener(self):
        last_height = self.db.chaintip().height
        last_tx_internal_id = self.db.latest_transactions(limit=1)[0].id

        while True:
            sleep(2)

            with self.db.new_session() as session:
                cur_height = session.chaintip().height
                if cur_height > last_height:
                    self.broadcast_new_blocks(session.blocks(last_height + 1, cur_height - last_height))
                    last_height = cur_height

                cur_tx_internal_id = session.latest_transactions(limit=1)[0].id
                if cur_tx_internal_id > last_tx_internal_id:
                    self.broadcast_new_txs(filter(lambda tx: tx.id > last_tx_internal_id, session.latest_transactions(limit=(cur_tx_internal_id - last_tx_internal_id))))
                    last_tx_internal_id = cur_tx_internal_id

    def keepalive(self):
        while True:
            sleep(20)
            self.publish(Event('keepalive', None, channel='keepalive'))

