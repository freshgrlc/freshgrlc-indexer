import __main__
import socket

from binascii import hexlify, unhexlify
from bitcoinrpc import authproxy
from cachetools import TTLCache
from datetime import datetime
from time import sleep, time
from traceback import print_exc
from sqlalchemy.orm import aliased
from sys import version_info, argv

from coinsupport import Daemon

from database import DatabaseIO
from models import Address, Block, BlockTransaction, CoinbaseInfo, CoinDaysDestroyed, Mutation, Transaction, TransactionInput, TransactionOutput
from config import Configuration
from logger import log, log_event, log_block_event, log_tx_event
from pidfile import make_pidfile


if version_info[0] > 2:
    import http.client as httplib
else:
    import httplib



class Context(Configuration):
    def __init__(self, db_timeout=30):
        self._daemon = None
        self.db = DatabaseIO(self.DATABASE_URL, timeout=db_timeout, utxo_cache=self.UTXO_CACHE, debug=self.DEBUG_SQL)
        self.mempoolcache = TTLCache(ttl=600, maxsize=4096)
        self.migration_type = 'init'
        self.migration_last_id = None
        self.coindays_destroyed_calc_last_block_id = 1
        self.last_synced_blk = None
        self.last_mempool_check_blk = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.flush()

    def daemon(self):
        if self._daemon is not None:
            try:
                self._daemon.uptime()
            except (socket.timeout, socket.error, httplib.BadStatusLine):
                self._daemon = None
        if self._daemon is None:
            self._daemon = Daemon(self.DAEMON_URL)
        return self._daemon

    def verify_state(self):
        # Looks like we can end up in a state where we have blocks
        # without coinbase info if we exit at the wrong point?
        self.db.remove_blocks_without_coinbase()

        # Verify transactions in on-chain blocks are confirmed
        self.db.verify_confirmed_transactions_state()

        # Verify confirmed transactions are on-chain
        self.db.verify_unconfirmed_transactions_state()

        self.db.session.commit()


    def find_common_ancestor(self):
        daemon = self.daemon()
        chaintip_height = daemon.get_current_height()
        indexer_tip = self.db.chaintip()

        if indexer_tip == None:
            return -1, -1, chaintip_height

        ancestor_height = indexer_tip.height if indexer_tip.height <= chaintip_height else chaintip_height
        chain_block_hash = daemon.getblockhash(ancestor_height)

        if indexer_tip.hash != unhexlify(chain_block_hash):
            ancestor_height -= 1
            while ancestor_height > 0:
                chain_block_hash = daemon.getblockhash(ancestor_height)
                indexer_block = self.db.block(ancestor_height)

                if indexer_block.hash == unhexlify(chain_block_hash):
                    break

                ancestor_height -= 1

        return ancestor_height, indexer_tip.height, chaintip_height

    def sync_blocks(self, initial=False):
        ancestor_height, indexer_height, chain_height = self.find_common_ancestor()

        if initial:
            log('Block heights:')
            log('  Network:     %7d' % chain_height)
            log('  Indexer:     %7d' % indexer_height)
            log('  Last common: %7d' % ancestor_height)
            log('')

        if ancestor_height == chain_height and not initial:
            return False

        if ancestor_height < indexer_height:
            self.db.orphan_blocks(ancestor_height + 1)

        if initial and self.db.blockcount() != ancestor_height:
            log('\nIndexer is missing %d blocks, doing full rescan...\n' % (ancestor_height - self.db.blockcount()))

            for base in range(1, ancestor_height + 1, 1000):
                if self.db.blockcount(range=(base, base + 1000)) != 1000:
                    for height in range(base, base + 1000 if base + 1000 < ancestor_height + 1 else ancestor_height + 1):
                        block = self.db.block(height)
                        if block == None:
                            self.import_blockheight(height)

        newblock = None
        next_commit = time() + 3
        for height in range(ancestor_height + 1, chain_height + 1):
            newblock = self.import_blockheight(height, commit=False)
            self.last_synced_blk = newblock.hash
            if next_commit <= time():
                log_block_event(hexlify(newblock.hash), 'Commit')
                self.db.session.commit()
                newblock = None
                next_commit = time() + 3

        if newblock is not None:
            log_block_event(hexlify(newblock.hash), 'Commit')
            self.db.session.commit()
        return True

    def import_blockheight(self, height, commit=True):
        daemon = self.daemon()
        blockhash = daemon.getblockhash(height)
        blockinfo = daemon.getblock(blockhash)
        last_blockhash = blockinfo['previousblockhash']
        next_blockhash = blockinfo['nextblockhash'] if 'nextblockhash' in blockinfo else None

        if height > 1:
            lastblock = self.db.block(height - 1)
            if unhexlify(last_blockhash) != lastblock.hash:
                raise Exception('Chain error: blocks %d and %d not chaining' % (height - 1, height))

        nextblock = self.db.block(height + 1)
        if nextblock != None:
            if next_blockhash is None or unhexlify(next_blockhash) != nextblock.hash:
                self.db.orphan_blocks(height + 1)

        return self.db.import_blockinfo(daemon.getblock(blockhash), tx_resolver=self.get_transaction, commit=commit)

    def query_mempool(self):
        new_txs = list(filter(lambda tx: tx not in self.mempoolcache, self.daemon().getrawmempool()))
        if len(new_txs) == 0:
            return False
        for txid in new_txs:
            self.db.check_need_import_transaction(txid, tx_resolver=self.get_transaction)
            self.mempoolcache[txid] = True
        return True

    def update_single_balance(self):
        dirty_address = self.db.next_dirty_address()
        if dirty_address is None:
            return False
        self.db.update_address_balance(dirty_address)
        return True

    def update_coindays_destroyed(self, amount_at_once=50):
        self.db.reset_session()

        results = self.db.session.query(
            Block.id,
            Block.timestamp,
            Transaction
        ).join(
            Block.transactionreferences
        ).join(
            BlockTransaction.transaction
        ).join(
            Transaction.coindays_destroyed,
            isouter=True
        ).join(
            Transaction.coinbaseinfo,
            isouter=True
        ).filter(
            Block.height != None,
            CoinDaysDestroyed.transaction_id == None,
            CoinbaseInfo.transaction_id == None,
            Block.id >= self.coindays_destroyed_calc_last_block_id
        ).order_by(
            Block.id.asc()
        ).limit(amount_at_once).all()

        if len(results) == 0:
            try:
                self.coindays_destroyed_calc_last_block_id = self.db.session.query(Block.id).order_by(Block.id.desc()).first()[0] + 1
            except TypeError:
                pass
            return False

        for _, block_timestamp, tx in results:
            tx_timestamp = tx.firstseen if tx.firstseen != None else block_timestamp

            inputs = self.db.session.query(
                TransactionOutput.amount,
                Block.timestamp
            ).join(
                TransactionInput.input
            ).join(
                TransactionOutput.transaction
            ).join(
                Transaction.confirmation
            ).join(
                BlockTransaction.block
            ).filter(
                TransactionInput.transaction_id == tx.id
            ).all()

            coindays_destroyed = sum([
                float(amount) * ((tx_timestamp - orig_timestamp).total_seconds() / 86400 if orig_timestamp < tx_timestamp else 0)
                    for amount, orig_timestamp
                    in inputs
            ])

            log_tx_event(hexlify(tx.txid), 'Coindays', inputs=len(inputs), coins=sum([input[0] for input in inputs]), coindays_destroyed=coindays_destroyed, date=tx_timestamp.strftime('%Y-%m-%d %H:%M:%S'))

            model = CoinDaysDestroyed()
            model.transaction_id = tx.id
            model.coindays = coindays_destroyed
            model.timestamp = tx_timestamp

            self.db.session.add(model)

        log_event('Commit', '%d' % len(results), 'destroyed coin-days entries')
        self.db.session.commit()

        self.coindays_destroyed_calc_last_block_id = max([result[0] for result in results])
        return True

    def check_mempool_for_doublespends(self):
        if self.last_mempool_check_blk == self.last_synced_blk:
            return False

        work_done = 0

        for unconfirmed_coinbase_tx, coinbaseinfo in self.db.mempool_query(result_columns=(Transaction, CoinbaseInfo)).join(Transaction.coinbaseinfo).all():
            height = coinbaseinfo.height
            if height is None or height > self.db.chaintip().height:
                continue
            unconfirmed_coinbase_tx.doublespends_id = self.db.chaintip().coinbaseinfo.transaction_id
            self.db.session.add(unconfirmed_coinbase_tx)
            log_tx_event(hexlify(unconfirmed_coinbase_tx.txid), 'DSpent', coinbase=True, height=height)
            work_done += 1

        if not work_done:
            DoubleSpendTransaction = aliased(Transaction)
            for double_spend_tx, parent_tx_id, parent_txid in self.db.mempool_query(
                result_columns=(Transaction, DoubleSpendTransaction.id, DoubleSpendTransaction.txid)
            ).join(
                Transaction.txinputs
            ).join(
                TransactionInput.input
            ).join(
                DoubleSpendTransaction,
                TransactionOutput.transaction
            ).filter(
                DoubleSpendTransaction.doublespends_id != None
            ).group_by(Transaction.id).all():
                double_spend_tx.doublespends_id = parent_tx_id
                self.db.session.add(double_spend_tx)
                log_tx_event(hexlify(double_spend_tx.txid), 'DSpent', parent=hexlify(parent_txid))
                work_done += 1

        if work_done == 0:
            self.last_mempool_check_blk = self.last_synced_blk
            return False

        log_event('Commit', '%d' % work_done, 'double spent transactions')
        self.db.session.commit()
        return True

    def migrate_old_data(self):
        if self.migration_type == 'init':
            self.migration_type = 'mutations'
            self.migration_last_id = 0

        if self.migration_type == 'mutations':
            if self.migration_update_tx_mutations():
                return True
            self.migration_type = 'address_script'
            self.migration_last_id = 0

        if self.migration_type == 'address_script':
            if self.migration_update_add_address_script():
                return True
            self.migration_type = 'block_totalfee'
            self.migration_last_id = 0

        if self.migration_type == 'block_totalfee':
            if self.migration_update_block_totalfee():
                return True
            self.migration_type = 'coinbase_newcoins'
            self.migration_last_id = 0

        if self.migration_type == 'coinbase_newcoins':
            if self.migration_update_coinbase_newcoins():
                return True
            self.migration_type = None
            self.migration_last_id = 0
        return False

    def migration_update_tx_mutations(self):
        transaction = self.db.session.query(
            Transaction
        ).join(
            Transaction.address_mutations,
            isouter=True
        ).filter(
            Transaction.id > self.migration_last_id,
            Mutation.id == None
        ).first()

        if transaction == None:
            return False

        self.migration_last_id = transaction.id

        self.db.add_tx_mutations_info(transaction)
        self.db.session.flush()
        return True

    def migration_update_add_address_script(self):
        address = self.db.session.query(
            Address
        ).filter(
            Address.id > self.migration_last_id,
            Address.type_id.in_([ 0, 1 ]),
            Address.raw == None
        ).first()

        if address == None:
            return False

        self.migration_last_id = address.id

        log_event('Import', 'scp', address.address)

        daemon = self.daemon()
        script = daemon.validateaddress(address.address)['scriptPubKey']
        script = daemon.decodescript(script)['asm']

        address.raw = script
        self.db.session.add(address)
        self.db.session.flush()
        return True

    def migration_update_block_totalfee(self):
        block = self.db.session.query(Block).filter(Block.totalfee == None).filter(Block.id > self.migration_last_id).first()

        if block == None:
            return False

        self.migration_last_id = block.id

        log_block_event(hexlify(block.hash), 'Migrate')
        block.totalfee = sum([ tx.fee for tx in block.transactions ])
        self.db.session.add(block)
        self.db.session.flush()
        return True

    def migration_update_coinbase_newcoins(self):
        cb = self.db.session.query(CoinbaseInfo).filter(CoinbaseInfo.newcoins == None).filter(CoinbaseInfo.block_id > self.migration_last_id).first()

        if cb == None:
            return False

        self.migration_last_id = cb.block_id

        log_event('Migrate', 'cb', hexlify(cb.transaction.txid))
        cb.newcoins = cb.transaction.totalvalue - cb.block.totalfee
        self.db.session.add(cb)
        self.db.session.flush()
        return True

    def get_transaction(self, txid):
        return self.daemon().load_transaction(txid)


def do_until_timeout(operation, timeout):
    if operation():
        while time() < timeout:
            if not operation():
                break
        return True
    return False


def do_in_loop(operation, before_sleep=None, after_first_run=None, interval=1):
    working = False
    first_run = True
    while True:
        if working is None:
            working = True
        elif working is False:
            working = None

        if operation():
            continue

        if working and before_sleep is not None:
            before_sleep()

        working = False

        if first_run:
            if after_first_run is not None:
                after_first_run()
            first_run = False

        sleep(interval)


def indexer(context):
    def main_loop():
        context.db.reset_session()

        context.query_mempool()
        if context.sync_blocks():
            return True

        if context.check_mempool_for_doublespends():
            return True

        timeout = time() + 3

        if do_until_timeout(context.update_single_balance, timeout):
            return True

        if do_until_timeout(context.update_coindays_destroyed, timeout):
            return True

        # Data migration is done in bulk (with large commits!)
        if do_until_timeout(context.migrate_old_data, timeout):
            context.db.session.commit()
            return True
        return False

    log('\nChecking database state...\n')
    context.verify_state()

    log('\nPerforming initial sync...\n')
    context.sync_blocks(initial=True)

    log('\nSwitching to live tracking of mempool and chaintip.\n')
    do_in_loop(operation=main_loop, before_sleep=lambda: log_event('Synced', 'chn', ''), after_first_run=lambda: make_pidfile(__main__))


def main(func, db_timeout=30):
    with Context(db_timeout) as c:
        try:
            func(c)
        except KeyboardInterrupt:
            return


if __name__ == '__main__':
    main(indexer)
