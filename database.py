from binascii import hexlify, unhexlify
from datetime import datetime
from decimal import Decimal
from cachetools import LFUCache, RRCache
from sqlalchemy import create_engine, tuple_, or_, func as sqlfunc
from sqlalchemy.orm import sessionmaker
from sys import version_info
from time import time

import coininfo
from addrcodecs import decode_any_address
from models import *
from postprocessor import convert_date
from logger import *


INTEGER_TYPES = [int] if version_info[0] > 2 else [int, long]

EPOCH = datetime.fromtimestamp(0)


class Cache(object):
    ALL_IDS = [
        CACHE_IDS.TOTAL_TRANSACTIONS,
        CACHE_IDS.TOTAL_BLOCKS,
        CACHE_IDS.TOTAL_FEES,
        CACHE_IDS.TOTAL_COINS_RELEASED
    ]
    BLOCK_CACHE_IDS = [
        CACHE_IDS.TOTAL_BLOCKS,
        CACHE_IDS.TOTAL_FEES,
        CACHE_IDS.TOTAL_COINS_RELEASED
    ]
    TRANSACTION_CACHE_IDS = [
        CACHE_IDS.TOTAL_TRANSACTIONS
    ]

    def __init__(self, db):
        self.db = db

    def get(self, id):
        return self.db.session.query(CachedValue).filter(CachedValue.id == id).first()

    def set(self, id, value, flush=True, commit=False):
        entry = self.get(id)
        entry.value = value
        entry.valid = True
        self.db.session.add(entry)

        if flush:
            self.db.session.flush()
        if commit:
            self.db.session.commit()

    def invalidate(self, commit=False):
        log_event('Drop', 'tx', 'cache')
        log_event('Drop', 'blk', 'cache')

        self.db.session.flush()
        self.db.session.execute('UPDATE `%s` SET `valid` = \'0\' WHERE \'1\' = \'1\';' % (CachedValue.__tablename__), {})
        if commit:
            self.db.session.commit()

    def is_valid(self, ids):
        return len(self.db.session.query(CachedValue).filter(CachedValue.id.in_(ids), CachedValue.valid == False).all()) == 0


    @property
    def total_transactions(self):
        return int(self.get(CACHE_IDS.TOTAL_TRANSACTIONS).value)

    @total_transactions.setter
    def total_transactions(self, value):
        self.set(CACHE_IDS.TOTAL_TRANSACTIONS, value)


    @property
    def total_blocks(self):
        return int(self.get(CACHE_IDS.TOTAL_BLOCKS).value)

    @total_blocks.setter
    def total_blocks(self, value):
        self.set(CACHE_IDS.TOTAL_BLOCKS, value)


    @property
    def total_fees(self):
        return self.get(CACHE_IDS.TOTAL_FEES).value

    @total_fees.setter
    def total_fees(self, value):
        self.set(CACHE_IDS.TOTAL_FEES, value)


    @property
    def total_coins_released(self):
        return self.get(CACHE_IDS.TOTAL_COINS_RELEASED).value

    @total_coins_released.setter
    def total_coins_released(self, value):
        self.set(CACHE_IDS.TOTAL_COINS_RELEASED, value)



class DatabaseSession(object):
    def __init__(self, session, address_cache, txid_cache, utxo_cache=None):
        self.session = session
        self._chaintip = None

        self.address_cache = address_cache
        self.txid_cache = txid_cache
        self.utxo_cache = utxo_cache

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    def flush(self):
        self.session.flush()
        self.session.close()

    def reset_session(self):
        self.session.rollback()

    @property
    def cache(self):
        return Cache(self)

    def decode_address_for(self, txout_type):
        txout = self.session.query(
            TransactionOutput
        ).filter(
            TransactionOutput.type_id == TXOUT_TYPES.internal_id(txout_type)
        ).first()

        if txout != None:
            try:
                return tuple(list(decode_any_address(txout.address.address))[:2])
            except ValueError:
                pass
        return None, None

    def detect_bech32_address_prefix(self):
        address = self.session.query(
            Address
        ).filter(
            Address.type_id == ADDRESS_TYPES.internal_id(ADDRESS_TYPES.BECH32)
        ).first()

        if address != None:
            return address.address.split('1')[0]

        _, p2pkh_address_version = self.decode_address_for(TXOUT_TYPES.P2PKH)
        _, p2sh_address_version = self.decode_address_for(TXOUT_TYPES.P2SH)

        coin_info = coininfo.by_address_versions(p2pkh_address_version, p2sh_address_version)
        if coin_info is None:
            return None

        return coin_info['bech32_prefix']

    def detect_address_translations(self):
        p2pkh_address_type, p2pkh_address_version = self.decode_address_for(TXOUT_TYPES.P2PKH)
        p2sh_address_type, p2sh_address_version = self.decode_address_for(TXOUT_TYPES.P2SH)
        p2wpkh_address_type, p2wpkh_address_version = self.decode_address_for(TXOUT_TYPES.P2WPKH)

        translations = {}

        if p2wpkh_address_type is not None and p2pkh_address_type == p2wpkh_address_type and p2pkh_address_version == p2wpkh_address_version:
            translations[(ADDRESS_TYPES.BECH32, 0)] = (p2pkh_address_type, p2pkh_address_version)

            coin_info = coininfo.by_address_versions(p2pkh_address_version, p2sh_address_version)
            if coin_info is not None and coin_info['segwit_info'] is not None and coin_info['segwit_info']['addresstype'] == ADDRESS_TYPES.BASE58:
                translations[(ADDRESS_TYPES.BASE58, coin_info['segwit_info']['address_version'])] = (p2pkh_address_type, p2pkh_address_version)

        return translations

    def chaintip(self):
        if self._chaintip is None:
            self._chaintip = self.session.query(Block).filter(Block.height != None).order_by(Block.height.desc()).first()
        return self._chaintip

    def block(self, blockid):
        if type(blockid) in INTEGER_TYPES:
            pass
        elif not len(blockid) in [32, 2*32]:
            blockid = int(blockid)

        if type(blockid) in INTEGER_TYPES:
            return self.session.query(Block).filter(Block.height == blockid).first()

        return self.session.query(Block).filter(Block.hash == (unhexlify(blockid) if len(blockid) == 64 else blockid)).first()

    def blocks(self, start_height, limit, interval=None):
        if interval is None:
            return self.session.query(Block).filter(Block.height >= start_height).order_by(Block.height).limit(limit).all()
        return self.session.query(Block).filter(Block.height >= start_height, Block.height % interval == start_height % interval).order_by(Block.height).limit(limit).all()

    def blockcount(self, range=None):
        query = self.session.query(sqlfunc.count(Block.id))

        if range is None:
            query = query.filter(Block.height != None)
        else:
            query = query.filter(Block.height >= range[0], Block.height < range[1])
        return int(query.all()[0][0])

    def address_info(self, address, mutations_limit=100):
        address = self.session.query(Address).filter(Address.address == address).first()
        if address == None:
            return None
        return {
            'address': address.address,
            'balance': float(address.balance),
            'pending': float(address.pending)
        }

    def address_balance(self, address):
        address = self.session.query(Address).filter(Address.address == address).first()
        if address != None:
            return float(address.balance)

    def address_pending_balance(self, address):
        return float(sum([ m['change'] for m in self.address_mutations(address, confirmed=False, limit=1000) ]))

    def address_mutations(self, address, confirmed=None, start=0, limit=100):
        if limit == 0:
            return []
        query = self.session.query(Transaction, Mutation).join(Mutation).join(Address).filter(Address.address == address)
        if confirmed is not None:
            if confirmed:
                query = query.filter(Transaction.confirmation_id != None)
            else:
                query = query.join(CoinbaseInfo, isouter=True).filter(Transaction.confirmation_id == None).filter(CoinbaseInfo.transaction_id == None)
        results = query.order_by(Transaction.id.desc()).offset(start).limit(limit).all()
        return [{'time': convert_date(result[0].time), 'txid': hexlify(result[0].txid), 'change': float(result[1].amount), 'confirmed': result[0].confirmed} for result in results]

    def query_transactions(self, include_confirmation_info=False, confirmed_only=False):
        if include_confirmation_info:
            query = self.session.query(
                Transaction,
                BlockTransaction,
                Block
            ).join(
                Transaction.confirmation,
                isouter=True
            ).join(
                Block,
                isouter=True
            )
        else:
            query = self.session.query(Transaction)

        if confirmed_only:
            query = query.filter(Transaction.confirmation_id != None)

        return query

    def transaction(self, txid, include_confirmation_info=False):
        if len(txid) == 64:
            txid = unhexlify(txid)
        result = self.query_transactions(include_confirmation_info=include_confirmation_info).filter(Transaction.txid == txid).first()
        return result if not include_confirmation_info else result[0] if result != None else None

    def transaction_internal_id(self, txid):
        _txid = unhexlify(txid)
        if _txid in self.txid_cache:
            return self.txid_cache[_txid]
        tx = self.transaction(txid)
        return tx.id if tx is not None else None

    def remove_blocks_without_coinbase(self):
        corrupt_blocks = self.session.query(
            Block
        ).join(
            CoinbaseInfo,
            isouter=True
        ).filter(
            Block.id != 0,  # Genesis doesn't have coinbase info
            CoinbaseInfo.block_id == None
        ).all()

        for block in corrupt_blocks:
            log_block_event(hexlify(block.hash), 'Clear', height=block.height)
            self.session.delete(block)

        self.session.flush()

    def verify_confirmed_transactions_state(self):
        for (block_id, blocktransaction_id, transaction) in self.session.query(
                    Block.id,
                    BlockTransaction.id,
                    Transaction
                ).join(
                    Block.transactionreferences
                ).join(
                    BlockTransaction.transaction
                ).filter(
                    Block.height != None,
                    Transaction.confirmation_id == None
                ).all():
            self.confirm_transaction(hexlify(transaction.txid), block_id, tx_resolver=None)

    def verify_unconfirmed_transactions_state(self):
        for transaction in self.session.query(
                    Transaction
                ).join(
                    Transaction.confirmation
                ).join(
                    BlockTransaction.block,
                    isouter=True
                ).filter(
                    Block.height == None
                ).all():
            self.unconfirm_transaction(transaction)

    def latest_transactions(self, confirmed_only=False, limit=100):
        return self.query_transactions(include_confirmation_info=False, confirmed_only=confirmed_only).order_by(Transaction.id.desc()).limit(limit).all()

    def pool_stats(self, since):
        results = self.session.query(
            Pool.name,
            sqlfunc.count(Block.id).label('blocks'),
            sqlfunc.max(Block.height).label('lastblock'),
            Pool.website,
            Pool.graphcolor
        ).join(Block).filter(Block.timestamp >= since).group_by(Pool.name).all()
        return [dict(zip(('name', 'amountmined', 'latestblock', 'website', 'graphcolor'), stats)) for stats in results]

    def block_stats(self, since=None, use_cache=True):
        if use_cache and (since is None or since == EPOCH):
            return {
                'blocks': self.cache.total_blocks,
                'totalfees': self.cache.total_fees,
                'coinsreleased': self.cache.total_coins_released
            }

        query = self.session.query(
            sqlfunc.count(Block.id),
            sqlfunc.sum(Block.totalfee),
            sqlfunc.sum(CoinbaseInfo.newcoins)
        ).join(CoinbaseInfo)

        if since is not None:
            query = query.filter(Block.timestamp >= since)

        return dict(zip(('blocks', 'totalfees', 'coinsreleased'), query.filter(Block.height != None).all()[0]))

    def transaction_stats(self, since=None):
        query = self.session.query(
            sqlfunc.count(Block.id),
            sqlfunc.sum(Transaction.totalvalue)
        ).join(
            BlockTransaction,
            Block.id == BlockTransaction.block_id
        ).join(
            Transaction,
            Transaction.id == BlockTransaction.transaction_id
        )

        if since is not None:
            query = query.filter(Block.timestamp >= since)

        return dict(zip(('transactions', 'transactedvalue'), query.filter(
            Block.height != None,
            Transaction.coinbaseinfo == None
        ).all()[0]))

    def total_transactions(self, use_cache=True):
        if use_cache:
            return self.cache.total_transactions
        return self.transaction_stats()['transactions']

    def total_transactions_since(self, since=None):
        if since is None or since == EPOCH:
            return self.total_transactions()
        return self.transaction_stats(since=since)['transactions']

    def network_stats(self, since, ignore=[]):
        if (since is None or since == EPOCH or 'coinsreleased' in ignore) and 'blocks' in ignore and 'totalfees' in ignore:
            network_stats = {}
            if 'coinsreleased' not in ignore:
                network_stats['coinsreleased'] = self.total_coins_released()
        else:
            network_stats = self.block_stats(since=since)

        if 'transactions' not in ignore or 'transactedvalue' not in ignore:
            network_stats.update(self.transaction_stats(since=since))

        return network_stats

    def total_coins_released(self, use_cache=True):
        if use_cache:
            return self.cache.total_coins_released

        return self.session.query(
            sqlfunc.count(Block.id),
            sqlfunc.sum(CoinbaseInfo.newcoins)
        ).join(
            CoinbaseInfo
        ).filter(Block.height != None).all()[0][1]

    def total_coins_in_addresses(self):
        return self.session.query(sqlfunc.sum(Address.balance)).first()[0]

    def total_coins_info(self):
        return { 'total': { 'released': self.total_coins_released(), 'current': self.total_coins_in_addresses() }}

    def richlist(self, limit, start=0):
        return [ { 'address': v[0], 'balance': v[1] } for v in self.session.query(Address.address, Address.balance).order_by(Address.balance.desc()).limit(limit).offset(start).all() ]

    def mempool(self):
        return self.session.query(Transaction).filter(Transaction.confirmation_id == None).filter(Transaction.coinbaseinfo == None).order_by(Transaction.id.desc()).all()

    def import_blockinfo(self, blockinfo, tx_resolver=None, commit=True):
        # Genesis block workaround
        if blockinfo['height'] == 0:
            blockinfo['tx'] = []

        log_block_event(blockinfo['hash'], 'Adding', via=(blockinfo['relayedby'] if 'relayedby' in blockinfo else None))

        coinbase_signatures = {}
        for txid in blockinfo['tx']:
            self.check_need_import_transaction(txid, tx_resolver=tx_resolver, coinbase_signatures=coinbase_signatures, commit=False)

        blockhash = unhexlify(blockinfo['hash'])
        block = self.block(blockhash)

        if block != None:
            log_block_event(hexlify(block.hash), 'Update', height=block.height)

            block.height = int(blockinfo['height'])
            self.session.add(block)

            self.cache.invalidate()     # Since we skip confirming txs, we need to do a full recalc

            if commit:
                self.session.commit()
            else:
                self.session.flush()

            self._chaintip = None
            return

        if not self.cache.is_valid(ids=Cache.ALL_IDS):
            self.session.flush()
            self.session.commit()

            cache = self.cache

            if not self.cache.is_valid(ids=Cache.BLOCK_CACHE_IDS):
                log_event('Recalc', 'blk', 'cache')
                block_stats = self.block_stats(use_cache=False)
                cache.total_blocks = block_stats['blocks']
                cache.total_fees = block_stats['totalfees']
                cache.total_coins_released = block_stats['coinsreleased']
                log_event('Updated', 'blk', 'cache')
                self.session.commit()

            if not self.cache.is_valid(ids=Cache.TRANSACTION_CACHE_IDS):
                log_event('Recalc', 'tx', 'cache')
                transaction_stats = self.transaction_stats()
                cache.total_transactions = transaction_stats['transactions']
                log_event('Updated', 'tx', 'cache')
                self.session.commit()


        block = Block()

        block.hash = blockhash
        block.height = int(blockinfo['height'])
        block.size = blockinfo['size']
        block.totalfee = 0.0
        block.timestamp = datetime.utcfromtimestamp(blockinfo['time'])
        block.difficulty = blockinfo['difficulty']
        block.firstseen = datetime.utcfromtimestamp(blockinfo['relayedat']) if 'relayedat' in blockinfo and blockinfo['relayedat'] is not None else None
        block.relayedby = blockinfo['relayedby'] if 'relayedby' in blockinfo else None
        block.miner_id = None

        self.session.add(block)
        self.session.flush()

        for tx in blockinfo['tx']:
            self.confirm_transaction(tx, block.id)

        block.totalfee = sum([ self.transaction(tx).fee for tx in blockinfo['tx'] ])
        self.session.add(block)

        if len(coinbase_signatures) > 0:
            log_event('Adding', 'cb', coinbase_signatures.keys()[0])
            self.add_coinbase_data(block, coinbase_signatures.keys()[0], coinbase_signatures.values()[0][0], coinbase_signatures.values()[0][1])

            if block.relayedby != None:
                tx = self.transaction(coinbase_signatures.keys()[0])
                tx.firstseen = block.firstseen
                tx.relayedby = block.relayedby
                self.session.add(tx)
        else:
            raise Exception('No coinbase!')

        self.cache.total_blocks = self.cache.total_blocks + 1
        self.cache.total_fees = self.cache.total_fees + block.totalfee
        log_event('Updated', 'blk', 'cache')

        self.cache.total_transactions = self.cache.total_transactions + len(blockinfo['tx']) - len(coinbase_signatures)
        log_event('Updated', 'tx', 'cache')

        if commit:
            log_block_event(hexlify(block.hash), 'Commit')
            self.session.commit()
        else:
            self.session.flush()

        self._chaintip = None
        log_block_event(hexlify(block.hash), 'Added', height=block.height, time=(block.firstseen or block.timestamp))
        return block

    def orphan_blocks(self, first_height):
        chaintip = self.chaintip()
        for height in range(chaintip.height, first_height - 1, -1):
            self.orphan_block(height)

        self.cache.invalidate(commit=True)

    def orphan_block(self, height):
        block = self.block(height)

        if block != None:
            block.height = None
            for txref in self.session.query(BlockTransaction).filter(BlockTransaction.block_id == block.id).all():
                self.unconfirm_transaction(txref.transaction)
            self.session.add(block)
            self.session.commit()

    def unconfirm_transaction(self, transaction):
        log_tx_event(hexlify(transaction.txid), 'Unconf')
        transaction.confirmation = None
        for tx_output in transaction.txoutputs:
            tx_output.address.balance_dirty = 1
        for tx_input in transaction.txinputs:
            tx_input.input.address.balance_dirty = 1
            tx_input.input.spentby_id = None
        self.session.add(transaction)

    def check_need_import_transaction(self, txid, tx_resolver, coinbase_signatures=None, commit=True):
        tx_id = self.transaction_internal_id(txid)

        if tx_id == None or coinbase_signatures is not None:
            txinfo = tx_resolver(txid)

            coinbase_inputs = list(filter(lambda txin: 'coinbase' in txin, txinfo['vin']))
            regular_inputs = list(filter(lambda txin: 'coinbase' not in txin, txinfo['vin']))

            is_coinbase_tx = len(coinbase_inputs) > 0

            if coinbase_signatures is not None and is_coinbase_tx:
                coinbase_regular_outputs = filter(
                    lambda txo: txo['value'] > 0.0 and 'addresses' in txo['scriptPubKey'] and len(txo['scriptPubKey']['addresses']) == 1,
                    txinfo['vout']
                )
                coinbase_signatures[txinfo['txid']] = (coinbase_inputs[0]['coinbase'], [
                    (txo['n'], txo['scriptPubKey']['addresses'][0], txo['value']) for txo in coinbase_regular_outputs
                ])

        if tx_id != None:
            return tx_id
        return self.import_transaction(txid, txinfo, regular_inputs, coinbase_inputs, commit=commit).id

    def import_transaction(self, txid, txinfo, regular_inputs, coinbase_inputs, commit=True):
        if len(regular_inputs) > 0:
            log_tx_event(txid, 'Adding', inputs=len(regular_inputs), outputs=len(txinfo['vout']), via=txinfo['relayedby'] if 'relayedby' in txinfo else 'unknown')
        else:
            log_tx_event(txid, 'Adding', coinbase=True, outputs=len(txinfo['vout']))

        tx = Transaction()

        tx.txid = unhexlify(txid)
        tx.size = txinfo['size']
        tx.fee = -1.0
        tx.totalvalue = -1.0
        tx.firstseen = datetime.utcfromtimestamp(txinfo['relayedat']) if 'relayedat' in txinfo and txinfo['relayedat'] is not None else None
        tx.relayedby = txinfo['relayedby'] if 'relayedby' in txinfo else None
        tx.confirmation_id = None

        self.session.add(tx)
        self.session.flush()

        self.txid_cache[tx.txid] = tx.id

        total_in = Decimal(0.0)

        utxo_cache_hits = 0
        txid_cache_hits = 0

        if len(regular_inputs) > 0:
            for inp in regular_inputs:
                inp['_txid'] = unhexlify(inp['txid'])
                inp['_txo'] = inp['txid'] + '_' + str(inp['vout'])

            utxo_cache_map, non_cached_inputs = self.lookup_input_utxos_from_utxo_cache(regular_inputs)
            txid_cache_map, non_cached_inputs = self.lookup_input_utxos_using_txid_cache(non_cached_inputs)
            txo_map = self.lookup_input_utxos_slow(non_cached_inputs)

            utxo_cache_hits = len(utxo_cache_map)
            txid_cache_hits = len(txid_cache_map)

            txo_map.update(utxo_cache_map)
            txo_map.update(txid_cache_map)

            total_in = self.import_tx_inputs(regular_inputs, tx.id, txo_map)

        address_map = dict({outp['n']: self.get_or_create_output_address(outp['scriptPubKey'], flushdb=False) for outp in txinfo['vout']})
        self.session.flush()

        utxos, total_out = self.import_tx_outputs(txinfo['vout'], tx.id, address_map)
        tx.totalvalue, tx.fee = self.calculate_tx_totals(total_in, total_out, coinbase=(len(coinbase_inputs) > 0))

        self.session.bulk_save_objects(utxos, return_defaults=(self.utxo_cache is not None))
        self.session.flush()

        self.add_tx_mutations_info(tx)

        if commit:
            log_tx_event(hexlify(tx.txid), 'Commit')
            self.session.commit()

        if self.utxo_cache is not None:
            self.update_utxo_cache(txinfo['txid'], tx.id, utxos)
            log_tx_event(txinfo['txid'], 'Added',
                utxo_cache=self.utxo_cache.currsize,
                hit='%d/%d' % (utxo_cache_hits, len(regular_inputs)),
                txid_cache=self.txid_cache.currsize,
                address_cache=self.address_cache.currsize
            )
        else:
            log_tx_event(txinfo['txid'], 'Added',
                txid_cache=self.txid_cache.currsize,
                hit='%d/%d' % (utxo_cache_hits, len(regular_inputs)),
                address_cache=self.address_cache.currsize
            )
        return tx

    def import_tx_inputs(self, inputs, internal_tx_id, utxo_info):
        inserts = []
        total_value = Decimal(0.0)

        for index, inp in enumerate(inputs):
            utxo_id, utxo_value = utxo_info[inp['_txo']]

            txin = TransactionInput()
            txin.input_id = utxo_id
            txin.transaction_id = internal_tx_id
            txin.index = index
            inserts.append(txin)

            total_value += utxo_value

        self.session.bulk_save_objects(inserts)
        return total_value

    def import_tx_outputs(self, outputs, internal_tx_id, address_id_mappings):
        inserts = []
        total_value = Decimal(0.0)

        for outp in outputs:
            utxo = TransactionOutput()
            utxo.transaction_id = internal_tx_id
            utxo.index = outp['n']
            utxo.type = TXOUT_TYPES.from_rpcapi_type(outp['scriptPubKey']['type'])
            utxo.amount = outp['value']

            utxo.address_id = address_id_mappings[outp['n']].id
            inserts.append(utxo)

            total_value += utxo.amount

        return inserts, total_value

    def calculate_tx_totals(self, total_in, total_out, coinbase=False):
        if coinbase:
            return total_out, Decimal(0.0)
        return total_in, total_in - total_out

    def update_utxo_cache(self, txid, internal_tx_id, utxos):
        if self.utxo_cache is None:
            return
        for utxo in utxos:
            if utxo.type != TXOUT_TYPES.RAW:
                self.utxo_cache[txid + '_' + str(utxo.index)] = (internal_tx_id, utxo.id, utxo.amount)

    def lookup_input_utxos_from_utxo_cache(self, inputs):
        if self.utxo_cache is None:
            return {}, inputs

        cache_misses = []
        resolved_utxos = {}

        for inp in inputs:
            key = inp['_txo']
            if key in self.utxo_cache:
                tx_internal_id, utxo_internal_id, utxo_value = self.utxo_cache[key]
                resolved_utxos[key] = (utxo_internal_id, utxo_value)
                del self.utxo_cache[key]
            else:
                cache_misses.append(inp)
        return resolved_utxos, cache_misses

    def lookup_input_utxos_using_txid_cache(self, inputs):
        queryfilter = tuple([
            tuple_(TransactionOutput.transaction_id, TransactionOutput.index) == (self.txid_cache[txo[0]], txo[1])
            for txo in
            filter(lambda txo: txo[0] in self.txid_cache, [
                (inp['_txid'], inp['vout'])
                for inp in
                inputs
            ])
        ])
        ctx_txo_map = {
            (str(txo.transaction_id) + '_' + str(txo.index)): (txo.id, txo.amount)
            for txo in
            self.session.query(TransactionOutput).filter(or_(*queryfilter)).all()
        } if queryfilter != () else {}

        cache_misses = []
        resolved_utxos = {}

        for inp in inputs:
            if inp['_txid'] in self.txid_cache:
                resolved_utxos[inp['_txo']] = ctx_txo_map[str(self.txid_cache[inp['_txid']]) + '_' + str(inp['vout'])]
            else:
                cache_misses.append(inp)
        return resolved_utxos, cache_misses

    def lookup_input_utxos_slow(self, inputs):
        if len(inputs) == 0:
            return {}

        queryfilter = tuple([
            tuple_(Transaction.txid, TransactionOutput.index) == (inp['_txid'], inp['vout'])
            for inp in inputs
        ])
        results = self.session.query(
            TransactionOutput,
            Transaction
        ).join(
            Transaction
        ).filter(or_(*queryfilter)).all()

        return {
            (hexlify(tx.txid) + '_' + str(txo.index)): (txo.id, txo.amount)
            for (txo, tx) in results
        }

    def confirm_transaction(self, txid, internal_block_id, tx_resolver=None):
        log_tx_event(txid, 'Confirm')

        tx_id = self.check_need_import_transaction(txid, tx_resolver=tx_resolver)

        blockref = self.session.query(BlockTransaction).filter(BlockTransaction.block_id == internal_block_id).filter(BlockTransaction.transaction_id == tx_id).first()
        if blockref == None:
            blockref = BlockTransaction()
            blockref.block_id = internal_block_id
            blockref.transaction_id = tx_id
            self.session.add(blockref)
            self.session.flush()

        # tx.confirmation_id = blockref.id
        #
        # for input in tx.txinputs:
        #    input.input.spentby_id = input.id
        self.session.execute('UPDATE `transaction` SET `confirmation` = :blockref WHERE `id` = :tx_id;', {'blockref': blockref.id, 'tx_id': tx_id})
        self.session.execute('UPDATE `txout` LEFT JOIN `txin` ON `txout`.`id` = `txin`.`input` SET `spentby` = `txin`.`id` WHERE `txin`.`transaction` = :tx_id;', {
            'tx_id': tx_id
        })

        self.session.execute('UPDATE `address` JOIN `txout` ON `txout`.`address` = `address`.`id` SET `address`.`balance_dirty` = \'1\' WHERE `txout`.`transaction` = :tx_id;', {
            'tx_id': tx_id
        })
        self.session.execute('UPDATE `address` JOIN `txout` ON `txout`.`address` = `address`.`id` JOIN `txin` ON `txin`.`input` = `txout`.`id` SET `address`.`balance_dirty` = \'1\' WHERE `txin`.`transaction` = :tx_id;', {
            'tx_id': tx_id
        })

    def add_coinbase_data(self, block, txid, signature, outputs):
        coinbaseinfo = CoinbaseInfo()
        coinbaseinfo.block_id = block.id
        coinbaseinfo.transaction_id = self.transaction_internal_id(txid)
        coinbaseinfo.raw = unhexlify(signature)
        coinbaseinfo.signature = None

        totalout = sum([o[2] for o in outputs])
        coinbaseinfo.newcoins = totalout - block.totalfee

        best_output = list(filter(lambda o: o[2] > (totalout * 95 / 100), outputs))
        best_output = best_output[0] if len(best_output) > 0 else None
        coinbaseinfo.mainoutput_id = self.session.query(
            TransactionOutput
        ).filter(
            TransactionOutput.transaction_id == coinbaseinfo.transaction_id,
            TransactionOutput.index == best_output[0]
        ).first().id if best_output is not None else None

        solo = len(coinbaseinfo.raw) <= 8

        if not solo:
            if coinbaseinfo.raw[-1] == b'/' and b'/' in coinbaseinfo.raw[:-1]:
                try:
                    coinbaseinfo.signature = coinbaseinfo.raw.split(b'/')[-2].decode('utf-8').join(2 * ['/'])
                except (IndexError, UnicodeDecodeError):
                    pass

        self.session.add(coinbaseinfo)
        self.session.flush()

        self.cache.total_coins_released = self.cache.total_coins_released + coinbaseinfo.newcoins

        self.find_and_set_miner(block, coinbaseinfo, solo)

    def find_and_set_miner(self, block, coinbaseinfo, solo):
        if not solo and coinbaseinfo.signature is not None:
            pool_cbsig = self.session.query(PoolCoinbaseSignature).filter(PoolCoinbaseSignature.signature == coinbaseinfo.signature).first()
            if pool_cbsig != None:
                block.miner_id = pool_cbsig.pool_id
                return

        if coinbaseinfo.mainoutput != None and coinbaseinfo.mainoutput.address_id != None:
            pool_addr = self.session.query(PoolAddress).filter(PoolAddress.address_id == coinbaseinfo.mainoutput.address_id).first()
            if pool_addr != None:
                block.miner_id = pool_addr.pool_id
                return

            new_pool = Pool()
            new_pool.group_id = SOLO_POOL_GROUP_ID if solo else None
            new_pool.solo = 1 if solo else 0
            new_pool.name = coinbaseinfo.mainoutput.address.address + ' ' + ('(Solo miner)' if solo else '(Unknown Pool)')

            self.session.add(new_pool)
            self.session.flush()

            pool_addr = PoolAddress()
            pool_addr.address_id = coinbaseinfo.mainoutput.address_id
            pool_addr.pool_id = new_pool.id

            self.session.add(pool_addr)
            block.miner_id = new_pool.id
            return

    def get_or_create_output_address_id(self, txout_address_info):
        return self.get_or_create_output_address(txout_address_info).id

    def get_or_create_output_address(self, txout_address_info, flushdb=True):
        class CachedAddress(object):
            def __init__(self, source):
                self.id = source.id
                self.type = source.type
                self.address = source.address
                self.raw = source.raw

        raw = txout_address_info['asm']

        if 'addresses' in txout_address_info and len(txout_address_info['addresses']) == 1:
            address = txout_address_info['addresses'][0]
            addr_type = ADDRESS_TYPES.BASE58

            if len(address) > 34:
                addr_type = ADDRESS_TYPES.BECH32

            if self.address_cache is not None and address in self.address_cache:
                db_address = self.address_cache[address]
            else:
                db_address = self.session.query(Address).filter(Address.address == address).first()
                if db_address != None:
                    self.address_cache[address] = CachedAddress(db_address)
        else:
            db_address = None
            address = None
            if raw[0:10] == 'OP_RETURN ' and len(raw.split(' ')) == 2:
                raw = raw.split(' ')[1]
                addr_type = ADDRESS_TYPES.DATA
            else:
                addr_type = ADDRESS_TYPES.RAW

        if db_address == None:
            db_address = Address()
            db_address.address = address
            db_address.type = addr_type
            db_address.raw = raw

            self.session.add(db_address)

            if flushdb:
                self.session.flush()

        return db_address

    def add_tx_mutations_info(self, tx, commit=False):
        log_event('Import', 'mts', hexlify(tx.txid))
        self.session.execute('''
            INSERT INTO `mutation` (`transaction`, `address`, `amount`)
                SELECT :tx_id, `address`, SUM(`amount`) FROM (
                    SELECT `txout`.`address`, `txout`.`amount` FROM `transaction`
                        JOIN `txout` ON `transaction`.`id` = `txout`.`transaction`
                    WHERE `transaction`.`id` = :tx_id
                UNION ALL
                    SELECT `txout`.`address`, '0' - `txout`.`amount` FROM `transaction`
                        JOIN `txin` ON `transaction`.`id` = `txin`.`transaction`
                        JOIN `txout` ON `txin`.`input` = `txout`.`id`
                    WHERE `transaction`.`id` = :tx_id
                ) temp
                    GROUP BY address;
            ''', {
                'tx_id': tx.id
        })
        if commit:
            self.session.commit()

    def next_dirty_address(self, check_for_id=1, random_address=False):
        return self.session.query(Address).filter(Address.balance_dirty == check_for_id).order_by(Address.id if not random_address else sqlfunc.rand()).first()

    def get_address_balance(self, address):
        return self.session.execute('''
            SELECT COALESCE(SUM(`txout`.`amount`), 0.0) FROM `txout`
                JOIN `transaction` ON `txout`.`transaction` = `transaction`.`id`
            WHERE `txout`.`address` = :address_id
                AND `txout`.`spentby` IS NULL
                AND `transaction`.`confirmation` IS NOT NULL;
        ''', {
            'address_id': address.id
        }).first()[0]

    def update_address_balance(self, address):
        address_s = address.address if address.address is not None else ' < RAW >'
        log_balance_event(address_s, 'Update')
        start_time = time()

        utxos = self.session.query(TransactionOutput).filter(TransactionOutput.address_id == address.id, TransactionOutput.spentby_id == None).count()
        skip = utxos > 5000

        if not skip:
            self.session.execute("""
                UPDATE `address` SET `balance_dirty` = '0', `balance` = (
                    SELECT COALESCE(SUM(`txout`.`amount`), 0.0)
                        FROM `txout`
                            JOIN `transaction` ON `txout`.`transaction` = `transaction`.`id`
                        WHERE `txout`.`address` = :address_id
                            AND `txout`.`spentby` IS NULL
                            AND `transaction`.`confirmation` IS NOT NULL
                    UNION
                    SELECT \'0.0\'
                        LIMIT 1
                ) WHERE `address`.`id` = :address_id;
            """, {
                'address_id': address.id
            })
        else:
            self.session.execute('UPDATE `address` SET `balance_dirty` = \'2\' WHERE `address`.`id` = :address_id;', {
                'address_id': address.id
            })

        self.session.commit()

        if not skip:
            log_balance_event(address_s, 'Updated', time='%3d msec' % int((time() - start_time) * 1000), utxos=utxos)
        else:
            log_balance_event(address_s, 'Skipped', utxos=utxos)

    def update_address_balance_slow(self, address):
        address_s = address.address if address.address is not None else ' < RAW >'
        log_balance_event(address_s, 'Update')
        start_time = time()

        address.balance_dirty = 3
        self.session.add(address)
        self.session.commit()

        balance = self.get_address_balance(address)
        self.session.expire(address)
        self.session.rollback()

        if address.balance_dirty == 3:
            address.balance_dirty = 0
            address.balance = balance
            self.session.add(address)
            self.session.commit()
            log_balance_event(address_s, 'Updated', balance=str(balance), time='%d secs' % (time() - start_time))
        else:
            log_balance_event(address_s, 'Abort')


    def reset_slow_address_balance_updates(self):
        self.session.execute('UPDATE `address` SET `balance_dirty` = \'2\' WHERE `balance_dirty` = \'3\';');
        self.session.commit()


class DatabaseIO(DatabaseSession):
    def __init__(self, url, timeout=30, utxo_cache=False, debug=False):
        self.sessionmaker = sessionmaker(bind=create_engine(url, connect_args={'connect_timeout': timeout}, encoding='utf8', echo=debug))

        self.address_cache = LFUCache(maxsize=16384)
        self.txid_cache = RRCache(maxsize=131072)
        self.utxo_cache = RRCache(maxsize=262144) if utxo_cache else None

        super(DatabaseIO, self).__init__(self.sessionmaker(), address_cache=self.address_cache, txid_cache=self.txid_cache, utxo_cache=self.utxo_cache)

    def new_session(self):
        return DatabaseSession(self.sessionmaker(), address_cache=self.address_cache, txid_cache=self.txid_cache, utxo_cache=self.utxo_cache)
