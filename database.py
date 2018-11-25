from binascii import hexlify, unhexlify
from datetime import datetime
from decimal import Decimal
from cachetools import LFUCache, RRCache
from sqlalchemy import create_engine, tuple_, or_, func as sqlfunc
from sqlalchemy.orm import sessionmaker
from sys import version_info
from time import time

from models import *
from postprocessor import convert_date


INTEGER_TYPES = [int] if version_info[0] > 2 else [int, long]


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

    def blocks(self, start_height, limit):
        return self.session.query(Block).filter(Block.height >= start_height).order_by(Block.height).limit(limit).all()

    def address_info(self, address, mutations_limit=100):
        address = self.session.query(Address).filter(Address.address == address).first()
        if address == None:
            return None
        return {
            'balance': float(address.balance),
            'pending': self.address_pending_balance(address.address)
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
        if not include_confirmation_info:
            return self.session.query(Transaction)

        return self.session.query(
            Transaction,
            BlockTransaction,
            Block
        ).join(
            Transaction.confirmation,
            isouter=(not confirmed_only)
        ).join(
            Block,
            isouter=(not confirmed_only)
        )

    def transaction(self, txid, include_confirmation_info=False):
        if len(txid) == 64:
            txid = unhexlify(txid)
        result = self.query_transactions(include_confirmation_info=include_confirmation_info).filter(Transaction.txid == txid).first()
        return result if not include_confirmation_info else result[0]

    def transaction_internal_id(self, txid):
        _txid = unhexlify(txid)
        if _txid in self.txid_cache:
            return self.txid_cache[_txid]
        tx = self.transaction(txid)
        return tx.id if tx is not None else None

    def latest_transactions(self, confirmed_only=False, limit=100):
        return [
            result[0]
            for result in self.query_transactions(include_confirmation_info=True, confirmed_only=confirmed_only).order_by(Transaction.id.desc()).limit(limit).all()
        ]

    def pool_stats(self, since):
        results = self.session.query(
            Pool.name,
            sqlfunc.count(Block.id).label('blocks'),
            sqlfunc.max(Block.height).label('lastblock'),
            Pool.website,
            Pool.graphcolor
        ).join(Block).filter(Block.timestamp >= since).group_by(Pool.name).all()
        return [dict(zip(('name', 'amountmined', 'latestblock', 'website', 'graphcolor'), stats)) for stats in results]

    def network_stats(self, since):
        block_stats = self.session.query(
            sqlfunc.count(Block.id)
        ).filter(Block.timestamp >= since).filter(Block.height != None).all()[0]
        transaction_stats = self.session.query(
            sqlfunc.count(Block.id),
            sqlfunc.sum(Transaction.totalvalue)
        ).join(
            BlockTransaction
        ).join(
            Transaction,
            Transaction.id == BlockTransaction.transaction_id
        ).filter(
            Block.timestamp >= since,
            Block.height != None,
            Transaction.coinbaseinfo == None
        ).all()[0]
        return dict(zip(('blocks', 'transactions', 'transactedvalue'), (block_stats[0], transaction_stats[0], transaction_stats[1])))

    def richlist(self, limit):
        return [ { 'address': v[0], 'balance': v[1] } for v in self.session.query(Address.address, Address.balance).order_by(Address.balance.desc()).limit(limit).all() ]

    def total_coins(self):
        return { 'total': self.session.query(sqlfunc.sum(Address.balance)).first()[0] }

    def mempool(self):
        return self.session.query(Transaction).filter(Transaction.confirmation_id == None).filter(Transaction.coinbaseinfo == None).order_by(Transaction.id.desc()).all()

    def import_blockinfo(self, blockinfo, runtime_metadata=None, tx_resolver=None):
        # Genesis block workaround
        if blockinfo['height'] == 0:
            blockinfo['tx'] = []

        print('Adding  blk %s%s' % (blockinfo['hash'], '' if runtime_metadata is None else (' (via %s)' % runtime_metadata['relayip'])))

        coinbase_signatures = {}
        for txid in blockinfo['tx']:
            if self.transaction_internal_id(txid) is None and tx_resolver is not None:
                txinfo, tx_runtime_metadata = tx_resolver(txid)
                self.import_transaction(txinfo, tx_runtime_metadata, coinbase_signatures=coinbase_signatures)

        blockhash = unhexlify(blockinfo['hash'])
        block = self.block(blockhash)

        if block != None:
            block.height = int(blockinfo['height'])
            self.session.commit()
            self._chaintip = None
            print('Update  blk %s (height %d)' % (hexlify(block.hash), block.height))
            return

        block = Block()

        block.hash = blockhash
        block.height = int(blockinfo['height'])
        block.size = blockinfo['size']
        block.timestamp = datetime.utcfromtimestamp(blockinfo['time'])
        block.difficulty = blockinfo['difficulty']
        block.firstseen = runtime_metadata['relaytime'] if runtime_metadata is not None else None
        block.relayedby = runtime_metadata['relayip'] if runtime_metadata is not None else None
        block.miner_id = None

        self.session.add(block)
        self.session.flush()

        for tx in blockinfo['tx']:
            self.confirm_transaction(tx, block.id)

        if len(coinbase_signatures) > 0:
            print('Adding  cb  %s' % coinbase_signatures.keys()[0])
            self.add_coinbase_data(block, coinbase_signatures.keys()[0], coinbase_signatures.values()[0][0], coinbase_signatures.values()[0][1])

            if runtime_metadata is not None:
                tx = self.transaction(coinbase_signatures.keys()[0])
                tx.firstseen = block.firstseen
                tx.relayedby = block.relayedby
                self.session.add(tx)

        print('Commit  blk %s' % hexlify(block.hash))
        self.session.commit()
        self._chaintip = None
        print('Added   blk %s (height %d)' % (hexlify(block.hash), block.height))

    def orphan_blocks(self, first_height):
        chaintip = self.chaintip()
        for height in range(chaintip.height, first_height - 1, -1):
            self.orphan_block(height)

    def orphan_block(self, height):
        block = self.block(height)

        if block != None:
            for txref in self.session.query(BlockTransaction).filter(BlockTransaction.block_id == block.id).all():
                txref.transaction.confirmation = None
                for tx_input in txref.transaction.inputs:
                    tx_input.input.spentby_id = None

            block.height = None
            self.session.commit()

    def import_transaction(self, txinfo, tx_runtime_metadata=None, coinbase_signatures=None):
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

        if len(regular_inputs) > 0:
            print('Adding  tx  %s (%d inputs, %d outputs, via %s)' % (
                txinfo['txid'],
                len(regular_inputs),
                len(txinfo['vout']),
                tx_runtime_metadata['relayip'] if tx_runtime_metadata is not None else 'unknown'
            ))
        else:
            print('Adding  tx  %s (coinbase, %d outputs)' % (txinfo['txid'], len(txinfo['vout'])))

        tx = Transaction()

        tx.txid = unhexlify(txinfo['txid'])
        tx.size = txinfo['size']
        tx.fee = -1.0
        tx.totalvalue = -1.0
        tx.firstseen = tx_runtime_metadata['relaytime'] if tx_runtime_metadata is not None else None
        tx.relayedby = tx_runtime_metadata['relayip'] if tx_runtime_metadata is not None else None
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
        tx.totalvalue, tx.fee = self.calculate_tx_totals(total_in, total_out, coinbase=is_coinbase_tx)

        self.session.bulk_save_objects(utxos, return_defaults=(self.utxo_cache is not None))

        print('Commit  tx  %s' % hexlify(tx.txid))
        self.session.commit()

        if self.utxo_cache is not None:
            self.update_utxo_cache(txinfo['txid'], tx.id, utxos)
            print('Added   tx  %s (utxo cache: %d, hit %d/%d, txid cache: %d, address cache: %d)' % (
                txinfo['txid'],
                self.utxo_cache.currsize,
                utxo_cache_hits,
                len(regular_inputs),
                self.txid_cache.currsize,
                self.address_cache.currsize
            ))
        else:
            print('Added   tx  %s (txid cache: %d, hit %d/%d, address cache: %d)' % (
                txinfo['txid'],
                self.txid_cache.currsize,
                txid_cache_hits,
                len(regular_inputs),
                self.address_cache.currsize
            ))

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
            utxo.type = TXOUT_TYPES.internal_id(TXOUT_TYPES.from_rpcapi_type(outp['scriptPubKey']['type']))
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
            if TXOUT_TYPES.resolve(utxo.type) != TXOUT_TYPES.RAW:
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
        print('Confirm tx  %s' % txid)

        tx_id = self.transaction_internal_id(txid)

        if tx_id is None and tx_resolver is not None:
            txinfo, tx_runtime_metadata = tx_resolver(txid)
            tx_id = self.import_transaction(txinfo, tx_runtime_metadata).id

        blockref = self.session.query(BlockTransaction).filter(BlockTransaction.block_id == internal_block_id).filter(BlockTransaction.transaction_id == tx_id).first()
        if blockref == None:
            blockref = BlockTransaction()
            blockref.block_id = internal_block_id
            blockref.transaction_id = tx_id
            self.session.add(blockref)
            self.session.flush()

        # tx.confirmation_id = blockref.id
        #
        # for input in tx.inputs:
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

        if 'addresses' in txout_address_info and len(txout_address_info['addresses']) == 1:
            address = txout_address_info['addresses'][0]
            raw = None
            addr_type = ADDRESS_TYPES.BASE58

            if len(address) > 34:
                addr_type = ADDRESS_TYPES.BECH32

            if address in self.address_cache:
                db_address = self.address_cache[address]
            else:
                db_address = self.session.query(Address).filter(Address.address == address).first()
                if db_address != None:
                    self.address_cache[address] = CachedAddress(db_address)
        else:
            db_address = None
            address = None
            raw = txout_address_info['asm']
            if raw[0:10] == 'OP_RETURN ' and len(raw.split(' ')) == 2:
                raw = raw.split(' ')[1]
                addr_type = ADDRESS_TYPES.DATA
            else:
                addr_type = ADDRESS_TYPES.RAW

        if db_address == None:
            db_address = Address()
            db_address.address = address
            db_address.type = ADDRESS_TYPES.internal_id(addr_type)
            db_address.raw = raw

            self.session.add(db_address)

            if flushdb:
                self.session.flush()

        return db_address

    def next_tx_without_mutations_info(self, last_id=None):
        self.session.rollback()

        query = self.session.query(Transaction).join(Mutation, isouter=True).filter(Mutation.id == None)

        if last_id is not None:
            query = query.filter(Transaction.id > last_id)

        return query.first()

    def add_tx_mutations_info(self, tx):
        print('Import  mts %s' % hexlify(tx.txid))
        self.session.execute('''
            INSERT INTO `mutation` (`transaction`, `address`, `amount`)
                SELECT :tx_id, `address`, SUM(`amount`) FROM (
                    SELECT `txout`.`address`, `txout`.`amount` FROM `transaction`
                        JOIN `txout` ON `transaction`.`id` = `txout`.`transaction`
                        WHERE `transaction`.`id` = :tx_id
                    UNION
                    SELECT `txout`.`address`, '0' - `txout`.`amount` FROM `transaction`
                        JOIN `txin` ON `transaction`.`id` = `txin`.`transaction`
                        JOIN `txout` ON `txin`.`input` = `txout`.`id`
                        WHERE `transaction`.`id` = :tx_id
                ) temp
                    GROUP BY address;
            ''', {
            'tx_id': tx.id
        })
        self.session.commit()

    def next_dirty_address(self, check_for_id=1, random_address=False):
        self.session.rollback()
        return self.session.query(Address).filter(Address.balance_dirty == check_for_id).order_by(Address.id if not random_address else sqlfunc.rand()).first()

    def get_address_balance(self, address):
        return self.session.execute('SELECT SUM(`txout`.`amount`) FROM `txout` JOIN `transaction` ON `txout`.`transaction` = `transaction`.`id` WHERE `txout`.`address` = :address_id AND `txout`.`spentby` IS NULL AND `transaction`.`confirmation` IS NOT NULL UNION SELECT \'0.0\' LIMIT 1;', {
            'address_id': address.id
        }).first()[0]

    def update_address_balance(self, address):
        print('Update  bal %s' % (address.address if address.address is not None else ' < RAW >'))
        utxos = self.session.query(TransactionOutput).filter(TransactionOutput.address_id == address.id, TransactionOutput.spentby_id == None).count()
        if utxos > 5000:
            self.session.execute('UPDATE `address` SET `balance_dirty` = \'2\' WHERE `address`.`id` = :address_id;', {
                'address_id': address.id
            })
            print('Skipped bal %s (%d utxos)' % (address.address if address.address is not None else ' < RAW >', utxos))
        else:
            self.session.execute('UPDATE `address` SET `balance_dirty` = \'0\', `balance` = (SELECT SUM(`txout`.`amount`) FROM `txout` JOIN `transaction` ON `txout`.`transaction` = `transaction`.`id` WHERE `txout`.`address` = :address_id AND `txout`.`spentby` IS NULL AND `transaction`.`confirmation` IS NOT NULL UNION SELECT \'0.0\' LIMIT 1) WHERE `address`.`id` = :address_id;', {
                'address_id': address.id
            })
        self.session.commit()

    def update_address_balance_slow(self, address):
        print('Update  bal %s' % (address.address if address.address is not None else ' < RAW >'))
        start_time = time()

        address.balance_dirty = 3
        self.session.commit()

        balance = self.get_address_balance(address)
        self.session.expire(address)

        if address.balance_dirty == 3:
            address.balance_dirty = 0
            address.balance = balance
            print('Updated bal %s (%s, in %d secs)' % (address.address if address.address is not None else ' < RAW >', str(balance), time() - start_time))
            self.session.commit()
        else:
            print('Abort   bal %s' % (address.address if address.address is not None else ' < RAW >'))

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
