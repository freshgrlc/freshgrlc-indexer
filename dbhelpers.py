from binascii import hexlify, unhexlify
from datetime import datetime
from decimal import Decimal
from cachetools import LFUCache, RRCache
from sqlalchemy import tuple_, or_

from models import *


class DatabaseIO(object):
    def __init__(self, url, utxo_cache=True, debug=False):
        self.session = sessionmaker(bind=create_engine(url, encoding='utf8', echo=debug))()
        self.address_cache = LFUCache(maxsize = 16384)
        self.txid_cache = RRCache(maxsize = 16384)
        self.utxo_cache = RRCache(maxsize = 32768 * 8) if utxo_cache else None

    def flush(self):
        self.session.flush()
        self.session.close()

    def chaintip(self):
        return self.session.query(Block).filter(Block.height != None).order_by(Block.height.desc()).first()

    def block(self, blockid):
        if type(blockid) in [ int, long ]:
            pass
        elif not len(blockid) in [ 32, 64 ]:
            blockid = int(blockid)

        if type(blockid) in [ int, long ]:
            return self.session.query(Block).filter(Block.height == blockid).first()

        return self.session.query(Block).filter(Block.hash == (unhexlify(blockid) if len(blockid) == 64 else blockid)).first()

    def transaction(self, txid):
        if len(txid) == 64:
            txid = unhexlify(txid)
        return self.session.query(Transaction).filter(Transaction.txid == txid).first()

    def transaction_internal_id(self, txid):
        _txid = unhexlify(txid)
        if _txid in self.txid_cache:
            return self.txid_cache[_txid]
        tx = self.transaction(txid)
        return tx.id if tx != None else None

    def import_blockinfo(self, blockinfo, runtime_metadata=None, tx_resolver=None):
        # Genesis block workaround
        if blockinfo['height'] == 0:
            blockinfo['tx'] = []

        print('Adding  blk %s' % blockinfo['hash'])

        coinbase_signatures = {}

        for txid in blockinfo['tx']:
            if self.transaction_internal_id(txid) == None and tx_resolver != None:
                txinfo, tx_runtime_metadata = tx_resolver(txid)
                self.import_transaction(txinfo, tx_runtime_metadata, coinbase_signatures=coinbase_signatures)
            elif tx_resolver != None:   # Temporary
                txinfo, tx_runtime_metadata = tx_resolver(txid)
                coinbase_inputs = filter(lambda txin: 'coinbase' in txin, txinfo['vin'])
                if len(coinbase_inputs) > 0:
                    coinbase_regular_outputs = filter(lambda txo: txo['value'] > 0.0 and 'addresses' in txo['scriptPubKey'] and len(txo['scriptPubKey']['addresses']) == 1, txinfo['vout'])
                    coinbase_signatures[txinfo['txid']] = (coinbase_inputs[0]['coinbase'], [ (txo['n'], txo['scriptPubKey']['addresses'][0], txo['value']) for txo in coinbase_regular_outputs ])

        hash = unhexlify(blockinfo['hash'])
        block = self.block(hash)

        if block != None:
            block.height = int(blockinfo['height'])
            self.session.commit()
            print('Update  blk %s (height %d)' % (hexlify(block.hash), block.height))
            return

        block = Block()

        block.hash = hash
        block.height = int(blockinfo['height'])
        block.size = blockinfo['size']
        block.timestamp = datetime.fromtimestamp(blockinfo['time'])
        block.difficulty = blockinfo['difficulty']
        block.firstseen = runtime_metadata['relaytime'] if runtime_metadata != None else None
        block.relayedby = runtime_metadata['relayip'] if runtime_metadata != None else None
        block.miner = None

        self.session.add(block)
        self.session.flush()

        for tx in blockinfo['tx']:
            self.confirm_transaction(tx, block.id)

        if len(coinbase_signatures) > 0:
            print('Adding  cb  %s' % coinbase_signatures.keys()[0])
            self.add_coinbase_data(block, coinbase_signatures.keys()[0], coinbase_signatures.values()[0][0], coinbase_signatures.values()[0][1])

        print('Commit  blk %s' % hexlify(block.hash))
        self.session.commit()
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
                for input in txref.transaction.inputs:
                    input.input.spentby_id = None

            block.height = None
            self.session.commit()

    def import_transaction(self, txinfo, tx_runtime_metadata=None, coinbase_signatures=None):
        coinbase_inputs = filter(lambda txin: 'coinbase' in txin, txinfo['vin'])
        regular_inputs = filter(lambda txin: not 'coinbase' in txin, txinfo['vin'])

        if coinbase_signatures != None and len(coinbase_inputs) > 0:
            coinbase_regular_outputs = filter(lambda txo: txo['value'] > 0.0 and 'addresses' in txo['scriptPubKey'] and len(txo['scriptPubKey']['addresses']) == 1, txinfo['vout'])
            coinbase_signatures[txinfo['txid']] = (coinbase_inputs[0]['coinbase'], [ (txo['n'], txo['scriptPubKey']['addresses'][0], txo['value']) for txo in coinbase_regular_outputs ])

        if len(regular_inputs) > 0:
            print('Adding  tx  %s (%d inputs, %d outputs)' % (txinfo['txid'], len(regular_inputs), len(txinfo['vout'])))
        else:
            print('Adding  tx  %s (coinbase, %d outputs)' % (txinfo['txid'], len(txinfo['vout'])))

        tx = Transaction()

        tx.txid = unhexlify(txinfo['txid'])
        tx.size = txinfo['size']
        tx.fee = -1.0
        tx.totalvalue = -1.0
        tx.firstseen = tx_runtime_metadata['relaytime'] if tx_runtime_metadata != None else None
        tx.relayedby = tx_runtime_metadata['relayip'] if tx_runtime_metadata != None else None
        tx.confirmation_id = None

        self.session.add(tx)
        self.session.flush()

        total_in = Decimal(0.0)
        total_out = Decimal(0.0)

        utxo_cache_map = {}

        if len(regular_inputs) > 0:
            for inp in regular_inputs:
                inp['_txid'] = unhexlify(inp['txid'])
                inp['_txo'] = inp['txid'] + '_' + str(inp['vout'])

            if self.utxo_cache != None:
                non_cached_inputs = []

                for inp in regular_inputs:
                    key = inp['_txo']
                    if key in self.utxo_cache:
                        utxo_cache_map[key] = self.utxo_cache[key]
                        del self.utxo_cache[key]
                    else:
                        non_cached_inputs.append(inp)
            else:
                non_cached_inputs = regular_inputs

            queryfilter = tuple([ tuple_(TransactionOutput.transaction_id, TransactionOutput.index) == (self.txid_cache[txo[0]], txo[1]) for txo in filter(lambda txo: txo[0] in self.txid_cache, [ (inp['_txid'], inp['vout']) for inp in non_cached_inputs ]) ])
            ctx_txo_map = { (str(txo.transaction_id) + '_' + str(txo.index)): (txo.transaction_id, txo.id, txo.amount) for txo in self.session.query(TransactionOutput).filter(or_(*queryfilter)).all() } if queryfilter != () else {}

            temp = non_cached_inputs
            non_cached_inputs = []

            for inp in temp:
                if inp['_txid'] in self.txid_cache:
                    utxo_cache_map[inp['_txo']] = ctx_txo_map[str(self.txid_cache[inp['_txid']]) + '_' + str(inp['vout'])]
                else:
                    non_cached_inputs.append(inp)

            if len(non_cached_inputs) > 0:
                queryfilter = tuple([ tuple_(Transaction.txid, TransactionOutput.index) == (inp['_txid'], inp['vout']) for inp in non_cached_inputs ])
                results = self.session.query(
                    TransactionOutput,
                    Transaction
                ).join(
                    Transaction
                ).filter(or_(*queryfilter)).all()

                txo_map = { (hexlify(tx.txid) + '_' + str(txo.index)): (tx.id, txo.id, txo.amount) for (txo, tx) in results }
            else:
                txo_map = {}

            for k, v in utxo_cache_map.items():
                txo_map[k] = v

            txins = []
            for index, inp in enumerate(regular_inputs):
                in_entry = txo_map[inp['txid'] + '_' + str(inp['vout'])]

                txin = TransactionInput()
                txin.input_id = in_entry[1]
                txin.transaction_id = in_entry[0]
                txin.index = index
                txins.append(txin)

                total_in += in_entry[2]

            self.session.bulk_save_objects(txins)

        address_map = {}
        for outp in txinfo['vout']:
            address_map[outp['n']] = self.get_or_create_output_address(outp['scriptPubKey'], flushdb=False)

        self.session.flush()

        utxos = []
        for outp in txinfo['vout']:
            utxo = TransactionOutput()
            utxo.transaction_id = tx.id
            utxo.index = outp['n']
            utxo.type = TXOUT_TYPES.internal_id(TXOUT_TYPES.from_rpcapi_type(outp['scriptPubKey']['type']))
            utxo.amount = outp['value']

            utxo.address_id = address_map[outp['n']].id

            total_out += utxo.amount

            utxos.append(utxo)

        if len(regular_inputs) != 0:
            tx.totalvalue = total_in
            tx.fee = total_in - total_out
        else:   # Coinbase
            tx.totalvalue = total_out
            tx.fee = 0

        self.session.bulk_save_objects(utxos, return_defaults=(self.utxo_cache != None))

        print('Commit  tx  %s' % hexlify(tx.txid))
        self.session.commit()

        if self.utxo_cache != None:
            for utxo in utxos:
                if TXOUT_TYPES.resolve(utxo.type) != TXOUT_TYPES.RAW:
                    self.utxo_cache[txinfo['txid'] + '_' + str(utxo.index)] = (tx.id, utxo.id, utxo.amount)

            print('Added   tx  %s (utxo cache: %d, hit %d/%d, txid cache: %d, address cache: %d)' % (hexlify(tx.txid), self.utxo_cache.currsize, len(utxo_cache_map), len(regular_inputs), self.txid_cache.currsize, self.address_cache.currsize))
        else:
            print('Added   tx  %s (txid cache: %d, hit %d/%d, address cache: %d)' % (hexlify(tx.txid), self.txid_cache.currsize, len(utxo_cache_map), len(regular_inputs), self.address_cache.currsize))

        self.txid_cache[tx.txid] = tx.id
        return tx

    def confirm_transaction(self, txid, internal_block_id, tx_resolver=None):
        print('Confirm tx  %s' % txid)

        tx_id = self.transaction_internal_id(txid)

        if tx_id == None and tx_resolver != None:
            txinfo, tx_runtime_metadata = tx_resolver(txid)
            tx_id = self.import_transaction(txinfo, tx_runtime_metadata).id

        blockref = self.session.query(BlockTransaction).filter(BlockTransaction.block_id == internal_block_id).filter(BlockTransaction.transaction_id == tx_id).first()
        if blockref == None:
            blockref = BlockTransaction()
            blockref.block_id = internal_block_id
            blockref.transaction_id = tx_id
            self.session.add(blockref)
            self.session.flush()

        #tx.confirmation_id = blockref.id
        #
        #for input in tx.inputs:
        #    input.input.spentby_id = input.id
        self.session.execute('UPDATE `transaction` SET `confirmation` = :blockref WHERE `id` = :tx_id;', { 'blockref': blockref.id, 'tx_id': tx_id })
        self.session.execute('UPDATE `txout` LEFT JOIN `txin` ON `txout`.`id` = `txin`.`input` SET `spentby` = `txin`.`id` WHERE `txin`.`transaction` = :tx_id;', { 'tx_id': tx_id })

    def add_coinbase_data(self, block, txid, signature, outputs):
        coinbaseinfo = CoinbaseInfo()
        coinbaseinfo.block_id = block.id
        coinbaseinfo.transaction_id = self.transaction_internal_id(txid)
        coinbaseinfo.raw = unhexlify(signature)
        coinbaseinfo.signature = None

        totalout = sum([ o[2] for o in outputs ])
        best_output = filter(lambda o: o[2] > (totalout * 95 / 100), outputs)
        best_output = best_output[0] if len(best_output) > 0 else None
        coinbaseinfo.mainoutput_id = self.session.query(TransactionOutput).filter(TransactionOutput.transaction_id == coinbaseinfo.transaction_id, TransactionOutput.index == best_output[0]).first().id if best_output != None else None

        solo = len(coinbaseinfo.raw) <= 8

        if not solo:
            if b'/' in coinbaseinfo.raw:
                try:
                    coinbaseinfo.signature = coinbaseinfo.raw.split(b'/')[-2].decode('utf-8').join(2 * ['/'])
                except IndexError:
                    pass

        self.session.add(coinbaseinfo)
        self.session.flush()

        self.find_and_set_miner(block, coinbaseinfo, solo)

    def find_and_set_miner(self, block, coinbaseinfo, solo):
        if not solo and coinbaseinfo.signature != None:
            pool_cbsig = self.session.query(PoolCoinbaseSignature).filter(PoolCoinbaseSignature.signature == coinbaseinfo.signature).first()
            if pool_cbsig != None:
                block.miner = pool_cbsig.pool_id
                return

        if coinbaseinfo.mainoutput != None and coinbaseinfo.mainoutput.address_id != None:
            pool_addr = self.session.query(PoolAddress).filter(PoolAddress.address_id == coinbaseinfo.mainoutput.address_id).first()
            if pool_addr != None:
                block.miner = pool_addr.pool_id
                return

            new_pool = Pool()
            new_pool.group_id = SOLO_POOL_GROUP_ID if solo else None
            new_pool.solo = 1 if solo else 0
            new_pool.name = coinbaseinfo.mainoutput.address.address + ' ' + ('(Solo miner)' if solo else '(Unkown Pool)')

            self.session.add(new_pool)
            self.session.flush()

            pool_addr = PoolAddress()
            pool_addr.address_id = coinbaseinfo.mainoutput.address_id
            pool_addr.pool_id = new_pool.id

            self.session.add(pool_addr)
            block.miner = new_pool.id
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

