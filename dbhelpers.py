from binascii import hexlify, unhexlify
from datetime import datetime
from decimal import Decimal

from models import *


class DatabaseIO(object):
    def __init__(self, url, debug=False):
        self.session = sessionmaker(bind=create_engine(url, encoding='utf8', echo=debug))()

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

    def import_blockinfo(self, blockinfo, runtime_metadata=None, tx_resolver=None):
        # Genesis block workaround
        if blockinfo['height'] == 0:
            blockinfo['tx'] = []

        print('Adding  blk %s' % blockinfo['hash'])

        for txid in blockinfo['tx']:
            if self.transaction(txid) == None and tx_resolver != None:
                txinfo, tx_runtime_metadata = tx_resolver(txid)
                self.import_transaction(txinfo, tx_runtime_metadata)

        hash = unhexlify(blockinfo['hash'])

        block = self.session.query(Block).filter(Block.hash == hash).first()

        if block != None:
            block.height = int(blockinfo['height'])
            self.session.commit()
            print('Updated block %d: %s' % (block.height, blockinfo['hash']))
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

    def import_transaction(self, txinfo, tx_runtime_metadata=None):
        #coinbase_inputs = filter(lambda txin: 'coinbase' in txin, txinfo['vin'])
        regular_inputs = filter(lambda txin: not 'coinbase' in txin, txinfo['vin'])

        if len(regular_inputs) > 0:
            print('Adding  tx  %s (%d inputs, %d outputs)' % (txinfo['hash'], len(regular_inputs), len(txinfo['vout'])))
        else:
            print('Adding  tx  %s (coinbase, %d outputs)' % (txinfo['hash'], len(txinfo['vout'])))

        tx = Transaction()

        tx.txid = unhexlify(txinfo['hash'])
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

        for index, inp in enumerate(regular_inputs):
            in_tx = self.session.query(Transaction).filter(Transaction.txid == unhexlify(inp['txid'])).first()
            in_txo = self.session.query(TransactionOutput).filter(TransactionOutput.transaction_id == in_tx.id).filter(TransactionOutput.index == inp['vout']).first()

            txin = TransactionInput()
            txin.input_id = in_txo.id
            txin.transaction_id = tx.id
            txin.index = index

            total_in += in_txo.amount

            self.session.add(txin)

        for outp in txinfo['vout']:
            utxo = TransactionOutput()
            utxo.transaction_id = tx.id
            utxo.index = outp['n']
            utxo.type = TXOUT_TYPES.internal_id(TXOUT_TYPES.from_rpcapi_type(outp['scriptPubKey']['type']))
            utxo.amount = outp['value']

            utxo.address_id = self.get_or_create_output_address_id(outp['scriptPubKey'])

            total_out += utxo.amount

            self.session.add(utxo)

        if len(regular_inputs) != 0:
            tx.totalvalue = total_in
            tx.fee = total_in - total_out
        else:   # Coinbase
            tx.totalvalue = total_out
            tx.fee = 0

        self.session.commit()

        print('Added   tx  %s' % hexlify(tx.txid))
        return tx

    def confirm_transaction(self, txid, internal_block_id, tx_resolver=None):
        print('Confirm tx  %s' % txid)

        tx = self.transaction(txid)

        if tx == None and tx_resolver != None:
            txinfo, tx_runtime_metadata = tx_resolver(txid)
            tx = self.import_transaction(txinfo, tx_runtime_metadata)

        blockref = self.session.query(BlockTransaction).filter(BlockTransaction.block_id == internal_block_id).filter(BlockTransaction.transaction_id == tx.id).first()
        if blockref == None:
            blockref = BlockTransaction()
            blockref.block_id = internal_block_id
            blockref.transaction_id = tx.id
            self.session.add(blockref)
            self.session.flush()

        tx.confirmation_id = blockref.id

        for input in tx.inputs:
            input.input.spentby_id = input.id

    def get_or_create_output_address_id(self, txout_address_info):
        if 'addresses' in txout_address_info and len(txout_address_info['addresses']) == 1:
            address = txout_address_info['addresses'][0]
            raw = None
            addr_type = ADDRESS_TYPES.BASE58

            if len(address) > 34:
                addr_type = ADDRESS_TYPES.BECH32

            db_address = self.session.query(Address).filter(Address.address == address).first()
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
            self.session.flush()

        return db_address.id

