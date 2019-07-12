from binascii import hexlify
from sqlalchemy import Column, ForeignKey, Integer, BigInteger, Float, String, CHAR, Binary, VARBINARY, DateTime, func as sqlfunc
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.declarative import declarative_base

from config import Configuration


SOLO_POOL_GROUP_ID = 1


class ADDRESS_TYPES:
    BASE58 = 'base58'
    BECH32 = 'bech32'
    DATA   = 'data'
    RAW    = 'raw'

    @classmethod
    def all(cls):
        return [
            cls.BASE58,
            cls.BECH32,
            cls.DATA,
            cls.RAW
        ]

    @classmethod
    def internal_id(cls, address_type):
        if address_type == cls.RAW:
            return -1

        try:
            return cls.all().index(address_type)
        except ValueError:
            return -1

    @classmethod
    def resolve(cls, internal_id):
        try:
            return cls.all()[internal_id]
        except IndexError:
            return cls.RAW


class TXOUT_TYPES:
    P2PK  = 'p2pk'
    P2PKH = 'p2pkh'
    P2SH  = 'p2sh'
    P2WPKH= 'p2wpkh'
    P2WSH = 'p2wsh'

    RAW   = 'raw'

    RPCAPI_MAPPINGS = {
        'nonstandard':              RAW,
        'pubkey':                   P2PK,
        'pubkeyhash':               P2PKH,
        'scripthash':               P2SH,
        'multisig':                 RAW,
        'nulldata':                 RAW,
        'witness_v0_keyhash':       P2WPKH,
        'witness_v0_scripthash':    P2WSH
    }

    @classmethod
    def all(cls):
        return [
            cls.P2PK,
            cls.P2PKH,
            cls.P2SH,
            cls.P2WPKH,
            cls.P2WSH,

            cls.RAW
        ]

    @classmethod
    def internal_id(cls, txtype):
        if txtype == cls.RAW:
            return -1

        try:
            return cls.all().index(txtype)
        except ValueError:
            return -1

    @classmethod
    def resolve(cls, internal_id):
        try:
            return cls.all()[internal_id]
        except IndexError:
            return cls.RAW

    @classmethod
    def from_rpcapi_type(cls, rpcapi_type):
        if rpcapi_type in cls.RPCAPI_MAPPINGS.keys():
            return cls.RPCAPI_MAPPINGS[rpcapi_type]
        return cls.RAW


def address_friendly_name(address):
    if address.address != None:
        return address.address
    if address.type == ADDRESS_TYPES.RAW:
        return 'Script: ' + address.raw
    if address.type == ADDRESS_TYPES.DATA:
        return 'Data: ' + address.raw
    return 'Unknown <' + address.raw + '>'


def make_witness_v0_p2wpkh(pubkeyhash):
    return ' '.join(['0', pubkeyhash])


def _make_transaction_ref(txid):
    return {'txid': txid, 'href': Configuration.API_ENDPOINT + '/transactions/' + txid + '/'}


def make_transaction_ref(transaction):
    return _make_transaction_ref(hexlify(transaction.txid))


def make_transaction_output_ref(txoutput):
    index = int(txoutput.index)
    ref = make_transaction_ref(txoutput.transaction)
    ref['href'] += 'outputs/' + str(index) + '/'
    ref['output'] = index
    return ref


def make_transaction_input_ref(txinput):
    index = int(txinput.index)
    ref = make_transaction_ref(txinput.transaction)
    ref['href'] += 'inputs/' + str(index) + '/'
    ref['input'] = index
    return ref


Base = declarative_base()


class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    type_id = Column('type', Integer)
    address = Column(String(64), unique=True)
    raw = Column(String(256))
    balance = Column(Integer)
    balance_dirty = Column(Integer, default=1)

    mutations = relationship('Mutation', back_populates='address')
    pool = relationship('PoolAddress', back_populates='address', cascade='save-update, merge, delete')

    @property
    def type(self):
        return ADDRESS_TYPES.resolve(self.type_id)

    @type.setter
    def type(self, type):
        self.type_id = ADDRESS_TYPES.internal_id(type)

    @property
    def pending(self):
        pending = Session.object_session(self).query(
            Address.id,
            sqlfunc.sum(Mutation.amount)
        ).join(
            Address.mutations
        ).join(
            Mutation.transaction
        ).filter(
            Address.id == self.id,
            Transaction.confirmation == None
        ).all()[0][1]
        return pending if pending != None else 0.0


class Block(Base):
    __tablename__ = 'block'

    id = Column(Integer, primary_key=True)
    hash = Column(Binary(32), unique=True)
    height = Column(Integer, unique=True)
    size = Column(Integer)
    totalfee = Column(Float(asdecimal=True))
    timestamp = Column(DateTime, index=True)
    difficulty = Column(Float(asdecimal=True))
    firstseen = Column(DateTime)
    relayedby = Column(String(48))
    miner_id = Column('miner', Integer, ForeignKey('pool.id'), index=True)

    miner = relationship('Pool')
    coinbaseinfo = relationship('CoinbaseInfo', back_populates='block', uselist=False)
    transactionreferences = relationship('BlockTransaction', back_populates='block', cascade='save-update, merge, delete')

    API_DATA_FIELDS = [hash, height, size, timestamp, difficulty, firstseen, relayedby, 'Block.totaltransacted', 'Block.totalfees', 'Block.miningreward']
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = [miner, 'Block.transactions', 'Transaction.mutations', 'Transaction.inputs', 'Transaction.outputs']

    @property
    def transactions(self):
        return [
            result[1]
            for result in Session.object_session(self).query(
                BlockTransaction.id,
                Transaction
            ).join(
                BlockTransaction.transaction
            ).filter(
                BlockTransaction.block_id == self.id
            ).order_by(BlockTransaction.id).all()
        ]

    @property
    def time(self):
        return self.firstseen if self.firstseen != None else self.timestamp

    @property
    def totalfees(self):
        return self.totalfee

    @property
    def miningreward(self):
        return self.coinbaseinfo.newcoins if self.coinbaseinfo != None else None

    @property
    def totaltransacted(self):
        return sum([ tx.totalvalue for tx in filter(lambda tx: not tx.coinbase, self.transactions) ])


class BlockTransaction(Base):
    __tablename__ = 'blocktx'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    block_id = Column('block', Integer, ForeignKey('block.id'), index=True)

    transaction = relationship('Transaction', back_populates='blockreferences', foreign_keys=[transaction_id])
    block = relationship('Block', back_populates='transactionreferences')


class CoinbaseInfo(Base):
    __tablename__ = 'coinbase'

    block_id = Column('block', Integer, ForeignKey('block.id'), primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), unique=True)
    newcoins = Column(Float(asdecimal=True))
    raw = Column(VARBINARY(256))
    signature = Column(String(32), index=True)
    mainoutput_id = Column('mainoutput', BigInteger, ForeignKey('txout.id'), index=True)

    block = relationship('Block', back_populates='coinbaseinfo', uselist=False)
    transaction = relationship('Transaction', back_populates='coinbaseinfo', uselist=False)
    mainoutput = relationship('TransactionOutput')


class Mutation(Base):
    __tablename__ = 'mutation'

    id = Column(Integer, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), unique=True)
    address_id = Column('address', Integer, ForeignKey('address.id'), index=True)
    amount = Column(Float(asdecimal=True))

    transaction = relationship('Transaction', back_populates='address_mutations')
    address = relationship('Address', back_populates='mutations')


class Pool(Base):
    __tablename__ = 'pool'

    id = Column(Integer, primary_key=True)
    group_id = Column('group', Integer, ForeignKey('poolgroup.id'), index=True)
    name = Column(String(64), unique=True)
    solo = Column(Integer)
    website = Column(String(64))
    graphcolor = Column(String(6))

    group = relationship('PoolGroup', back_populates='pools')
    addresses = relationship('PoolAddress', back_populates='pool', cascade='save-update, merge, delete')
    coinbasesignatures = relationship('PoolCoinbaseSignature', back_populates='pool', cascade='save-update, merge, delete')

    API_DATA_FIELDS = [name, website, graphcolor]
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = [group]


class PoolAddress(Base):
    __tablename__ = 'pooladdress'

    address_id = Column('address', Integer, ForeignKey('address.id'), primary_key=True)
    pool_id = Column('pool', Integer, ForeignKey('pool.id'), index=True)

    address = relationship('Address', back_populates='pool')
    pool = relationship('Pool', back_populates='addresses')


class PoolGroup(Base):
    __tablename__ = 'poolgroup'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), unique=True)
    website = Column(String(64))
    graphcolor = Column(CHAR(6))

    pools = relationship('Pool', back_populates='group')

    API_DATA_FIELDS = [name, graphcolor]
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = []


class PoolCoinbaseSignature(Base):
    __tablename__ = 'poolsignature'

    id = Column(Integer, primary_key=True)
    signature = Column(String(32), unique=True)
    pool_id = Column('pool', Integer, ForeignKey('pool.id'), index=True)

    pool = relationship('Pool', back_populates='coinbasesignatures')


class Transaction(Base):
    __tablename__ = 'transaction'

    id = Column(BigInteger, primary_key=True)
    txid = Column(Binary(32), unique=True)
    size = Column(Integer)
    fee = Column(Float(asdecimal=True))
    totalvalue = Column(Float(asdecimal=True))
    firstseen = Column(DateTime())
    relayedby = Column(String(48))
    confirmation_id = Column('confirmation', BigInteger, ForeignKey('blocktx.id'), unique=True)

    confirmation = relationship('BlockTransaction', foreign_keys=[confirmation_id])
    blockreferences = relationship('BlockTransaction', back_populates='transaction', foreign_keys=[BlockTransaction.transaction_id], cascade='save-update, merge, delete')
    coinbaseinfo = relationship('CoinbaseInfo', back_populates='transaction', uselist=False)
    txinputs = relationship('TransactionInput', back_populates='transaction', cascade='save-update, merge, delete')
    txoutputs = relationship('TransactionOutput', back_populates='transaction', cascade='save-update, merge, delete')
    address_mutations = relationship('Mutation', back_populates='transaction', cascade='save-update, merge, delete')

    API_DATA_FIELDS = [txid, size, fee, totalvalue, firstseen, 'Transaction.confirmed', 'Transaction.coinbase']
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = ['Transaction.block', 'Transaction.mutations', 'Transaction.inputs', 'Transaction.outputs']

    @property
    def confirmed(self):
        return self.confirmation_id != None

    @property
    def coinbase(self):
        return self.coinbaseinfo != None

    @property
    def block(self):
        return self.confirmation.block if self.confirmation_id != None else None

    @property
    def block_id(self):
        return self.confirmation.block_id if self.confirmation_id != None else None

    @property
    def block_id(self):
        return self.confirmation.block_id if self.confirmation_id != None else None

    @property
    def mutations(self):
        mutations = [ (address_friendly_name(m.address), m.amount) for m in self.address_mutations ]
        return {
            'inputs': dict([ (m[0], -m[1]) for m in filter(lambda m: m[1] < 0.0, mutations) ]),
            'outputs': dict(filter(lambda m: m[1] > 0.0, mutations))
        }

    @property
    def inputs(self):
        return dict([
            (input.index, {
                'amount':   input.input.amount,
                'type':     input.input.type,
                'address':  address_friendly_name(input.input.address),
                'spends':   make_transaction_output_ref(input.input)
            }) for input in self.txinputs
        ])

    @property
    def outputs(self):
        return dict([
            (output.index, {
                'amount':   output.amount,
                'type':     output.type,
                'address':  address_friendly_name(output.address),
                'script':   output.script,
                'spentby':  make_transaction_input_ref(output.spentby) if output.spentby != None else None
            }) for output in self.txoutputs
        ])

    @property
    def time(self):
        if self.firstseen != None:
            return self.firstseen
        block = self.block
        return block.time if block != None else None


class TransactionInput(Base):
    __tablename__ = 'txin'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    input_id = Column('input', BigInteger, ForeignKey('txout.id'), index=True)

    transaction = relationship('Transaction', back_populates='txinputs')
    input = relationship('TransactionOutput', back_populates='spenders', foreign_keys=[input_id])


class TransactionOutput(Base):
    __tablename__ = 'txout'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    type_id = Column('type', Integer)
    address_id = Column('address', Integer, ForeignKey('address.id'), index=True)
    amount = Column(Float(asdecimal=True))
    spentby_id = Column('spentby', BigInteger, ForeignKey('txin.id'), unique=True)

    transaction = relationship('Transaction', back_populates='txoutputs')
    address = relationship('Address')
    spenders = relationship('TransactionInput', back_populates='input', foreign_keys=[TransactionInput.input_id])
    spentby = relationship('TransactionInput', foreign_keys=[spentby_id])

    @property
    def type(self):
        return TXOUT_TYPES.resolve(self.type_id)

    @type.setter
    def type(self, type):
        self.type_id = TXOUT_TYPES.internal_id(type)

    @property
    def script(self):
        if self.address.type == ADDRESS_TYPES.DATA:
            return None

        if self.type != TXOUT_TYPES.P2WPKH or self.address.type == ADDRESS_TYPES.BECH32:
            return self.address.raw

        #
        #   Dealing with a p2wpkh output but coin uses same address for both transaction types,
        #   hence script saved on address object is legacy script, so we need to convert.
        #
        #   We'll just assume we need to convert from normal p2pkh script to witness v0 program.
        #
        opcodes = self.address.raw.split(' ') if self.address.raw != None else []

        if len(opcodes) == 5 and opcodes[:2] + opcodes[3:] == [ 'OP_DUP', 'OP_HASH160', 'OP_EQUALVERIFY', 'OP_CHECKSIG' ]:
            return make_witness_v0_p2wpkh(opcodes[2])

        # Not sure, just pretend everything is fine
        return self.address.raw

