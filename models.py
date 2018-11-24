from binascii import hexlify
from sqlalchemy import Column, ForeignKey, Integer, BigInteger, Float, String, CHAR, Binary, VARBINARY, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

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


Base = declarative_base()


class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    type = Column(Integer)
    address = Column(String(64), unique=True)
    raw = Column(String(256))
    balance = Column(Integer)
    balance_dirty = Column(Integer, default=1)

    mutations = relationship('Mutation', back_populates='address')


class Block(Base):
    __tablename__ = 'block'

    id = Column(Integer, primary_key=True)
    hash = Column(Binary(32), unique=True)
    height = Column(Integer, unique=True)
    size = Column(Integer)
    timestamp = Column(DateTime, index=True)
    difficulty = Column(Float(asdecimal=True))
    firstseen = Column(DateTime)
    relayedby = Column(String(48))
    miner_id = Column('miner', Integer, ForeignKey('pool.id'), index=True)

    miner = relationship('Pool')
    coinbaseinfo = relationship('CoinbaseInfo', back_populates='block')
    transactionreferences = relationship('BlockTransaction', back_populates='block')

    API_DATA_FIELDS = [hash, height, size, timestamp, difficulty, firstseen, relayedby]
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = [miner, 'Block.transactions']

    def __getattribute__(self, name):
        if name == 'transactions':
            return [ref.transaction for ref in self.transactionreferences]
        if name == 'time':
            return self.firstseen if self.firstseen != None else self.timestamp
        return super(Block, self).__getattribute__(name)


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
    raw = Column(VARBINARY(256))
    signature = Column(String(32), index=True)
    mainoutput_id = Column('mainoutput', BigInteger, ForeignKey('txout.id'), index=True)

    block = relationship('Block', back_populates='coinbaseinfo')
    transaction = relationship('Transaction', back_populates='coinbaseinfo')
    mainoutput = relationship('TransactionOutput')


class Mutation(Base):
    __tablename__ = 'mutation'

    id = Column(Integer, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), unique=True)
    address_id = Column('address', Integer, ForeignKey('address.id'), index=True)
    amount = Column(Float(asdecimal=True))

    transaction = relationship('Transaction')
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
    addresses = relationship('PoolAddress', back_populates='pool')
    coinbasesignatures = relationship('PoolCoinbaseSignature', back_populates='pool')

    API_DATA_FIELDS = [name, website, graphcolor]
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = [group]


class PoolAddress(Base):
    __tablename__ = 'pooladdress'

    address_id = Column('address', Integer, ForeignKey('address.id'), primary_key=True)
    pool_id = Column('pool', Integer, ForeignKey('pool.id'), index=True)

    address = relationship('Address')
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
    blockreferences = relationship('BlockTransaction', back_populates='transaction', foreign_keys=[BlockTransaction.transaction_id])
    coinbaseinfo = relationship('CoinbaseInfo', back_populates='transaction')
    inputs = relationship('TransactionInput', back_populates='transaction')
    outputs = relationship('TransactionOutput', back_populates='transaction')

    API_DATA_FIELDS = [txid, size, fee, totalvalue, firstseen, 'Transaction.confirmed', 'Transaction.coinbase']
    POSTPROCESS_RESOLVE_FOREIGN_KEYS = ['Transaction.block']

    def __getattribute__(self, name):
        if name == 'confirmed':
            return self.confirmation_id != None
        if name == 'coinbase':
            return len(self.coinbaseinfo) > 0
        if name == 'block':
            return self.confirmation.block if self.confirmation_id != None else None
        if name == 'block_id':
            return self.confirmation.block_id if self.confirmation_id != None else None
        if name == 'time':
            if self.firstseen != None:
                return self.firstseen
            block = self.block
            return block.time if block != None else None
        return super(Transaction, self).__getattribute__(name)


class TransactionInput(Base):
    __tablename__ = 'txin'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    input_id = Column('input', BigInteger, ForeignKey('txout.id'), index=True)

    transaction = relationship('Transaction', back_populates='inputs')
    input = relationship('TransactionOutput', back_populates='spenders', foreign_keys=[input_id])


class TransactionOutput(Base):
    __tablename__ = 'txout'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    type = Column(Integer)
    address_id = Column('address', Integer, ForeignKey('address.id'), index=True)
    amount = Column(Float(asdecimal=True))
    spentby_id = Column('spentby', BigInteger, ForeignKey('txin.id'), unique=True)

    transaction = relationship('Transaction', back_populates='outputs')
    address = relationship('Address')
    spenders = relationship('TransactionInput', back_populates='input', foreign_keys=[TransactionInput.input_id])
    spentby = relationship('TransactionInput', foreign_keys=[spentby_id])

