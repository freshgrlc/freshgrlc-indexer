from sqlalchemy import Column, ForeignKey, Integer, BigInteger, Float, String, CHAR, Binary, VARBINARY, DateTime, create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
Session = sessionmaker(bind=create_engine("mysql://indexer:indexer@localhost/blockchain", encoding='utf8', echo=True))

class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    type = Column(Integer)
    address = Column(String(64), unique=True)


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
    miner = Column(Integer, index=True)

    coinbaseinfo = relationship('CoinbaseInfo', back_populates='block')
    transactionreferences = relationship('BlockTransaction', back_populates='block')


class BlockTransaction(Base):
    __tablename__ = 'blocktx'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    block_id = Column('block', Integer, ForeignKey('block.id'), index=True)

    transaction = relationship('Transaction', back_populates='blockreferences', foreign_keys=[ transaction_id ])
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
    solo = Column(Integer)
    website = Column(String(64))
    graphcolor = Column(CHAR(6))

    pools = relationship('Pool', back_populates='group')


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

    confirmation = relationship('BlockTransaction', foreign_keys=[ confirmation_id ])
    blockreferences = relationship('BlockTransaction', back_populates='transaction', foreign_keys=[ BlockTransaction.transaction_id ])
    coinbaseinfo = relationship('CoinbaseInfo', back_populates='transaction')
    inputs = relationship('TransactionInput', back_populates='transaction')
    outputs = relationship('TransactionOutput', back_populates='transaction')


class TransactionInput(Base):
    __tablename__ = 'txin'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    input_id = Column('input', BigInteger, ForeignKey('txout.id'), index=True)

    transaction = relationship('Transaction', back_populates='inputs')
    input = relationship('TransactionOutput', back_populates='spenders', foreign_keys=[ input_id ])


class TransactionOutput(Base):
    __tablename__ = 'txout'

    id = Column(BigInteger, primary_key=True)
    transaction_id = Column('transaction', BigInteger, ForeignKey('transaction.id'), index=True)
    index = Column(Integer)
    type = Column(Integer)
    address = Column(Integer, index=True)
    amount = Column(Float(asdecimal=True))
    spentby_id = Column('spentby', BigInteger, ForeignKey('txin.id'), unique=True)

    transaction = relationship('Transaction', back_populates='outputs')
    spenders = relationship('TransactionInput', back_populates='input', foreign_keys=[ TransactionInput.input_id ])
    spentby = relationship('TransactionInput', foreign_keys=[ spentby_id ])

