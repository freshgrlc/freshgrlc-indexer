from gevent import monkey

monkey.patch_all()

from flask import Flask, request, Response
from sqlalchemy import Column

from config import Configuration
from database import DatabaseIO
from models import Block
from postprocessor import QueryDataPostProcessor
from eventgen import IndexerEventStream

webapp = Flask('indexer-api')
db = DatabaseIO(Configuration.DATABASE_URL, debug=False)

stream = IndexerEventStream(db)

@webapp.route('/events/subscribe')
def subscribe():
    return Response(stream.subscriber(channels=(request.args.get('channels').split(',') if request.args.get('channels') != None else [])), mimetype='text/event-stream')

@webapp.route('/blocks/')
def blocks():
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.pagination(backwards_indexes=True, tipresolver=(lambda: session.chaintip().height + 1))
            pp.baseurl('/blocks/<Block.hash>/').reflinks('miner', 'transactions').autoexpand()

            return pp.process(session.blocks(pp.start, pp.limit)).json()

@webapp.route('/blocks/<blockid>/')
def block(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.baseurl('/blocks/<Block.hash>/').reflinks('miner', 'transactions').autoexpand()

            return pp.process(session.block(blockid)).json()

@webapp.route('/blocks/<blockid>/miner/')
def blockminer(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.resolve_keys(Block.miner)
            return pp.process(session.block(blockid))['miner'].json()

@webapp.route('/blocks/<blockid>/transactions/')
def blocktransactions(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.resolve_keys('Block.transactions')
            return pp.process(session.block(blockid))['transactions'].json()

@webapp.route('/transactions/')
def transactions():
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.pagination()
            pp.baseurl('/transactions/<Transaction.txid>/')

            query_confirmed = request.args.get('confirmed')
            if query_confirmed == None or query_confirmed == '':
                data = session.latest_transactions(limit=pp.limit)
            elif query_confirmed == 'true':
                data = session.latest_transactions(limit=pp.limit, confirmed_only=True)
            elif query_confirmed == 'false':
                data = session.mempool()
            else:
                data = []

            return pp.process(data).json()
