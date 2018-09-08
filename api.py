from gevent import monkey; monkey.patch_all()

from datetime import datetime
from flask import Flask, request, Response
from flask_cors import cross_origin
from werkzeug.datastructures import Headers

from config import Configuration
from database import DatabaseIO
from models import Block
from postprocessor import QueryDataPostProcessor
from eventgen import IndexerEventStream


webapp = Flask('indexer-api')
db = DatabaseIO(Configuration.DATABASE_URL, debug=Configuration.DEBUG_SQL)

stream = IndexerEventStream(db)


@webapp.route('/events/subscribe')
@cross_origin()
def subscribe():
    headers = Headers()
    headers.add('X-Accel-Buffering', 'no')
    headers.add('Cache-Control', 'no-cache')
    return Response(stream.subscriber(
        channels=(request.args.get('channels').split(',') if request.args.get('channels') is not None else [])),
        mimetype='text/event-stream',
        headers=headers
    )


@webapp.route('/blocks/')
@cross_origin()
def blocks():
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.pagination(backwards_indexes=True, tipresolver=(lambda: session.chaintip().height + 1))
            pp.baseurl('/blocks/<Block.hash>/').reflinks('miner', 'transactions').autoexpand()
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height'])
            return pp.process(session.blocks(pp.start, pp.limit)).json()


@webapp.route('/blocks/<blockid>/')
@cross_origin()
def block(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.baseurl('/blocks/<Block.hash>/').reflinks('miner', 'transactions').autoexpand()
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height'])
            return pp.process(session.block(blockid)).json()


@webapp.route('/blocks/<blockid>/miner/')
@cross_origin()
def blockminer(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.resolve_keys(Block.miner)
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height'])
            return pp.process(session.block(blockid))['miner'].json()


@webapp.route('/blocks/<blockid>/transactions/')
@cross_origin()
def blocktransactions(blockid):
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.resolve_keys('Block.transactions')
            return pp.process(session.block(blockid))['transactions'].json()


@webapp.route('/transactions/')
@cross_origin()
def transactions():
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            pp.pagination()
            pp.baseurl('/transactions/<Transaction.txid>/')
            pp.reflink('block', '/blocks/<query:transaction.block.hash>/', ['hash', 'height']).autoexpand()
            pp.reflinks('transactions')

            query_confirmed = request.args.get('confirmed')
            if query_confirmed is None or query_confirmed == '':
                data = session.latest_transactions(limit=pp.limit)
            elif query_confirmed == 'true':
                data = session.latest_transactions(limit=pp.limit, confirmed_only=True)
            elif query_confirmed == 'false':
                data = session.mempool()
            else:
                data = []

            return pp.process(data).json()


@webapp.route('/networkstats/')
@cross_origin()
def stats():
    since = datetime.fromtimestamp(int(request.args.get('since')))
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            data = pp.process_raw(session.network_stats(since=since)).data
            return pp.process_raw({
                'blocks': {
                    'amount':       data['blocks']
                },
                'transactions': {
                    'amount':       data['transactions'],
                    'totalvalue':   data['transactedvalue']
                }
            }).json()


@webapp.route('/poolstats/')
@cross_origin()
def poolstats():
    since = datetime.fromtimestamp(int(request.args.get('since')))
    with db.new_session() as session:
        with QueryDataPostProcessor() as pp:
            return pp.process_raw(session.pool_stats(since=since)).json()
