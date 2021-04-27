

class Configuration(object):
    COIN_TICKER = 'grlc'
    DAEMON_URL = 'http://indexerrpc:indexer@127.0.0.1:42070'
    DATABASE_URL = 'mysql+pymysql://indexer:indexer@localhost/garlicoin'

    UTXO_CACHE = False

    API_ENDPOINT = ''

    DEBUG_SQL = False

