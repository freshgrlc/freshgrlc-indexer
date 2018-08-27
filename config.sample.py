
class Configuration(object):
    DAEMON_URL = 'http://indexerrpc:indexer@127.0.0.1:42070'
    DATABASE_URL = 'mysql+pymysql://indexer:indexer@localhost/garlicoin'
    NODE_LOGFILE = '/var/log/garlicoin.log'

    UTXO_CACHE = False

    API_ENDPOINT = ''

    DEBUG_SQL = False

