from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

class Daemon(AuthServiceProxy):
    def __init__(self, url):
        super(Daemon, self).__init__(url)

    def get_current_height(self):
        return self.getblockchaininfo()['blocks']

    def getblockheight(self, height):
        return self.getblock(self.getblockhash(height))

    def load_transaction(self, txid):
        return self.decoderawtransaction(self.getrawtransaction(txid))

