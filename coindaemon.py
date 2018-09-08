from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException


class TransactionSignError(Exception):
    pass


class Daemon(AuthServiceProxy):
    def __init__(self, url):
        super(Daemon, self).__init__(url)

    def get_current_height(self):
        return self.getblockchaininfo()['blocks']

    def getblockheight(self, height):
        return self.getblock(self.getblockhash(height))

    def load_transaction(self, txid):
        return self.decoderawtransaction(self.getrawtransaction(txid))

    def sign_transaction(self, raw_tx_hex, private_keys):
        result = self.signrawtransaction(raw_tx_hex, None, private_keys)
        if result['complete']:
            return result['hex']

        raise TransactionSignError(result['errors'])

