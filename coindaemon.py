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

    #def get_miner_id(self, height):
        #if height == 0:
            #return 'GENESIS BLOCK', None

        #coinbase_tx = self.load_transaction(self.getblockheight(height)['tx'][0])

        #coinbase_pool_id = None
        #miner_address = None

        #vouts = list(filter(lambda out: 'type' in out['scriptPubKey'] and out['scriptPubKey']['type'] != 'nulldata' and 'addresses' in out['scriptPubKey'] and len(out['scriptPubKey']['addresses']) == 1, coinbase_tx['vout']))

        #if len(vouts) > 1 and len(vouts) < 5:
            #known_vouts = list(filter(lambda out: vouts[0]['scriptPubKey']['addresses'][0] in ALIASES, vouts))
            #vouts = [ known_vouts[0] ] if len(known_vouts) > 1 else list(filter(lambda out: out['value'] > 45, vouts))

        #if len(vouts) == 1:
            #miner_address = vouts[0]['scriptPubKey']['addresses'][0]

        #if len(coinbase_tx['vin']) == 1 and 'coinbase' in coinbase_tx['vin'][0]:
            #coinbase_data = unhexlify(coinbase_tx['vin'][0]['coinbase'])
            #if (coinbase_data[-1] == b'/' or coinbase_data[-1] == ord('/')) and len(coinbase_data.split(b'/')) > 2:
                #try:
                    #coinbase_pool_id = coinbase_data.split(b'/')[-2].decode('utf-8')
                #except:
                    #coinbase_pool_id = None
            #elif len(coinbase_data) <= 8:
                #coinbase_pool_id = SOLO
        #return coinbase_pool_id if not coinbase_pool_id in COINBASE_ID_BLACKLIST else None, miner_address
