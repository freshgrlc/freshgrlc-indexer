
#
#   Hardcoded coin info, expand at will
#

GRLC = {
    'name':                 'Garlicoin',
    'ticker':               'GRLC',
    'bech32_prefix':        'grlc',
    'address_version':      38,
    'p2sh_address_version': 50,
    'privkey_version':      176,
    'segwit_info': {
        'addresstype':      'base58',
        'address_version':  73,
        'receive_only':     True
    },
}


TGRLC = {
    'name':                 'Garlicoin Testnet',
    'ticker':               'tGRLC',
    'bech32_prefix':        None,
    'address_version':      111,
    'p2sh_address_version': 58,
    'privkey_version':      239,
    'segwit_info':          None
}


TUX = {
    'name':                 'Tuxcoin',
    'ticker':               'TUX',
    'address_prefix':       'tux',
    'address_version':      65,
    'p2sh_address_version': 64,
    'privkey_version':      193,
    'segwit_info': {
        'addresstype':      'bech32',
        'address_prefix':   'tux',
        'receive_only':     False
    },
    'allow_tx_subsidy':     False
}


COINS = [ GRLC, TGRLC, TUX ]


def get_by_filter(value, filter_func):
    filtered = list(filter(filter_func, COINS))
    return filtered[0] if len(filtered) > 0 else None


def by_name(name):
    return get_by_filter(name, lambda coin: coin['name'].lower() == name.lower())


def by_ticker(ticker):
    return get_by_filter(ticker, lambda coin: coin['ticker'].lower() == ticker.lower())


def by_address_versions(p2pkh_address_version, p2sh_address_version):
    return get_by_filter(
        '{ p2pkh address version: %d, p2sh address version: %d }' % (p2pkh_address_version, p2sh_address_version),
        lambda coin: coin['address_version'] == p2pkh_address_version and coin['p2sh_address_version'] == p2sh_address_version
    )
