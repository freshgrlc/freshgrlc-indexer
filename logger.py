from datetime import datetime


def log(s):
    time = '%s' % datetime.now()
    for line in s.split('\n'):
        print('%s: %s' % (time, line))


def log_event(event, objtype, obj, params=None):
    if params is not None:
        if type(params) == dict:
            params = ', '.join([
                ('%s: %s' % (k, v) if type(v) != bool else k)
                for (k, v) in filter(lambda e: e[0] is not None and (type(e[1]) != bool or e[1] == True), {
                    k.replace('_', ' '): v
                    for (k, v) in params.items()
                }.items())
            ])
    params = ' (%s)' % params if params is not None else ''
    log('%-7s %-4s %s%s' % (event, objtype, obj, params))


def log_tx_event(tx, event, **kwargs):
    log_event(event, 'tx', tx, kwargs if kwargs != {} else None)


def log_block_event(block, event, **kwargs):
    log_event(event, 'blk', block, kwargs if kwargs != {} else None)


def log_balance_event(address, event, **kwargs):
    log_event(event, 'bal', address, kwargs if kwargs != {} else None)

