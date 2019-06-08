from database import DatabaseSession


def import_address(address, dbsession, daemon, commit=False):
    db = DatabaseSession(dbsession, address_cache=None, txid_cache=None)
    script = daemon.validateaddress(address)['scriptPubKey']
    address_info = daemon.decodescript(script)
    id = db.get_or_create_output_address_id(address_info)

    if commit:
        db.session.commit()

    return id




