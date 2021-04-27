import json

from binascii import hexlify
from datetime import datetime
from decimal import Decimal
from flask import request, Response
from inspect import isclass
from sqlalchemy.orm.collections import InstrumentedDict, InstrumentedList

from config import Configuration
from models import *


LIST_LIKE_TYPES = (list, InstrumentedList)
DICT_LIKE_TYPES = (dict, InstrumentedDict)


def convert_date(date):
    if date != None:
        return int((date - datetime(1970, 1, 1)).total_seconds())


def json_preprocess_value(k, v, cls):
    if v == None:
        return None
    if type(v) in LIST_LIKE_TYPES:
        return [ json_preprocess_value(None, e, None) for e in v ]
    if type(v) in DICT_LIKE_TYPES:
        return json_preprocess_dict(v)

    if isclass(type(v)) and hasattr(v, 'API_DATA_FIELDS'):
        return json_preprocess_dbobject(v)

    try:
        datatype = type(getattr(cls, k).type)
    except (AttributeError, TypeError):
        datatype = type(v)

    if datatype == Binary:
        return hexlify(v)
    if datatype == DateTime or datatype == datetime:
        return convert_date(v)
    if datatype == Float or datatype == Decimal:
        return float(v)
    return v


def json_preprocess_dict(d):
    return {k: json_preprocess_value(k, v, None) for k, v in d.items()}


def substitute_contextinfo(template, context):
    parts = template.split('<')
    result = parts.pop(0)

    while len(parts) > 0:
        key, template_part = tuple(parts.pop(0).split('>'))
        obj, key = tuple(key.split('.', 1))
        if obj[0:6] == 'query:':
            obj = context[obj]
            while '.' in key:
                refname, key = key.split('.', 1)
                obj = getattr(obj, refname)
            result += json_preprocess_value(key, getattr(obj, key), obj.__class__) + template_part
        else:
            result += context[obj][key] + template_part

    return result


def json_preprocess_dbobject(obj, resolve_foreignkeys=None, whitelist=None, reflinks={}, context={}):
    if obj == None:
        return

    try:
        my_foreignkeys = filter(lambda fk: str(fk).split('.')[0] == obj.__class__.__name__, (resolve_foreignkeys or obj.__class__.POSTPROCESS_RESOLVE_FOREIGN_KEYS))
    except AttributeError:
        my_foreignkeys = []

    try:
        my_whitelist = [
            n[1]
            for n in filter(lambda n: n[0].split('.')[0] in [obj.__class__.__name__, obj.__class__.__tablename__], [
                (str(col), col.name if type(col) != str else col.split('.')[-1])
                for col in (whitelist or obj.__class__.API_DATA_FIELDS)
            ])
        ]
    except AttributeError:
        my_whitelist = obj.__dict__.keys()

    converted = {colname: json_preprocess_value(colname, obj.__getattribute__(colname), obj.__class__) for colname in my_whitelist}

    my_context = context.copy()
    my_context[obj.__class__.__name__] = converted
    my_context[obj.__class__.__tablename__] = converted
    my_context['query:' + obj.__class__.__tablename__] = obj

    for foreignkey in my_foreignkeys:
        colname = str(foreignkey).split('.')[-1]
        try:
            refid = getattr(obj, colname + '_id')
        except AttributeError:
            refid = -1

        if refid != None:
            if colname in reflinks.keys():
                converted[colname] = {'href': substitute_contextinfo(reflinks[colname][0], my_context)}
                if reflinks[colname][1] is not None and len(reflinks[colname][1]) > 0:
                    ref = getattr(obj, colname)
                    for inline_resolve_key in reflinks[colname][1]:
                        converted[colname][inline_resolve_key] = json_preprocess_value(inline_resolve_key, getattr(ref, inline_resolve_key), ref.__class__)
            else:
                refs = getattr(obj, colname)
                if isinstance(refs, list):
                    converted[colname] = [
                        json_preprocess_dbobject(ref, resolve_foreignkeys=resolve_foreignkeys, whitelist=whitelist, reflinks=reflinks, context=my_context)
                        for ref in refs
                    ]
                elif isinstance(refs, dict):
                    converted[colname] = json_preprocess_dict(refs)
                else:
                    converted[colname] = json_preprocess_dbobject(refs, resolve_foreignkeys=resolve_foreignkeys, whitelist=whitelist, reflinks=reflinks, context=my_context)
        else:
            converted[colname] = None

    return converted


class QueryDataPostProcessor(Configuration):
    DEFAULT_OBJECTS_PER_PAGE = 20
    MAX_OBJECTS_PER_PAGE = 1000

    class ProcessedData(object):
        def __init__(self, data):
            self.data = data

        def json(self):
            return Response(json.dumps(self.data), mimetype='application/json')

        def __getitem__(self, key):
            return QueryDataPostProcessor.ProcessedData(self.data[key])

    def __init__(self):
        self.filter_keys = None
        self.resolve_foreignkeys = None
        self.start = None
        self.limit = None
        self.end = None
        self._baseurl = None
        self._reflinks = {}
        self._expansion_requested = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @property
    def expansion_requested(self):
        if self._expansion_requested is None:
            self._expansion_requested = filter(lambda key: key != 'none', (request.args.get('expand') or 'none').split(','))
        return self._expansion_requested

    def pagination(self, backwards_indexes=False, tipresolver=None, allow_interval=False, default_limit=None):
        if default_limit is None:
            default_limit = self.DEFAULT_OBJECTS_PER_PAGE
        start = int(request.args.get('start') or (-default_limit if backwards_indexes else 0))
        limit = int(request.args.get('limit') or default_limit)
        interval = int(request.args.get('interval')) if allow_interval and request.args.get('interval') else None
        if interval <= 0:
            interval = None

        if limit <= 0 or limit > (self.MAX_OBJECTS_PER_PAGE if interval is None else self.MAX_OBJECTS_PER_PAGE * interval):
            limit = self.MAX_OBJECTS_PER_PAGE

        if start < 0:
            if backwards_indexes:
                start = tipresolver() + start
            else:
                start = 0
                limit = 0

        self.start = start
        self.limit = limit
        self.end = start + limit
        self.interval = interval
        return self

    def filter(self, *args):
        self.filter_keys = args
        return self

    def resolve_keys(self, *args):
        self.resolve_foreignkeys = args
        return self

    def baseurl(self, url):
        self._baseurl = url
        return self

    def reflink(self, key, template, inline_resolve=[]):
        self._reflinks[key] = (self.API_ENDPOINT + template, inline_resolve)
        return self

    def reflinks(self, *keys):
        for key in keys:
            self.reflink(key, self._baseurl + key + '/')
        return self

    def get_reflink_object(self, template):
        return {'href': self.API_ENDPOINT + template}

    def get_reflink_object_or_data(self, template, name, data_cb):
        return self.get_reflink_object(template) if name not in self.expansion_requested else data_cb()

    def autoexpand(self):
        self._reflinks = dict(filter(lambda pair: not pair[0] in self.expansion_requested, self._reflinks.items())) if '*' not in self.expansion_requested else {}
        return self

    def _process(self, data):
        return json_preprocess_dbobject(data, resolve_foreignkeys=self.resolve_foreignkeys, whitelist=self.filter_keys, reflinks=self._reflinks)

    def process(self, data):
        if type(data) in LIST_LIKE_TYPES:
            return self.ProcessedData([self._process(obj) for obj in data])
        if type(data) in DICT_LIKE_TYPES:
            return self.ProcessedData({ k: self._process(obj) for k, obj in data.items() })
        return self.ProcessedData(self._process(data))

    def process_raw(self, data):
        if type(data) in LIST_LIKE_TYPES:
            return self.ProcessedData([json_preprocess_dict(obj) for obj in data])
        elif type(data) in DICT_LIKE_TYPES:
            return self.ProcessedData(json_preprocess_dict(data))
        return self.ProcessedData(data)
