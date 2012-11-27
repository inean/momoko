# -*- coding: utf-8 -*-
"""
    momoko.orm
    ~~~~~~~~~~~~

    Simple Activerecord like pattern to make easy to handle
    common cases

    :copyright: (c) 2012 by Carlos Martin
    :license: MIT, see LICENSE for more details.
"""

import re
import json
import btlroute
import itertools
import collections
import contextlib

class Operation(object):
    _operation        = None
    _query            = None
    _field_regex      = { '__pkeys':  re.compile(r'pkey[\d]?$') }
    _field_params     = []
    _blacklist_params = []
    
    def __init__(self, params, fields, field_params = _field_params, blacklist_params = _blacklist_params):
        # Store key value pairs
        self._fields = {}
        # Remove params from param list when are claimed by fields
        for param in itertools.ifilter(lambda x: x in params, field_params):
            fields[param] = params[param]
        for param in list(itertools.ifilter(lambda x: x in params, blacklist_params)):
            del params[param]

        # Set special table field, if present. NO regex needed IMO
        if 'table' in fields:
            self._fields["__table"] = fields["table"]

        # process fields. After this loop, _fields attribute for an entry of:
        # { "pkey": "username", "username": "usal" }
        # {
        #    "__pkeys": ['__username'],
        #    "__pkey": "__username",
        #    "__username_value": "usal"
        # }
        #
        for key, value in fields.iteritems():
            for field, field_value in self._field_regex.iteritems():
                if field_value.match(key):
                    self._fields.setdefault(field, [])
                    self._fields["__" + key] = value
                    if value is not None and value in fields:
                        self._fields[field].append("__" + key)
                        self._fields["__" + key +'_value'] = fields[value]
                    break

        # update params
        self._params = params

    def __iter__(self):
        yield self.operation
        yield self.query
        yield tuple(self.params)

    @property
    def operation(self):
        return self._operation

    @property
    def query(self):
        
        args = dict(self._fields)
        # calcute field sugars
        for field in self._field_regex.iterkeys():
            fields = []
            for field_key in self._fields.get(field, []):
                field_value = field_key + "_value"
                if field_value in self._fields:
                    fields.append(self._fields[field_key] + "=" + self._fields[field_key + "_value"])
            if fields:
                args[field] = "AND ".join(fields)# if fields else None
                assert ";" not in args[field]
        # calculate sugar "__params"
        if self.params:
            args['__params'] = ",".join(itertools.repeat('(%s)=(%s)',  (len(self.params) / 2)))
        retval = self._query.format(**args) if args else self._query
        # sanity check
        assert ';' not in retval
        return retval

    @property
    #cached
    def params(self):
        return list(itertools.chain(*self._params.iteritems()))

    def invoke(self, db, callback):
        getattr(db, self.operation)([self.query, self.params], callback)

        
class Table(object):
    
    __table__            = None
    __field_args__       = {}
    __field_params__     = []
    __blacklist_params__ = []
    __slots__            = ('params',)
    
    def __init__(self, fields):
        self.fields = dict(self.__field_args__)
        self.fields["table"] = self.__table__
        self.fields.update(fields)
        
    def replace(self, params):
        return self.Replace(
            params, self.fields,
            field_params=self.__field_params__,
            blacklist_params=self.__blacklist_params__
        )
        

class OneToOne(Table):
    class Replace(Operation):
        _operation    = "execute"
        _query        = "UPDATE {__table} SET {__params} WHERE {__pkeys}"

class OneToMany(Table):
    class Replace(Operation):
        _operation    = "execute"
        _query        = "UPDATE {__table} SET {__params} WHERE {__pkey} = {__pkey_value} AND " \
                        "{__pkey0} = (SELECT {__subpkey} FROM {__subtable} WHERE {__subpkeys})"
        _field_regex  = {
            "__pkeys":    re.compile(r'pkey[\d]?$'),
            "__subpkeys": re.compile(r'subpkey[\d]?$'),
            "__subtable": re.compile(r'subtable$'),
        }
        
class Person(OneToOne):
    __table__      = "Persons"
    __field_args__ = { "pkey": 'username' }
    
class Directions(OneToMany):
    __table__            = "Directions"
    __field_args__       = {"pkey": "id", "pkey0": "thing_id", "subtable": "Persons", "subpkey" : "username" }
    # Params that shoud be used also as fields
    __field_params__     = ["id"]
    # Params that should be removed from param list
    __blacklist_params__ = ["id"]

    
# JSON Mapper Stuff
class InvalidRuleException(Exception):
    pass

class JSONPath(btlroute.Path):
    pass

class JSONContext(object):
    
    maps = {}
    
    def __contains__(self, value):
        return value in self.maps
        
    def add_map(self, name, mapper):
        assert name not in self.maps
        self.maps[name] = mapper

    @contextlib.contextmanager
    def get_map(self, key):
        yield self.maps[key]
        
    @classmethod
    def get_instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

JSONTask = collections.namedtuple(
    'JSONTask',
    ['operation', 'rule', 'params', 'kwargs']
)

JSONRule = collections.namedtuple(
    'JSONRule',
    ['path', 'model', 'stackable', 'hook']
)

class JSONMapper(object):

    def __init__(self):
        self.verbs = {}
        self.queue = {}
        
    def when_replace(self, path, model, stackable=False, hook=None):
        verb = self.verbs.setdefault('replace', [])
        verb.append(JSONRule(JSONPath(path), model, stackable, hook))
        
    def parse(self, patch, **kwargs):
        def create_task(operation, path, value):
            """Returns first matching rule for operation and path"""
            for route in self.verbs[operation]:
                try:
                    args = route.path.match(path)
                except btlroute.RouteNotFoundError, err:
                    continue
                if args is not None:
                    if callable(route.hook):
                        args = route.hook(args, value, **kwargs)
                    if 'field' in args:
                        args[args.pop('field')] = value
                    return JSONTask(operation, route, args, kwargs)
            raise InvalidRuleException("bad path " + path)

        def process_task(task):
            new = { task.rule.path.name: [task] }
            try:
                while(True):
                    name, pending = self.queue.popitem()
                    if name == task.rule.path.name and task.rule.stackable:
                        assert len(pending) == 1
                        pending[0].params.update(task.params)
                        new[name] = pending
                        continue
                    for value in pending:
                        yield getattr(value.rule.model(value.kwargs), operation)(value.params)
            except KeyError:
                # update old queue
                if not task.rule.stackable:
                    yield getattr(task.rule.model(task.kwargs), task.operation)(task.params)
                    new = {}
                self.queue = new
                raise StopIteration
            
        for op in json.loads(patch):
            operation, path, value = op['op'], op['path'], op['value']
            if operation not in self.verbs:
                raise InvalidMapperException("bad operation " + operation)

            # find a valid rule
            task = create_task(operation, path, value)
            for value in process_task(task):
                yield value
        # we should have a queue with 0 or 1 value
        if self.queue:
            _, tasks = self.queue.popitem()
            assert len(tasks) == 1
            assert not self.queue
            assert tasks[0].rule.stackable
            task = tasks[0]
            yield getattr(task.rule.model(task.kwargs), task.operation)(task.params)

        
if __name__ == "__main__":

    def persons_update_hook(params, value, **kwargs):
        if params['field'] == 'avatar' and value != None:
            raise NotImplementedError
        return params
            
    patch = [
        {'op':'replace', 'path':'/persons/age', 'value': 35},
        {'op':'replace', 'path':'/persons/avatar', 'value': None},
        {'op':'replace', 'path':'/persons/directions/4/street', 'value': "New value"},
    ]

    context = JSONContext().get_instance()
    if 'account_update' not in context:
        # register a map
        jmap = JSONMapper()
        jmap.when_replace("/persons/:field", Person, stackable=True, hook=persons_update_hook)
        jmap.when_replace("/persons/directions/:id/:value", Directions)
        context.add_map('account_update', jmap)

    with context.get_map('account_update') as jmap:
        for model in jmap.parse(json.dumps(patch), username='usal'):
            print list(model)
        
