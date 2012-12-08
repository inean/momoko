# -*- coding: utf-8 -*-
"""
    momoko.utils
    ~~~~~~~~~~~~

    Utilities for Momoko.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

import psycopg2
import psycopg2.extensions

from functools import partial
from tornado.ioloop import IOLoop
from itertools import izip, islice, chain


class CollectionMixin(object):

    methods = ('execute','callproc',)

    def __init__(self, db, callback):
        self._db = db
        self._callback = callback

    def _method(self, query):
        method = query.pop(0) if query[0] in self.methods else 'execute'
        return getattr(self._db, method)
        
    @staticmethod
    def _cursor_factory(query):
        retval = query.pop(-1) if type(query[-1]) == type else None
        return retval

    def _collect(self, cursor, error):
        raise NotImplementedError

        
class QueryChain(CollectionMixin):
    """Run a chain of queries in the given order.

    A list/tuple with queries looks like this::

        (
            [query, 'SELECT 42, 12, %s, 11;', (23,), factory)],
             'SELECT 1, 2, 3, 4, 5;'
        )

    A query with parameters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'parameters here')]``. A query
    without parameters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A list with the resulting cursors is passed on to the callback.

    .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
    """
    def __init__(self, db, queries, callback):
        super(QueryChain, self).__init__(db, callback)
        self._cursors = []
        self._errors  = []
        self._queries = list(queries)
        self._queries.reverse()
        self._collect(None, None)

    def _collect(self, cursor, error):
        if cursor is not None:
            self._cursors.append(cursor)
            self._errors.append(error)
        if not self._queries:
            if self._callback:
                self._callback(self._cursors, self._errors)
            return
        query = self._queries.pop()
        if isinstance(query, basestring):
            query = [query]
        factory = self._cursor_factory(query)
        self._method(query)(*query, cursor_factory=factory, callback=self._collect)


class BatchQuery(CollectionMixin):
    """Run a batch of queries all at once.

    **Note:** Every query needs a free connection. So if three queries are
    are executed, three free connections are used.

    A dictionary with queries looks like this::

        {
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'
        }

    A query with paramaters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'paramaters here')]``. A query
    without paramaters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A dictionary with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A dictionary with the same keys as the given queries with the
             resulting cursors as values is passed on to the callback.
    """
    def __init__(self, db, queries, callback):
        super(BatchQuery, self).__init__(db, callback)
        self._queries = {}
        self._args = {}
        self._size = len(queries)

        for key, query in list(queries.items()):
            if isinstance(query, basestring):
                query = [query, ()]
            factory   = self._cursor_factory(query)
            callback  = partial(self._collect, key)
            self._queries[key] = (query, factory, callback)
            
        for query, factory, callback in list(self._queries.values()):
            self._method(query)(*query, cursor_factory=factory, callback=callback)

    def _collect(self, key, cursor, error):
        self._size -= 1
        self._args[key] = (cursor, error,)
        if not self._size and self._callback:
            cursors = dict(
                izip(self._args.iterkeys(),
                     islice(chain(*self._args.itervalues()), 0, None, 2))
            )
            errors  = dict(
                izip(self._args.iterkeys(),
                     islice(chain(*self._args.itervalues()), 1, None, 2))
            )
            self._callback(cursors, errors)

            
class TransactionChain(CollectionMixin):
    """Run queries as a transaction

    A list/tuple with queries looks like this::

        (
            ['SELECT 42, 12, %s, 11;', (23,), factory],
            'SELECT 1, 2, 3, 4, 5;'
        )

    A query with parameters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'parameters here')]``. A query
    without parameters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param statements: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A list with the resulting cursors is passed on to the callback.

    .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
    """
    def __init__(self, db, statements, callback):
        super(TransactionChain, self).__init__(db, callback)
        self._cursors = []
        self._errors  = []
        self._statements = list(statements)
        self._statements.reverse()
        self._db._pool.get_connection(self._set_connection)

    def _set_connection(self, conn, _=None):
        self._connection = conn
        try:
            # don't let other connections mess up the transaction
            self._db._pool._pool.remove(conn)
        except ValueError:
            pass
        self._collect(None, None)

    def _collect(self, cursor, error):
        if cursor is not None:
            self._cursors.append(cursor)
            self._errors.append(error)
        if not self._statements:
            if self._callback:
                self._callback(self._cursors, self._errors)
            self._db._pool._pool.append(self._connection)
            return
        statement = self._statements.pop()
        if isinstance(statement, basestring):
            statement = [statement]
        factory = self._cursor_factory(statement)
        self._method(statement)(*statement,
                                cursor_factory=factory,
                                callback=self._collect,
                                connection=self._connection)


