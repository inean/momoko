# -*- coding: utf-8 -*-
"""
    momoko.clients
    ~~~~~~~~~~~~~~

    This module contains clients (blocking, non-blocking/async and adisp).

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

from .pools import ConnectionPool
from .utils import BatchQuery, QueryChain, TransactionChain


class AsyncClient(object):
    """The ``AsyncClient`` class is a wrapper for ``AsyncPool``, ``BatchQuery``
     ``TransactionChain'' and ``QueryChain``. It also provides the ``execute``
     and ``callproc`` functions.

    """

    def __init__(self, *args, **kwargs):
        self._pool = ConnectionPool(*args, **kwargs)

    @property
    def pool(self):
        """Get Client connection Pool"""
        return self._pool

    def get_connection(self, callback, callback_args=[]):
        self._pool.get_connection(callback, callback_args)

    def batch(self, queries, callback=None):
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

        :param queries: A dictionary with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished. Optional.
        :return: A dictionary with the same keys as the given queries with the
                 resulting cursors as values.

        .. _connection.cursor:
        http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        return BatchQuery(self, queries, callback)

    def transaction(self, statements, callback=None):
        """Run a chain of statements in the given order using a single
        connection.  The statements will be wrapped between a "begin;"
        and a "commit;". The connection will be unavailable while the
        chain is running.

        A list/tuple with statements looks like this::

            (
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            )

        A statement with parameters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'parameters here')]``. A statement
        without parameters doesn't need to be in a list.

        :param statements: A tuple or list with all the statements.
        :param callback: The function that needs to be executed once all the
                         queries are finished. Optional.
        :return: A list with the resulting cursors.

        .. _connection.cursor:
        http://initd.org/psycopg/docs/connection.html#connection.cursor

        """
        return TransactionChain(self, statements, callback)

    def chain(self, queries, callback=None):
        """Run a chain of queries in the given order.

        A list/tuple with queries looks like this::

            (
                ['SELECT 42, 12, %s, 11;', (23,)],
                'SELECT 1, 2, 3, 4, 5;'
            )

        A query with parameters is contained in a list: ``['some sql
        here %s, %s', ('and some', 'parameters here')]``. A query
        without parameters doesn't need to be in a list.

        :param queries: A tuple or list with all the queries.
        :param callback: The function that needs to be executed once all the
                         queries are finished. Optional.
        :return: A list with the resulting cursors.

        .. _connection.cursor:
        http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        return QueryChain(self, queries, callback)

    def execute(self, operation, parameters=(), cursor_factory=None,
                callback=None, connection = None):
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation. Variables are specified either with
        positional (``%s``) or named (``%(name)s``) placeholders. See Passing
        parameters to SQL queries `[1]`_ in the Psycopg2 documentation.

        .. _[1]: http://initd.org/psycopg/docs/usage.html#query-parameters

        :param operation: The database operation (an SQL query or command).
        :param parameters: A tuple, list or dictionary with parameters. This is
                           an empty tuple by default.
        :param cursor_factory: A valid factory to Psycopg's  cursor
        :param callback: A callable that is executed once the operation is
                         finished. Optional.

        .. _connection.cursor:
        http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        if connection:
            self._pool.new_cursor('execute', (operation, parameters),
                                  cursor_factory, callback, True, connection)
        else:
            self._pool.new_cursor('execute', (operation, parameters),
                                  cursor_factory, callback, False)

    def callproc(self, procname, parameters=None, cursor_factory=None,
                 callback=None, connection=None):
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each
        argument that the procedure expects. The result of the call is
        returned as modified copy of the input sequence. Input
        parameters are left untouched, output and input/output
        parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then
        be made available through the standard ``fetch*()`` methods.

        :param procname: The name of the procedure.
        :param parameters: A sequence with parameters. This is
        ``None`` by default.
        :param cursor_factory: A valid factory to Psycopg's  cursor
        :param callback: A callable that is executed once the procedure is
                         finished. Optional.

        .. _connection.cursor:
        http://initd.org/psycopg/docs/connection.html#connection.cursor

        """
        if connection:
            self._pool.new_cursor('callproc', (procname, parameters),
                                  cursor_factory, callback, True, connection)
        else:
            self._pool.new_cursor('callproc', (procname, parameters),
                                  cursor_factory, callback, False)

    def close(self):
        """Close all connections in the connection pool.
        """
        self._pool.close()
