# -*- coding: utf-8 -*-
"""
    momoko.pools
    ~~~~~~~~~~~~

    This module contains all the connection pools.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""

import time
import select
import logging as log

import psycopg2
from psycopg2 import DatabaseError, InterfaceError
from psycopg2.extensions import STATUS_READY

from functools import partial
from tornado.ioloop import IOLoop, PeriodicCallback


class ConnectionPool(object):
    """
    Asynchronous connection pool acting as a single connection.

    `dsn` and `connection_factory` are passed to `momoko.connection.Connection`
    when a new connection is created. It also contains the documentation about
    these two parameters.

    - **minconn** --
        Amount of connection created upon initialization.
    - **maxconn** --
        Maximum amount of connections supported by the pool.
    - **cleanup_timeout** --
        Time in seconds between pool cleanups. Unused connections
        are closed and removed from the pool until only `minconn` are left. When
        an integer below `1` is used the pool cleaner will be disabled.
    - **ioloop** --
        An instance of Tornado's IOLoop.
    """
    def __init__(self, dsn, connection_factory=None, minconn=1, maxconn=20,
                 cleanup_timeout=10, ioloop=None):
        self._dsn = dsn
        self._minconn = minconn
        self._maxconn = maxconn
        self._connection_factory = connection_factory
        self._ioloop = ioloop or IOLoop.instance()
        self._last_reconnect = 0
        self._pool = []

        self.closed = False

        #for i in range(self._minconn):
        for _ in range(self._minconn):
            self._new_conn()
        self._last_reconnect = time.time()

        # Create a periodic callback that tries to close inactive connections
        if cleanup_timeout > 0:
            self._cleaner = PeriodicCallback(self._clean_pool,
                cleanup_timeout * 1000)
            self._cleaner.start()

    def _new_conn(self, callback=None, callback_args=[]):
        """Create a new connection.

        :param callback_args: Parameters for the callback - connection will be appended
        to the parameters
        """
        if len(self._pool) > self._maxconn:
            self._clean_pool()
        if len(self._pool) > self._maxconn:
            raise PoolError('connection pool exhausted')
        timeout = self._last_reconnect + .25 # 1/4 second delay between reconnection
        timenow = time.time()
        if timenow > timeout or len(self._pool) <= self._minconn:
            self._last_reconnect = timenow
            conn = AsyncConnection(self._ioloop)
            callbacks = [partial(lambda conn, _: self._pool.append, conn)] # add new connection to the pool
            if callback:
                callbacks.append(partial(callback, *(callback_args+[conn])))

            conn.open(self._dsn, self._connection_factory, callbacks)
        else:
            # recursive timeout call, retaining the parameters
            self._ioloop.add_timeout(timeout, partial(self._new_conn, callback, callback_args))

    def _get_free_conn(self):
        """Look for a free connection and return it.

        `None` is returned when no free connection can be found.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.isexecuting():
                return conn
        return None

    def get_connection(self, callback = None, callback_args=[]):
        """Get a connection, trying available ones, and if not available - create a new one;

        Afterwards, the callback will be called
        """
        connection = self._get_free_conn()
        if connection is None:
            self._new_conn(callback,callback_args)
        else:
            callback(*(callback_args+[connection]))


    def new_cursor(self, function, function_args=(), cursor_factory=None, callback=None, connection=None, transaction=False):
        """Create a new cursor.

        If there's no connection available, a new connection will be created and
        `new_cursor` will be called again after the connection has been made.

        :param function: ``execute``, ``executemany`` or ``callproc``.
        :param function_args: A tuple with the arguments for the specified function.
        :param callback: A callable that is executed once the operation is done.
        :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.
        :param connection: An ``AsyncConnection`` connection. Optional.

        .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
        """

        cursor_kwargs = {}
        if cursor_factory is not None:
            cursor_kwargs["cursor_factory"] = cursor_factory
            
        if connection is not None:
            try:
                connection.cursor(function, function_args, callback, cursor_kwargs)
                return
            except (DatabaseError, InterfaceError):  # Recover from lost connection
                log.warning('Requested connection was closed')
                self._pool.remove(connection)

        # if no connection, or if exception caught
        if not transaction:
            self.get_connection(callback=self.new_cursor, callback_args=[function, function_args, cursor_factory, callback])
        else:
            raise TransactionError


    def _clean_pool(self):
        """Close a number of inactive connections when the number of connections
        in the pool exceeds the number in `min_conn`.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        if len(self._pool) > self._minconn:
            conns = len(self._pool) - self._minconn
            for conn in self._pool[:]:
                if not conn.isexecuting():
                    conn.close()
                    conns -= 1
                    self._pool.remove(conn)
                    if not conns:
                        break

    def close(self):
        """Close all open connections in the pool.
        """
        if self.closed:
            raise PoolError('connection pool is closed')
        for conn in self._pool:
            if not conn.closed:
                conn.close()
        self._cleaner.stop()
        self._pool = []
        self.closed = True


class PoolError(Exception):
    pass

class TransactionError(Exception):
    pass

class AsyncConnection(object):
    """An asynchronous connection object.

    :param ioloop: An instance of Tornado's IOLoop.
    """
    def __init__(self, ioloop=None):
        self._conn = None
        self._fileno = -1
        self._ioloop = ioloop or IOLoop.getInstance()
        self._callbacks = []

    def cursor(self, function, function_args, callback, cursor_kwargs={}):
        """Get a cursor and execute the requested function

        :param function: ``execute``, ``executemany`` or ``callproc``.
        :param function_args: A tuple with the arguments for the specified function.
        :param callback: A callable that is executed once the operation is done.
        :param cursor_kwargs: A dictionary with Psycopg's `connection.cursor`_ arguments.

        .. _connection.cursor: http://initd.org/psycopg/docs/connection.html#connection.cursor
        """
        cursor = self._conn.cursor(**cursor_kwargs)
        getattr(cursor, function)(*function_args)
        self._callbacks = [partial(callback, cursor)]

        # Connection state should be 1 (write)
        self._ioloop.update_handler(self._fileno, IOLoop.READ)

    def _io_callback(self, fd, events):
        try:
            error = None
            state = self._conn.poll()
        except (psycopg2.Warning, psycopg2.Error) as err:
            error = err
            state = psycopg2.extensions.POLL_OK
        if state == psycopg2.extensions.POLL_OK:
            for callback in self._callbacks:
                callback(error)
        elif state == psycopg2.extensions.POLL_READ:
            self._ioloop.update_handler(self._fileno, IOLoop.READ)
        elif state == psycopg2.extensions.POLL_WRITE:
            self._ioloop.update_handler(self._fileno, IOLoop.WRITE)

    def open(self, dsn, connection_factory=None, callbacks=[]):
        """
        Open an asynchronous connection.

        - **dsn** --
            A [Data Source Name][1] string containing one of the collowing values:

            + **dbname** - the database name
            + **user** - user name used to authenticate
            + **password** - password used to authenticate
            + **host** - database host address (defaults to UNIX socket if not provided)
            + **port** - connection port number (defaults to 5432 if not provided)

            Or any other parameter supported by PostgreSQL. See the PostgreSQL
            documentation for a complete list of supported [parameters][2].

        - **connection_factory** --
            The `connection_factory` argument can be used to create non-standard
            connections. The class returned should be a subclass of
            [psycopg2.extensions.connection][3].

        - **callbacks** --
            Sequence of callables. These are executed after the connection has
            been established.

        [1]: http://en.wikipedia.org/wiki/Data_Source_Name
        [2]: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS
        [3]: http://initd.org/psycopg/docs/connection.html#connection
        """
        args = []
        if not connection_factory is None:
            args.append(connection_factory)
        self._conn = psycopg2.connect(dsn, *args, async=1)

        self._transaction_status = self._conn.get_transaction_status
        self._fileno = self._conn.fileno()
        self._callbacks = callbacks

        # Set connection state
        self._ioloop.add_handler(self._fileno, self._io_callback, IOLoop.WRITE)

    def _wait(self, timeout):
        assert self._conn
        while 1:
            state = self._conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                retval = select.select([], [self._conn.fileno()], [], timeout)
            elif state == psycopg2.extensions.POLL_READ:
                retval = select.select([self._conn.fileno()], [], [], timeout)
            if retval[0] == retval[1]:
                raise psycopg2.OperationalError("timeout for connection")
 
    def close(self):
        """Close connection.
        """
        self._ioloop.remove_handler(self._fileno)
        return self._conn.close()

    @property
    def closed(self):
        """Read-only attribute reporting whether the database connection is
        open (0) or closed (1).
        """
        return self._conn.closed

    def isexecuting(self):
        """Return True if the connection is executing an asynchronous operation.
        """
        return self._conn.isexecuting()
