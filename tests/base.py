# -*- coding: utf-8 -*-

from __future__ import print_function

import logging
import os
from contextlib import contextmanager
from functools import wraps
from unittest import TestCase

from peewee import Model, SqliteDatabase
from peewee.core import AliasMap, QueryCompiler, SelectQuery, logger

TEST_BACKEND = os.environ.get('PEEWEE_TEST_BACKEND') or 'sqlite'
TEST_DATABASE = os.environ.get('PEEWEE_TEST_DATABASE') or 'peewee_test'
TEST_VERBOSITY = int(os.environ.get('PEEWEE_TEST_VERBOSITY') or 1)

if TEST_VERBOSITY > 1:
    handler = logging.StreamHandler()
    handler.setLevel(logging.ERROR)
    logger.addHandler(handler)


class DatabaseInitializer(object):
    def __init__(self, backend, database_name):
        self.backend = self.normalize(backend)
        self.database_name = database_name

    def normalize(self, backend):
        backend = backend.lower().strip()
        mapping = {'sqlite': ('sqlite3', 'pysqlite')}
        for key, alias_list in mapping.items():
            for db_alias in alias_list:
                if backend == db_alias:
                    return key
        return backend

    def get_database_class(self, backend=None):
        mapping = {'sqlite': SqliteDatabase}

        backend = backend or self.backend
        try:
            return mapping[backend]
        except KeyError:
            print('Unrecognized database: "{0!s}".'.format(backend))
            print('Available choices:\n{0!s}'.format('\n'.join(
                sorted(mapping.keys()))))
            raise

    def get_database(self, backend=None, db_class=None, **kwargs):
        backend = backend or self.backend
        method = 'get_{0!s}_database'.format(backend)
        kwargs.setdefault('use_speedups', False)

        if db_class is None:
            db_class = self.get_database_class(backend)

        if not hasattr(self, method):
            return db_class(self.database_name, **kwargs)
        else:
            return getattr(self, method)(db_class, **kwargs)

    def get_filename(self, extension):
        return os.path.join('/tmp',
                            '{0!s}{1!s}'.format(self.database_name, extension))

    def get_apsw_database(self, db_class, **kwargs):
        return db_class(self.get_filename('.db'), timeout=1000, **kwargs)

    def get_berkeleydb_database(self, db_class, **kwargs):
        return db_class(self.get_filename('.bdb.db'), timeout=1000, **kwargs)

    def get_sqlcipher_database(self, db_class, **kwargs):
        passphrase = kwargs.pop('passphrase', 'snakeoilpassphrase')
        return db_class(self.get_filename('.cipher.db'),
                        passphrase=passphrase, **kwargs)

    def get_sqlite_database(self, db_class, **kwargs):
        return db_class(self.get_filename('.db'), **kwargs)

    def get_in_memory_database(self, db_class=None, **kwargs):
        kwargs.setdefault('use_speedups', False)
        db_class = db_class or SqliteDatabase
        return db_class(':memory:', **kwargs)


class TestAliasMap(AliasMap):
    def add(self, obj, alias=None):
        if isinstance(obj, SelectQuery):
            self._alias_map[obj] = obj._alias
        else:
            self._alias_map[obj] = obj._meta.db_table


class TestQueryCompiler(QueryCompiler):
    alias_map_class = TestAliasMap


class TestDatabase(SqliteDatabase):
    compiler_class = TestQueryCompiler
    field_overrides = {}
    interpolation = '?'
    op_overrides = {}
    quote_char = '"'

    def execute_sql(self, sql, params=None, require_commit=True):
        try:
            return super(TestDatabase, self).execute_sql(
                sql, params, require_commit)
        except Exception:
            self.last_error = (sql, params)
            raise


class QueryLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.queries = []
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.queries.append(record)


database_initializer = DatabaseInitializer(TEST_BACKEND, TEST_DATABASE)

database_class = database_initializer.get_database_class()
test_db = database_initializer.get_database()
query_db = TestDatabase(':memory:')

compiler = query_db.compiler()
normal_compiler = QueryCompiler('"', '?', {}, {})


class TestModel(Model):
    class Meta:
        database = test_db


class PeeweeTestCase(TestCase):
    def setUp(self):
        self.qh = QueryLogHandler()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self.qh)

    def tearDown(self):
        logger.removeHandler(self.qh)

    def assertIsNone(self, value):
        assert value is None, '{0!r} is not None'.format(value)

    def assertIsNotNone(self, value):
        assert value is not None

    @contextmanager
    def assertRaisesCtx(self, exc_class):
        try:
            yield
        except exc_class:
            return
        else:
            raise AssertionError(
                'Exception {0!s} not raised.'.format(exc_class))

    def queries(self, ignore_txn=False):
        queries = [x.msg for x in self.qh.queries]
        if ignore_txn:
            skips = ('BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE')
            queries = [q for q in queries if not q[0].startswith(skips)]
        return queries

    @contextmanager
    def assertQueryCount(self, num, ignore_txn=False):
        qc = len(self.queries(ignore_txn=ignore_txn))
        yield
        assert len(self.queries(ignore_txn=ignore_txn)) - qc == num

    def log_queries(self):
        return QueryLogger(self)

    def parse_node(self, query, expr_list, compiler=compiler):
        am = compiler.calculate_alias_map(query)
        return compiler.parse_node_list(expr_list, am)

    def parse_query(self, query, node, compiler=compiler):
        am = compiler.calculate_alias_map(query)
        if node is not None:
            return compiler.parse_node(node, am)
        return '', []

    def make_fn(fn_name, attr_name):
        def inner(self, query, expected, expected_params, compiler=compiler):
            fn = getattr(self, fn_name)
            att = getattr(query, attr_name)
            sql, params = fn(query, att, compiler=compiler)
            assert sql == expected
            assert params == expected_params

        return inner

    assertSelect = make_fn('parse_node', '_select')
    assertWhere = make_fn('parse_query', '_where')
    assertGroupBy = make_fn('parse_node', '_group_by')
    assertHaving = make_fn('parse_query', '_having')
    assertOrderBy = make_fn('parse_node', '_order_by')

    def assertJoins(self, sq, exp_joins, compiler=compiler):
        am = compiler.calculate_alias_map(sq)
        clauses = compiler.generate_joins(sq._joins, sq.model_class, am)
        joins = [compiler.parse_node(clause, am)[0] for clause in clauses]
        assert sorted(joins) == sorted(exp_joins)

    def new_connection(self):
        return database_initializer.get_database()


class ModelTestCase(PeeweeTestCase):
    requires = None

    def setUp(self):
        super(ModelTestCase, self).setUp()
        if self.requires:
            test_db.drop_tables(self.requires, True)
            test_db.create_tables(self.requires)

    def tearDown(self):
        super(ModelTestCase, self).tearDown()
        if self.requires:
            test_db.drop_tables(self.requires, True)


# TestCase class decorators that allow skipping entire test-cases.

def skip_if(expression):
    def decorator(klass):
        if expression():
            if TEST_VERBOSITY > 0:
                print('Skipping {0!s} tests.'.format(klass.__name__))

            class Dummy(object):
                pass

            return Dummy
        return klass

    return decorator


def skip_unless(expression):
    return skip_if(lambda: not expression())


# TestCase method decorators that allow skipping single test methods.

def skip_test_if(expression):
    def decorator(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            if expression():
                if TEST_VERBOSITY > 1:
                    print('Skipping {0!s} test.'.format(fn.__name__))
            else:
                return fn(*args, **kwargs)

        return inner

    return decorator


def skip_test_unless(expression):
    return skip_test_if(lambda: not expression())


def log_console(s):
    if TEST_VERBOSITY > 1:
        print(s)


class QueryLogger(object):
    def __init__(self, test_case):
        self.test_case = test_case
        self.queries = []

    def __enter__(self):
        self._initial_query_count = len(self.test_case.queries())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        all_queries = self.test_case.queries()
        self._final_query_count = len(all_queries)
        self.queries = (
            all_queries[self._initial_query_count:self._final_query_count])
