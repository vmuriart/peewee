# -*- coding: utf-8 -*-

import threading

import pytest

from peewee import CharField, IntegerField, Model, SqliteDatabase
from tests.base import (ModelTestCase, PeeweeTestCase, TestModel, compiler,
                        database_initializer, query_db, skip_unless, test_db)
from tests.models import (Blog, MultiIndexModel, SeqModelA,
                          SeqModelB, UniqueModel, User)

try:
    from Queue import Queue
except ImportError:
    from queue import Queue


class TestMultiThreadedQueries(ModelTestCase):
    requires = [User]
    threads = 4

    def setUp(self):
        self._orig_db = test_db
        kwargs = {}
        try:  # Some engines need the extra kwargs.
            kwargs.update(test_db.connect_kwargs)
        except Exception:
            pass
        if isinstance(test_db, SqliteDatabase):
            # Put a very large timeout in place to avoid `database is locked`
            # when using SQLite (default is 5).
            kwargs['timeout'] = 30

        User._meta.database = self.new_connection()
        super(TestMultiThreadedQueries, self).setUp()

    def tearDown(self):
        User._meta.database = self._orig_db
        super(TestMultiThreadedQueries, self).tearDown()

    def test_multiple_writers(self):
        def create_user_thread(low, hi):
            for i in range(low, hi):
                User.create(username='u{0:d}'.format(i))
            User._meta.database.close()

        threads = []

        for i in range(self.threads):
            threads.append(threading.Thread(target=create_user_thread,
                                            args=(i * 10, i * 10 + 10)))

        [t.start() for t in threads]
        [t.join() for t in threads]

        assert User.select().count() == self.threads * 10

    def test_multiple_readers(self):
        data_queue = Queue()

        def reader_thread(q, num):
            for i in range(num):
                data_queue.put(User.select().count())

        threads = []

        for i in range(self.threads):
            threads.append(threading.Thread(target=reader_thread,
                                            args=(data_queue, 20)))

        [t.start() for t in threads]
        [t.join() for t in threads]

        assert data_queue.qsize() == self.threads * 20


class TestDeferredDatabase(PeeweeTestCase):
    def test_deferred_database(self):
        deferred_db = SqliteDatabase(None)
        assert deferred_db.deferred

        class DeferredModel(Model):
            class Meta:
                database = deferred_db

        with pytest.raises(Exception):
            deferred_db.connect()
        sq = DeferredModel.select()
        with pytest.raises(Exception):
            sq.execute()

        deferred_db.init(':memory:')
        assert not deferred_db.deferred

        # connecting works
        deferred_db.connect()
        DeferredModel.create_table()
        sq = DeferredModel.select()
        assert list(sq) == []

        deferred_db.init(None)
        assert deferred_db.deferred


class TestSQLAll(PeeweeTestCase):
    def setUp(self):
        super(TestSQLAll, self).setUp()
        fake_db = SqliteDatabase(':memory:')
        UniqueModel._meta.database = fake_db
        SeqModelA._meta.database = fake_db
        MultiIndexModel._meta.database = fake_db

    def tearDown(self):
        super(TestSQLAll, self).tearDown()
        UniqueModel._meta.database = test_db
        SeqModelA._meta.database = test_db
        MultiIndexModel._meta.database = test_db

    def test_sqlall(self):
        sql = UniqueModel.sqlall()
        assert sql == [
            ('CREATE TABLE "uniquemodel" ("id" INTEGER NOT NULL PRIMARY KEY, '
             '"name" VARCHAR(255) NOT NULL)'),
            'CREATE UNIQUE INDEX "uniquemodel_name" ON "uniquemodel" ("name")']

        sql = MultiIndexModel.sqlall()
        assert sql == [
            ('CREATE TABLE "multiindexmodel" ("id" INTEGER NOT NULL PRIMARY '
             'KEY, "f1" VARCHAR(255) NOT NULL, "f2" VARCHAR(255) NOT NULL, '
             '"f3" VARCHAR(255) NOT NULL)'),
            ('CREATE UNIQUE INDEX "multiindexmodel_f1_f2" ON "multiindexmodel"'
             ' ("f1", "f2")'),
            ('CREATE INDEX "multiindexmodel_f2_f3" ON "multiindexmodel" '
             '("f2", "f3")')]

        sql = SeqModelA.sqlall()
        assert sql == [
            ('CREATE TABLE "seqmodela" ("id" INTEGER NOT NULL PRIMARY KEY '
             'DEFAULT NEXTVAL(\'just_testing_seq\'), "num" INTEGER NOT NULL)')]


class TestLongIndexName(PeeweeTestCase):
    def test_long_index(self):
        class LongIndexModel(TestModel):
            a123456789012345678901234567890 = CharField()
            b123456789012345678901234567890 = CharField()
            c123456789012345678901234567890 = CharField()

        fields = LongIndexModel._meta.sorted_fields[1:]
        assert len(fields) == 3

        sql, params = compiler.create_index(LongIndexModel, fields, False)
        assert sql == (
            'CREATE INDEX "longindexmodel_85c2f7db" '
            'ON "longindexmodel" ('
            '"a123456789012345678901234567890", '
            '"b123456789012345678901234567890", '
            '"c123456789012345678901234567890")')


class TestDroppingIndex(ModelTestCase):
    def test_drop_index(self):
        db = database_initializer.get_in_memory_database()

        class IndexedModel(Model):
            idx = CharField(index=True)
            uniq = CharField(unique=True)
            f1 = IntegerField()
            f2 = IntegerField()

            class Meta:
                database = db
                indexes = ((('f1', 'f2'), True),
                           (('idx', 'uniq'), False))

        IndexedModel.create_table()
        indexes = db.get_indexes(IndexedModel._meta.db_table)

        assert sorted(idx.name for idx in indexes) == [
            'indexedmodel_f1_f2',
            'indexedmodel_idx',
            'indexedmodel_idx_uniq',
            'indexedmodel_uniq']

        with self.log_queries() as query_log:
            IndexedModel._drop_indexes()

        assert sorted(query_log.queries) == sorted(
            [('DROP INDEX "{0!s}"'.format(idx.name), []) for idx in indexes])
        assert db.get_indexes(IndexedModel._meta.db_table) == []


class TestConnectionState(PeeweeTestCase):
    def test_connection_state(self):
        test_db.get_conn()
        assert not test_db.is_closed()
        test_db.close()
        assert test_db.is_closed()
        test_db.get_conn()
        assert not test_db.is_closed()

    def test_sql_error(self):
        bad_sql = 'select asdf from -1;'
        with pytest.raises(Exception):
            query_db.execute_sql(bad_sql)
        assert query_db.last_error == (bad_sql, None)


@skip_unless(lambda: test_db.drop_cascade)
class TestDropTableCascade(ModelTestCase):
    requires = [User, Blog]

    def test_drop_cascade(self):
        u1 = User.create(username='u1')
        Blog.create(user=u1, title='b1')

        User.drop_table(cascade=True)
        assert not User.table_exists()

        # The constraint is dropped, we can create a blog for a non-
        # existant user.
        Blog.create(user=-1, title='b2')


@skip_unless(lambda: test_db.sequences)
class TestDatabaseSequences(ModelTestCase):
    requires = [SeqModelA, SeqModelB]

    def test_sequence_shared(self):
        a1 = SeqModelA.create(num=1)
        a2 = SeqModelA.create(num=2)
        b1 = SeqModelB.create(other_num=101)
        b2 = SeqModelB.create(other_num=102)
        a3 = SeqModelA.create(num=3)

        assert a1.id == a2.id - 1
        assert a2.id == b1.id - 1
        assert b1.id == b2.id - 1
        assert b2.id == a3.id - 1


@skip_unless(lambda: isinstance(test_db, SqliteDatabase))
class TestOuterLoopInnerCommit(ModelTestCase):
    requires = [User, Blog]

    def tearDown(self):
        test_db.set_autocommit(True)
        super(TestOuterLoopInnerCommit, self).tearDown()

    def test_outer_loop_inner_commit(self):
        # By default we are in autocommit mode (isolation_level=None).
        assert test_db.get_conn().isolation_level == None

        for username in ['u1', 'u2', 'u3']:
            User.create(username=username)

        for user in User.select():
            Blog.create(user=user, title='b-{0!s}'.format(user.username))

        # These statements are auto-committed.
        new_db = self.new_connection()
        count = new_db.execute_sql('select count(*) from blog;').fetchone()
        assert count[0] == 3

        assert Blog.select().count() == 3
        blog_titles = [b.title for b in Blog.select().order_by(Blog.title)]
        assert blog_titles == ['b-u1', 'b-u2', 'b-u3']

        assert Blog.delete().execute() == 3

        # If we disable autocommit, we need to explicitly call begin().
        test_db.set_autocommit(False)
        test_db.begin()

        for user in User.select():
            Blog.create(user=user, title='b-{0!s}'.format(user.username))

        # These statements have not been committed.
        new_db = self.new_connection()
        count = new_db.execute_sql('select count(*) from blog;').fetchone()
        assert count[0] == 0

        assert Blog.select().count() == 3
        blog_titles = [b.title for b in Blog.select().order_by(Blog.title)]
        assert blog_titles == ['b-u1', 'b-u2', 'b-u3']

        test_db.commit()
        count = new_db.execute_sql('select count(*) from blog;').fetchone()
        assert count[0] == 3


class TestConnectionInitialization(PeeweeTestCase):
    def test_initialize_connection(self):
        state = {'initialized': 0}

        class TestDatabase(SqliteDatabase):
            def initialize_connection(self, conn):
                state['initialized'] += 1

                # Ensure we can execute a query at this point.
                self.execute_sql('pragma stats;').fetchone()

        db = TestDatabase(':memory:')
        assert not state['initialized']

        db.get_conn()
        assert state['initialized'] == 1

        # Since a conn is already open, this will return the existing conn.
        db.get_conn()
        assert state['initialized'] == 1

        db.close()
        db.connect()
        assert state['initialized'] == 2
