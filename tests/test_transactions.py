# -*- coding: utf-8 -*-

import threading

import mock
import pytest

from peewee import IntegrityError, InternalError, SqliteDatabase
from peewee.core import _atomic, transaction
from tests.base import ModelTestCase, test_db
from tests.models import Blog, UniqueModel, User


class TestTransaction(ModelTestCase):
    requires = [User, Blog]

    def tearDown(self):
        super(TestTransaction, self).tearDown()
        test_db.set_autocommit(True)

    def test_transaction_connection_handling(self):
        patch = 'peewee.core.Database'
        db = SqliteDatabase(':memory:')
        with mock.patch(patch, wraps=db) as patched_db:
            with transaction(patched_db):
                patched_db.begin.assert_called_once_with()
                assert patched_db.commit.call_count == 0
                assert patched_db.rollback.call_count == 0

            patched_db.begin.assert_called_once_with()
            patched_db.commit.assert_called_once_with()
            assert patched_db.rollback.call_count == 0

        with mock.patch(patch, wraps=db) as patched_db:
            def _test_patched():
                patched_db.commit.side_effect = ValueError
                with transaction(patched_db):
                    pass

            with pytest.raises(ValueError):
                _test_patched()
            patched_db.begin.assert_called_once_with()
            patched_db.commit.assert_called_once_with()
            patched_db.rollback.assert_called_once_with()

    def test_atomic_nesting(self):
        db = SqliteDatabase(':memory:')
        db_patches = mock.patch.multiple(
            db,
            begin=mock.DEFAULT,
            commit=mock.DEFAULT,
            execute_sql=mock.DEFAULT,
            rollback=mock.DEFAULT)

        with mock.patch('peewee.core.Database', wraps=db) as patched_db:
            with db_patches as db_mocks:
                begin = db_mocks['begin']
                commit = db_mocks['commit']
                execute_sql = db_mocks['execute_sql']
                rollback = db_mocks['rollback']

                with _atomic(patched_db):
                    patched_db.transaction.assert_called_once_with()
                    begin.assert_called_once_with()
                    assert patched_db.savepoint.call_count == 0

                    with _atomic(patched_db):
                        patched_db.transaction.assert_called_once_with()
                        begin.assert_called_once_with()
                        patched_db.savepoint.assert_called_once_with()
                        assert commit.call_count == 0
                        assert rollback.call_count == 0

                        with _atomic(patched_db):
                            patched_db.transaction.assert_called_once_with()
                            begin.assert_called_once_with()
                            assert patched_db.savepoint.call_count == \
                                   2

                    begin.assert_called_once_with()
                    assert commit.call_count == 0
                    assert rollback.call_count == 0

                commit.assert_called_once_with()
                assert rollback.call_count == 0

    def test_autocommit(self):
        test_db.set_autocommit(False)
        test_db.begin()

        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        # open up a new connection to the database, it won't register any blogs
        # as being created
        new_db = self.new_connection()
        res = new_db.execute_sql('select count(*) from users;')
        assert res.fetchone()[0] == 0

        # commit our blog inserts
        test_db.commit()

        # now the blogs are query-able from another connection
        res = new_db.execute_sql('select count(*) from users;')
        assert res.fetchone()[0] == 2

    def test_transactions(self):
        def transaction_generator():
            with test_db.transaction():
                User.create(username='u1')
                yield
                User.create(username='u2')

        gen = transaction_generator()
        next(gen)

        conn2 = self.new_connection()
        res = conn2.execute_sql('select count(*) from users;').fetchone()
        assert res[0] == 0

        assert User.select().count() == 1

        # Consume the rest of the generator.
        for _ in gen:
            pass

        assert User.select().count() == 2
        res = conn2.execute_sql('select count(*) from users;').fetchone()
        assert res[0] == 2

    def test_manual_commit_rollback(self):
        def assertUsers(expected):
            query = User.select(User.username).order_by(User.username)
            assert [username for username, in query.tuples()] == \
                   expected

        with test_db.transaction() as txn:
            User.create(username='charlie')
            txn.commit()
            User.create(username='huey')
            txn.rollback()

        assertUsers(['charlie'])

        with test_db.transaction() as txn:
            User.create(username='huey')
            txn.rollback()
            User.create(username='zaizee')

        assertUsers(['charlie', 'zaizee'])

    def test_transaction_decorator(self):
        @test_db.transaction()
        def create_user(username):
            User.create(username=username)

        create_user('charlie')
        assert User.select().count() == 1

    def test_commit_on_success(self):
        assert test_db.get_autocommit()

        @test_db.commit_on_success
        def will_fail():
            User.create(username='u1')
            Blog.create()  # no blog, will raise an error

        with pytest.raises(IntegrityError):
            will_fail()
        assert User.select().count() == 0
        assert Blog.select().count() == 0

        @test_db.commit_on_success
        def will_succeed():
            u = User.create(username='u1')
            Blog.create(title='b1', user=u)

        will_succeed()
        assert User.select().count() == 1
        assert Blog.select().count() == 1

    def test_context_mgr(self):
        def do_will_fail():
            with test_db.transaction():
                User.create(username='u1')
                Blog.create()  # no blog, will raise an error

        with pytest.raises(IntegrityError):
            do_will_fail()
        assert Blog.select().count() == 0

        def do_will_succeed():
            with transaction(test_db):
                u = User.create(username='u1')
                Blog.create(title='b1', user=u)

        do_will_succeed()
        assert User.select().count() == 1
        assert Blog.select().count() == 1

        def do_manual_rollback():
            with test_db.transaction() as txn:
                User.create(username='u2')
                txn.rollback()

        do_manual_rollback()
        assert User.select().count() == 1
        assert Blog.select().count() == 1

    def test_nesting_transactions(self):
        @test_db.commit_on_success
        def outer(should_fail=False):
            assert test_db.transaction_depth() == 1
            User.create(username='outer')
            inner(should_fail)
            assert test_db.transaction_depth() == 1

        @test_db.commit_on_success
        def inner(should_fail):
            assert test_db.transaction_depth() == 2
            User.create(username='inner')
            if should_fail:
                raise ValueError('failing')

        with pytest.raises(ValueError):
            outer(should_fail=True)
        assert User.select().count() == 0
        assert test_db.transaction_depth() == 0

        outer(should_fail=False)
        assert User.select().count() == 2
        assert test_db.transaction_depth() == 0


class TestExecutionContext(ModelTestCase):
    requires = [User]

    def test_context_simple(self):
        with test_db.execution_context():
            User.create(username='charlie')
            assert test_db.execution_context_depth() == 1
        assert test_db.execution_context_depth() == 0

        with test_db.execution_context():
            assert User.select().where(User.username == 'charlie').exists()
            assert test_db.execution_context_depth() == 1
        assert test_db.execution_context_depth() == 0
        queries = self.queries()

    def test_context_ext(self):
        with test_db.execution_context():
            with test_db.execution_context() as inner_ctx:
                with test_db.execution_context():
                    User.create(username='huey')
                    assert test_db.execution_context_depth() == 3

                conn = test_db.get_conn()
                assert conn == inner_ctx.connection

                assert User.select().where(User.username == 'huey').exists()

        assert test_db.execution_context_depth() == 0

    def test_context_multithreaded(self):
        conn = test_db.get_conn()
        evt = threading.Event()
        evt2 = threading.Event()

        def create():
            with test_db.execution_context() as ctx:
                database = ctx.database
                assert database.execution_context_depth() == 1
                evt2.set()
                evt.wait()
                assert conn != ctx.connection
                User.create(username='huey')

        create_t = threading.Thread(target=create)
        create_t.daemon = True
        create_t.start()

        evt2.wait()
        assert test_db.execution_context_depth() == 0
        evt.set()
        create_t.join()

        assert test_db.execution_context_depth() == 0
        assert User.select().count() == 1

    def test_context_concurrency(self):
        def create(i):
            with test_db.execution_context():
                with test_db.execution_context() as ctx:
                    User.create(username='u{0!s}'.format(i))
                    assert ctx.database.execution_context_depth() == 2

        threads = [threading.Thread(target=create, args=(i,))
                   for i in range(5)]
        for thread in threads:
            thread.start()
        [thread.join() for thread in threads]
        assert [user.username for user in
                User.select().order_by(User.username)] == \
               ['u0', 'u1', 'u2', 'u3', 'u4']

    def test_context_conn_error(self):
        class MagicException(Exception):
            pass

        class FailDB(SqliteDatabase):
            def _connect(self, *args, **kwargs):
                raise MagicException('boo')

        db = FailDB(':memory:')

        def generate_exc():
            try:
                with db.execution_context():
                    db.execute_sql('SELECT 1;')
            except MagicException:
                db.get_conn()

        with pytest.raises(MagicException):
            generate_exc()


class TestAutoRollback(ModelTestCase):
    requires = [User, Blog]

    def setUp(self):
        test_db.autorollback = True
        super(TestAutoRollback, self).setUp()

    def tearDown(self):
        test_db.autorollback = False
        test_db.set_autocommit(True)
        super(TestAutoRollback, self).tearDown()

    def test_auto_rollback(self):
        # Exceptions are still raised.
        with pytest.raises(IntegrityError):
            Blog.create()

        # The transaction should have been automatically rolled-back, allowing
        # us to create new objects (in a new transaction).
        u = User.create(username='u')
        assert u.id

        # No-op, the previous INSERT was already committed.
        test_db.rollback()

        # Ensure we can get our user back.
        u_db = User.get(User.username == 'u')
        assert u.id == u_db.id

    def test_transaction_ctx_mgr(self):
        'Only auto-rollback when autocommit is enabled.'

        def create_error():
            with pytest.raises(IntegrityError):
                Blog.create()

        # autocommit is disabled in a transaction ctx manager.
        with test_db.transaction():
            # Error occurs, but exception is caught, leaving the current txn
            # in a bad state.
            create_error()

            try:
                create_error()
            except Exception as exc:
                # Subsequent call will raise an InternalError with postgres.
                assert isinstance(exc, InternalError)
            else:
                assert True

        # New transactions are not affected.
        self.test_auto_rollback()

    def test_manual(self):
        test_db.set_autocommit(False)

        # Will not be rolled back.
        with pytest.raises(IntegrityError):
            Blog.create()

        test_db.rollback()
        u = User.create(username='u')
        test_db.commit()
        u_db = User.get(User.username == 'u')
        assert u.id == u_db.id


class TestSavepoints(ModelTestCase):
    requires = [User]

    def _outer(self, fail_outer=False, fail_inner=False):
        with test_db.savepoint():
            User.create(username='outer')
            try:
                self._inner(fail_inner)
            except ValueError:
                pass
            if fail_outer:
                raise ValueError

    def _inner(self, fail_inner):
        with test_db.savepoint():
            User.create(username='inner')
            if fail_inner:
                raise ValueError('failing')

    def assertNames(self, expected):
        query = User.select().order_by(User.username)
        assert [u.username for u in query] == expected

    def test_success(self):
        with test_db.transaction():
            self._outer()
            assert User.select().count() == 2
        self.assertNames(['inner', 'outer'])

    def test_inner_failure(self):
        with test_db.transaction():
            self._outer(fail_inner=True)
            assert User.select().count() == 1
        self.assertNames(['outer'])

    def test_outer_failure(self):
        # Because the outer savepoint is rolled back, we'll lose the
        # inner savepoint as well.
        with test_db.transaction():
            with pytest.raises(ValueError):
                self._outer(fail_outer=True)
            assert User.select().count() == 0

    def test_failure(self):
        with test_db.transaction():
            with pytest.raises(
                ValueError):
                self._outer(fail_outer=True, fail_inner=True)
            assert User.select().count() == 0


class TestAtomic(ModelTestCase):
    requires = [User, UniqueModel]

    def test_atomic(self):
        with test_db.atomic():
            User.create(username='u1')
            with test_db.atomic():
                User.create(username='u2')
                with test_db.atomic() as txn3:
                    User.create(username='u3')
                    txn3.rollback()

                with test_db.atomic():
                    User.create(username='u4')

            with test_db.atomic() as txn5:
                User.create(username='u5')
                txn5.rollback()

            User.create(username='u6')

        query = User.select().order_by(User.username)
        assert [u.username for u in query] == \
               ['u1', 'u2', 'u4', 'u6']

    def test_atomic_second_connection(self):
        def test_separate_conn(expected):
            new_db = self.new_connection()
            cursor = new_db.execute_sql('select username from users;')
            usernames = sorted(row[0] for row in cursor.fetchall())
            assert usernames == expected
            new_db.close()

        with test_db.atomic():
            User.create(username='u1')
            test_separate_conn([])

            with test_db.atomic():
                User.create(username='u2')

            with test_db.atomic() as tx3:
                User.create(username='u3')
                tx3.rollback()

            test_separate_conn([])

            users = User.select(User.username).order_by(User.username)
            assert [user.username for user in users] == \
                   ['u1', 'u2']

        users = User.select(User.username).order_by(User.username)
        assert [user.username for user in users] == \
               ['u1', 'u2']

    def test_atomic_decorator(self):
        @test_db.atomic()
        def create_user(username):
            User.create(username=username)

        create_user('charlie')
        assert User.select().count() == 1

    def test_atomic_decorator_nesting(self):
        @test_db.atomic()
        def create_unique(name):
            UniqueModel.create(name=name)

        @test_db.atomic()
        def create_both(username):
            User.create(username=username)
            try:
                create_unique(username)
            except IntegrityError:
                pass

        create_unique('huey')
        assert UniqueModel.select().count() == 1

        create_both('charlie')
        assert User.select().count() == 1
        assert UniqueModel.select().count() == 2

        create_both('huey')
        assert User.select().count() == 2
        assert UniqueModel.select().count() == 2

    def test_atomic_rollback(self):
        with test_db.atomic():
            UniqueModel.create(name='charlie')
            try:
                with test_db.atomic():
                    UniqueModel.create(name='charlie')
            except IntegrityError:
                pass
            else:
                assert False

            with test_db.atomic():
                UniqueModel.create(name='zaizee')
                try:
                    with test_db.atomic():
                        UniqueModel.create(name='zaizee')
                except IntegrityError:
                    pass
                else:
                    assert False

                UniqueModel.create(name='mickey')
            UniqueModel.create(name='huey')

        names = [um.name for um in
                 UniqueModel.select().order_by(UniqueModel.name)]
        assert names == ['charlie', 'huey', 'mickey', 'zaizee']

    def test_atomic_with_delete(self):
        for i in range(3):
            User.create(username='u{0!s}'.format(i))

        with test_db.atomic():
            User.get(User.username == 'u1').delete_instance()

        usernames = [u.username for u in User.select()]
        assert sorted(usernames) == ['u0', 'u2']

        with test_db.atomic():
            with test_db.atomic():
                User.get(User.username == 'u2').delete_instance()

        usernames = [u.username for u in User.select()]
        assert usernames == ['u0']
