# -*- coding: utf-8 -*-

import datetime
import sys
from functools import partial

import pytest

from peewee import (CharField, CompositeKey, DateTimeField, Field,
                    ForeignKeyField, IntegerField, IntegrityError, JOIN, Model,
                    MySQLDatabase, PostgresqlDatabase, R, SQL, SqliteDatabase,
                    TextField, fn, prefetch)
from peewee.core import ModelOptions
from tests.base import (ModelTestCase, PeeweeTestCase, TestModel, compiler,
                        database_initializer, normal_compiler, skip_if,
                        skip_unless, test_db, ulit)
from tests.models import (Blog, BlogTwo, Category, Child, ChildNullableData,
                          ChildPet, Comment, EmptyModel, Flag, NoPKModel,
                          NonIntModel, Note, NoteFlagNullable, OrderedModel,
                          Orphan, OrphanPet, Package, PackageItem, Parent,
                          UniqueMultiField, UpperModel, User, UserCategory)

in_memory_db = database_initializer.get_in_memory_database()


class GCModel(Model):
    name = CharField(unique=True)
    key = CharField()
    value = CharField()
    number = IntegerField(default=0)

    class Meta:
        database = in_memory_db
        indexes = (
            (('key', 'value'), True),
        )


def incrementer():
    d = {'value': 0}

    def increment():
        d['value'] += 1
        return d['value']

    return increment


class DefaultsModel(Model):
    field = IntegerField(default=incrementer())
    control = IntegerField(default=1)

    class Meta:
        database = in_memory_db


class TestQueryingModels(ModelTestCase):
    requires = [User, Blog]

    def setUp(self):
        super(TestQueryingModels, self).setUp()
        self._orig_db_insert_many = test_db.insert_many

    def tearDown(self):
        super(TestQueryingModels, self).tearDown()
        test_db.insert_many = self._orig_db_insert_many

    def create_users_blogs(self, n=10, nb=5):
        for i in range(n):
            u = User.create(username='u{0:d}'.format(i))
            for j in range(nb):
                b = Blog.create(title='b-{0:d}-{1:d}'.format(i, j), content=str(j),
                                user=u)

    def test_select(self):
        self.create_users_blogs()

        users = User.select().where(User.username << ['u0', 'u5']).order_by(
            User.username)
        assert [u.username for u in users] == ['u0', 'u5']

        blogs = Blog.select().join(User).where(
            (User.username << ['u0', 'u3']) &
            (Blog.content == '4')
        ).order_by(Blog.title)
        assert [b.title for b in blogs] == ['b-0-4', 'b-3-4']

        users = User.select().paginate(2, 3)
        assert [u.username for u in users] == ['u3', 'u4', 'u5']

    def test_select_all(self):
        self.create_users_blogs(2, 2)
        all_cols = SQL('*')
        query = Blog.select(all_cols)
        blogs = [blog for blog in query.order_by(Blog.pk)]
        assert [b.title for b in blogs] == \
               ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1']
        assert [b.user.username for b in blogs] == \
               ['u0', 'u0', 'u1', 'u1']

    def test_select_subquery(self):
        # 10 users, 5 blogs each
        self.create_users_blogs(5, 3)

        # delete user 2's 2nd blog
        Blog.delete().where(Blog.title == 'b-2-2').execute()

        subquery = Blog.select(fn.Count(Blog.pk)).where(
            Blog.user == User.id).group_by(Blog.user)
        users = User.select(User, subquery.alias('ct')).order_by(R('ct'),
                                                                 User.id)

        assert [(x.username, x.ct) for x in users] == [
            ('u2', 2),
            ('u0', 3),
            ('u1', 3),
            ('u3', 3),
            ('u4', 3),
        ]

    def test_select_with_bind_to(self):
        self.create_users_blogs(1, 1)
        blog = Blog.select(
            Blog,
            User,
            (User.username == 'u0').alias('is_u0').bind_to(User),
            (User.username == 'u1').alias('is_u1').bind_to(User)
        ).join(User).get()

        assert blog.user.is_u0
        assert not blog.user.is_u1

    def test_scalar(self):
        User.create_users(5)

        users = User.select(fn.Count(User.id)).scalar()
        assert users == 5

        users = User.select(fn.Count(User.id)).where(
            User.username << ['u1', 'u2'])
        assert users.scalar() == 2
        assert users.scalar(True) == (2,)

        users = User.select(fn.Count(User.id)).where(
            User.username == 'not-here')
        assert users.scalar() == 0
        assert users.scalar(True) == (0,)

        users = User.select(fn.Count(User.id), fn.Count(User.username))
        assert users.scalar() == 5
        assert users.scalar(True) == (5, 5)

        User.create(username='u1')
        User.create(username='u2')
        User.create(username='u3')
        User.create(username='u99')
        users = User.select(fn.Count(fn.Distinct(User.username))).scalar()
        assert users == 6

    def test_noop_query(self):
        query = User.noop()
        with self.assertQueryCount(1) as qc:
            result = [row for row in query]

        assert result == []

    def test_update(self):
        User.create_users(5)
        uq = User.update(username='u-edited').where(
            User.username << ['u1', 'u2', 'u3'])
        assert [u.username for u in User.select().order_by(User.id)] == \
               ['u1', 'u2', 'u3', 'u4', 'u5']

        uq.execute()
        assert [u.username for u in User.select().order_by(User.id)] == \
               ['u-edited', 'u-edited', 'u-edited', 'u4', 'u5']

        with pytest.raises(KeyError):
            User.update(doesnotexist='invalid')

    def test_update_subquery(self):
        User.create_users(3)
        u1, u2, u3 = [user for user in User.select().order_by(User.id)]
        for i in range(4):
            Blog.create(title='b{0!s}'.format(i), user=u1)
        for i in range(2):
            Blog.create(title='b{0!s}'.format(i), user=u3)

        subquery = Blog.select(fn.COUNT(Blog.pk)).where(Blog.user == User.id)
        query = User.update(username=subquery)
        sql, params = normal_compiler.generate_update(query)
        assert sql == (
            'UPDATE "users" SET "username" = ('
            'SELECT COUNT("t2"."pk") FROM "blog" AS t2 '
            'WHERE ("t2"."user_id" = "users"."id"))')
        assert query.execute() == 3

        usernames = [u.username for u in User.select().order_by(User.id)]
        assert usernames == ['4', '0', '2']

    def test_insert(self):
        iq = User.insert(username='u1')
        assert User.select().count() == 0
        uid = iq.execute()
        assert uid > 0
        assert User.select().count() == 1
        u = User.get(User.id == uid)
        assert u.username == 'u1'

        with pytest.raises(KeyError):
            User.insert(doesnotexist='invalid')

    def test_insert_from(self):
        u0, u1, u2 = [User.create(username='U{0!s}'.format(i)) for i in range(3)]

        subquery = (User
                    .select(fn.LOWER(User.username))
                    .where(User.username << ['U0', 'U2']))
        iq = User.insert_from([User.username], subquery)
        sql, params = normal_compiler.generate_insert(iq)
        assert sql == (
            'INSERT INTO "users" ("username") '
            'SELECT LOWER("t2"."username") FROM "users" AS t2 '
            'WHERE ("t2"."username" IN (?, ?))')
        assert params == ['U0', 'U2']

        iq.execute()
        usernames = sorted([u.username for u in User.select()])
        assert usernames == ['U0', 'U1', 'U2', 'u0', 'u2']

    def test_insert_many(self):
        qc = len(self.queries())
        iq = User.insert_many([
            {'username': 'u1'},
            {'username': 'u2'},
            {'username': 'u3'},
            {'username': 'u4'}])
        assert iq.execute()

        qc2 = len(self.queries())
        if test_db.insert_many:
            assert qc2 - qc == 1
        else:
            assert qc2 - qc == 4
        assert User.select().count() == 4

        sq = User.select(User.username).order_by(User.username)
        assert [u.username for u in sq] == ['u1', 'u2', 'u3', 'u4']

        iq = User.insert_many([{'username': 'u5'}])
        assert iq.execute()
        assert User.select().count() == 5

        iq = User.insert_many([
            {User.username: 'u6'},
            {User.username: 'u7'},
            {'username': 'u8'}]).execute()

        sq = User.select(User.username).order_by(User.username)
        assert [u.username for u in sq] == \
               ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8']

    def test_insert_many_fallback(self):
        # Simulate database not supporting multiple insert (older versions of
        # sqlite).
        test_db.insert_many = False
        with self.assertQueryCount(4):
            iq = User.insert_many([
                {'username': 'u1'},
                {'username': 'u2'},
                {'username': 'u3'},
                {'username': 'u4'}])
            assert iq.execute()

        assert User.select().count() == 4

    def test_insert_many_validates_fields_by_default(self):
        assert User.insert_many([])._validate_fields

    def test_insert_many_without_field_validation(self):
        assert not User.insert_many([], validate_fields=False)._validate_fields

    def test_delete(self):
        User.create_users(5)
        dq = User.delete().where(User.username << ['u1', 'u2', 'u3'])
        assert User.select().count() == 5
        nr = dq.execute()
        assert nr == 3
        assert [u.username for u in User.select()] == ['u4', 'u5']

    def test_raw(self):
        User.create_users(3)
        interpolation = test_db.interpolation

        with self.assertQueryCount(1):
            query = 'select * from users where username IN ({0!s}, {1!s})'.format(
                interpolation, interpolation)
            rq = User.raw(query, 'u1', 'u3')
            assert [u.username for u in rq] == ['u1', 'u3']

            # iterate again
            assert [u.username for u in rq] == ['u1', 'u3']

        query = ('select id, username, %s as secret '
                 'from users where username = %s')
        rq = User.raw(
            query % (interpolation, interpolation),
            'sh', 'u2')
        assert [u.secret for u in rq] == ['sh']
        assert [u.username for u in rq] == ['u2']

        rq = User.raw('select count(id) from users')
        assert rq.scalar() == 3

        rq = User.raw('select username from users').tuples()
        assert [r for r in rq] == [
            ('u1',), ('u2',), ('u3',),
        ]

    def test_limits_offsets(self):
        for i in range(10):
            User.create(username='u{0:d}'.format(i))
        sq = User.select().order_by(User.id)

        offset_no_lim = sq.offset(3)
        assert [u.username for u in offset_no_lim] == \
               ['u{0:d}'.format(i) for i in range(3, 10)]

        offset_with_lim = sq.offset(5).limit(3)
        assert [u.username for u in offset_with_lim] == \
               ['u{0:d}'.format(i) for i in range(5, 8)]

    def test_raw_fn(self):
        self.create_users_blogs(3, 2)  # 3 users, 2 blogs each.
        query = User.raw('select count(1) as ct from blog group by user_id')
        results = [x.ct for x in query]
        assert results == [2, 2, 2]

    def test_model_iter(self):
        self.create_users_blogs(3, 2)
        usernames = [user.username for user in User]
        assert sorted(usernames) == ['u0', 'u1', 'u2']

        blogs = list(Blog)
        assert len(blogs) == 6


class TestInsertEmptyModel(ModelTestCase):
    requires = [EmptyModel, NoPKModel]

    def test_insert_empty(self):
        query = EmptyModel.insert()
        sql, params = compiler.generate_insert(query)
        if isinstance(test_db, MySQLDatabase):
            assert sql == (
                'INSERT INTO "emptymodel" ("emptymodel"."id") '
                'VALUES (DEFAULT)')
        else:
            assert sql == 'INSERT INTO "emptymodel" DEFAULT VALUES'
        assert params == []

        # Verify the query works.
        pk = query.execute()
        em = EmptyModel.get(EmptyModel.id == pk)

        # Verify we can also use `create()`.
        em2 = EmptyModel.create()
        assert EmptyModel.select().count() == 2

    def test_no_pk(self):
        obj = NoPKModel.create(data='1')
        assert NoPKModel.select(fn.COUNT('1')).scalar() == 1

        res = (NoPKModel
               .update(data='1-e')
               .where(NoPKModel.data == '1')
               .execute())
        assert res == 1
        assert NoPKModel.select(fn.COUNT('1')).scalar() == 1

        NoPKModel(data='2').save()
        NoPKModel(data='3').save()
        assert [obj.data for obj in
                NoPKModel.select().order_by(NoPKModel.data)] == \
               ['1-e', '2', '3']


class TestModelAPIs(ModelTestCase):
    requires = [User, Blog, Category, UserCategory, UniqueMultiField,
                NonIntModel]

    def setUp(self):
        super(TestModelAPIs, self).setUp()
        GCModel.drop_table(True)
        GCModel.create_table()

    def test_related_name(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        b11 = Blog.create(user=u1, title='b11')
        b12 = Blog.create(user=u1, title='b12')
        b2 = Blog.create(user=u2, title='b2')

        assert [b.title for b in u1.blog_set.order_by(Blog.title)] == \
               ['b11', 'b12']
        assert [b.title for b in u2.blog_set.order_by(Blog.title)] == \
               ['b2']

    def test_related_name_collision(self):
        class Foo(TestModel):
            f1 = CharField()

        def make_klass():
            class FooRel(TestModel):
                foo = ForeignKeyField(Foo, related_name='f1')

        with pytest.raises(AttributeError):
            make_klass()

    def test_callable_related_name(self):
        class Foo(TestModel):
            pass

        def rel_name(field):
            return '{0!s}_{1!s}_ref'.format(field.model_class._meta.name, field.name)

        class Bar(TestModel):
            fk1 = ForeignKeyField(Foo, related_name=rel_name)
            fk2 = ForeignKeyField(Foo, related_name=rel_name)

        class Baz(Bar):
            pass

        assert Foo.bar_fk1_ref.rel_model is Bar
        assert Foo.bar_fk2_ref.rel_model is Bar
        assert Foo.baz_fk1_ref.rel_model is Baz
        assert Foo.baz_fk2_ref.rel_model is Baz
        assert not hasattr(Foo, 'bar_set')
        assert not hasattr(Foo, 'baz_set')

    def test_fk_exceptions(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(parent=c1, name='c2')
        assert c1.parent == None
        assert c2.parent == c1

        c2_db = Category.get(Category.id == c2.id)
        assert c2_db.parent == c1

        u = User.create(username='u1')
        b = Blog.create(user=u, title='b')
        b2 = Blog(title='b2')

        assert b.user == u
        with pytest.raises(User.DoesNotExist):
            getattr(b2, 'user')

    def test_fk_cache_invalidated(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        b = Blog.create(user=u1, title='b')

        blog = Blog.get(Blog.pk == b)
        with self.assertQueryCount(1):
            assert blog.user.id == u1.id

        blog.user = u2.id
        with self.assertQueryCount(1):
            assert blog.user.id == u2.id

        # No additional query.
        blog.user = u2.id
        with self.assertQueryCount(0):
            assert blog.user.id == u2.id

    def test_fk_ints(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(name='c2', parent=c1.id)
        c2_db = Category.get(Category.id == c2.id)
        assert c2_db.parent == c1

    def test_fk_object_id(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(name='c2')
        c2.parent_id = c1.id
        c2.save()
        assert c2.parent == c1
        c2_db = Category.get(Category.name == 'c2')
        assert c2_db.parent == c1

    def test_fk_caching(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(name='c2', parent=c1)
        c2_db = Category.get(Category.id == c2.id)

        with self.assertQueryCount(1):
            parent = c2_db.parent
            assert parent == c1

            parent = c2_db.parent

    def test_related_id(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        for u in [u1, u2]:
            for j in range(2):
                Blog.create(user=u, title='{0!s}-{1!s}'.format(u.username, j))

        with self.assertQueryCount(1):
            query = Blog.select().order_by(Blog.pk)
            user_ids = [blog.user_id for blog in query]

        assert user_ids == [u1.id, u1.id, u2.id, u2.id]

        p1 = Category.create(name='p1')
        p2 = Category.create(name='p2')
        c1 = Category.create(name='c1', parent=p1)
        c2 = Category.create(name='c2', parent=p2)

        with self.assertQueryCount(1):
            query = Category.select().order_by(Category.id)
            assert [cat.parent_id for cat in query] == \
                   [None, None, p1.id, p2.id]

    def test_fk_object_id(self):
        u = User.create(username='u')
        b = Blog.create(user_id=u.id, title='b1')
        assert b._data['user'] == u.id
        assert not ('user' in b._obj_cache)

        with self.assertQueryCount(1):
            u_db = b.user
            assert u_db.id == u.id

        b_db = Blog.get(Blog.pk == b.pk)
        with self.assertQueryCount(0):
            assert b_db.user_id == u.id

        u2 = User.create(username='u2')
        Blog.create(user=u, title='b1x')
        Blog.create(user=u2, title='b2')

        q = Blog.select().where(Blog.user_id == u2.id)
        assert q.count() == 1
        assert q.get().title == 'b2'

        q = Blog.select(Blog.pk, Blog.user_id).where(Blog.user_id == u.id)
        assert q.count() == 2
        result = q.order_by(Blog.pk).first()
        assert result.user_id == u.id
        with self.assertQueryCount(1):
            assert result.user.id == u.id

    def test_object_id_descriptor_naming(self):
        class Person(Model):
            pass

        class Foo(Model):
            me = ForeignKeyField(Person, db_column='me', related_name='foo1')
            another = ForeignKeyField(Person, db_column='_whatever_',
                                      related_name='foo2')
            another2 = ForeignKeyField(Person, db_column='person_id',
                                       related_name='foo3')
            plain = ForeignKeyField(Person, related_name='foo4')

        assert Foo.me is Foo.me_id
        assert Foo.another is Foo._whatever_
        assert Foo.another2 is Foo.person_id
        assert Foo.plain is Foo.plain_id

        with pytest.raises(AttributeError):
            Foo.another_id
        with pytest.raises(AttributeError):
            Foo.another2_id

    def test_category_select_related_alias(self):
        g1 = Category.create(name='g1')
        g2 = Category.create(name='g2')

        p1 = Category.create(name='p1', parent=g1)
        p2 = Category.create(name='p2', parent=g2)

        c1 = Category.create(name='c1', parent=p1)
        c11 = Category.create(name='c11', parent=p1)
        c2 = Category.create(name='c2', parent=p2)

        with self.assertQueryCount(1):
            Grandparent = Category.alias()
            Parent = Category.alias()
            sq = (Category
                  .select(Category, Parent, Grandparent)
                  .join(Parent, on=(Category.parent == Parent.id))
                  .join(Grandparent, on=(Parent.parent == Grandparent.id))
                  .where(Grandparent.name == 'g1')
                  .order_by(Category.name))

            assert [(c.name, c.parent.name, c.parent.parent.name) for c in
                    sq] == \
                   [('c1', 'p1', 'g1'), ('c11', 'p1', 'g1')]

    def test_creation(self):
        User.create_users(10)
        assert User.select().count() == 10

    def test_saving(self):
        assert User.select().count() == 0

        u = User(username='u1')
        assert u.save() == 1
        u.username = 'u2'
        assert u.save() == 1

        assert User.select().count() == 1

        assert u.delete_instance() == 1
        assert u.save() == 0

    def test_save_fk(self):
        blog = Blog(title='b1', content='')
        blog.user = User(username='u1')
        blog.user.save()
        with self.assertQueryCount(1):
            blog.save()

        with self.assertQueryCount(1):
            blog_db = (Blog
                       .select(Blog, User)
                       .join(User)
                       .where(Blog.pk == blog.pk)
                       .get())
            assert blog_db.user.username == 'u1'

    def test_modify_model_cause_it_dirty(self):
        u = User(username='u1')
        u.save()
        assert not u.is_dirty()

        u.username = 'u2'
        assert u.is_dirty()
        assert u.dirty_fields == [User.username]

        u.save()
        assert not u.is_dirty()

        b = Blog.create(user=u, title='b1')
        assert not b.is_dirty()

        b.user = u
        assert b.is_dirty()
        assert b.dirty_fields == [Blog.user]

    def test_dirty_from_query(self):
        u1 = User.create(username='u1')
        b1 = Blog.create(title='b1', user=u1)
        b2 = Blog.create(title='b2', user=u1)

        u_db = User.get()
        assert not u_db.is_dirty()

        b_with_u = (Blog
                    .select(Blog, User)
                    .join(User)
                    .where(Blog.title == 'b2')
                    .get())
        assert not b_with_u.is_dirty()
        assert not b_with_u.user.is_dirty()

        u_with_blogs = (User
                        .select(User, Blog)
                        .join(Blog)
                        .order_by(Blog.title)
                        .aggregate_rows())[0]
        assert not u_with_blogs.is_dirty()
        for blog in u_with_blogs.blog_set:
            assert not blog.is_dirty()

        b_with_users = (Blog
                        .select(Blog, User)
                        .join(User)
                        .order_by(Blog.title)
                        .aggregate_rows())
        b1, b2 = b_with_users
        assert not b1.is_dirty()
        assert not b1.user.is_dirty()
        assert not b2.is_dirty()
        assert not b2.user.is_dirty()

    def test_save_only(self):
        u = User.create(username='u')
        b = Blog.create(user=u, title='b1', content='ct')
        b.title = 'b1-edit'
        b.content = 'ct-edit'

        b.save(only=[Blog.title])

        b_db = Blog.get(Blog.pk == b.pk)
        assert b_db.title == 'b1-edit'
        assert b_db.content == 'ct'

        b = Blog(user=u, title='b2', content='foo')
        b.save(only=[Blog.user, Blog.title])

        b_db = Blog.get(Blog.pk == b.pk)

        assert b_db.title == 'b2'
        assert b_db.content == ''

    def test_save_only_dirty_fields(self):
        u = User.create(username='u1')
        b = Blog.create(title='b1', user=u, content='huey')
        b_db = Blog.get(Blog.pk == b.pk)
        b.title = 'baby huey'
        b.save(only=b.dirty_fields)
        b_db.content = 'mickey-nugget'
        b_db.save(only=b_db.dirty_fields)
        saved = Blog.get(Blog.pk == b.pk)
        assert saved.title == 'baby huey'
        assert saved.content == 'mickey-nugget'

    def test_save_dirty_auto(self):
        User._meta.only_save_dirty = True
        Blog._meta.only_save_dirty = True
        try:
            with self.log_queries() as query_logger:
                u = User.create(username='u1')
                b = Blog.create(title='b1', user=u)

            # The default value for the blog content will be saved as well.
            assert [params for _, params in query_logger.queries] == \
                   [['u1'], [u.id, 'b1', '']]

            with self.assertQueryCount(0):
                assert u.save() is False
                assert b.save() is False

            u.username = 'u1-edited'
            b.title = 'b1-edited'
            with self.assertQueryCount(1):
                with self.log_queries() as query_logger:
                    assert u.save() == 1

            sql, params = query_logger.queries[0]
            assert sql.startswith('UPDATE')
            assert params == ['u1-edited', u.id]

            with self.assertQueryCount(1):
                with self.log_queries() as query_logger:
                    assert b.save() == 1

            sql, params = query_logger.queries[0]
            assert sql.startswith('UPDATE')
            assert params == ['b1-edited', b.pk]
        finally:
            User._meta.only_save_dirty = False
            Blog._meta.only_save_dirty = False

    def test_zero_id(self):
        if isinstance(test_db, MySQLDatabase):
            # Need to explicitly tell MySQL it's OK to use zero.
            test_db.execute_sql("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
        query = 'insert into users (id, username) values ({0!s}, {1!s})'.format(
            test_db.interpolation, test_db.interpolation)
        test_db.execute_sql(query, (0, 'foo'))
        Blog.insert(title='foo2', user=0).execute()

        u = User.get(User.id == 0)
        b = Blog.get(Blog.user == u)

        assert u == u
        assert u == b.user

    def test_saving_via_create_gh111(self):
        u = User.create(username='u')
        b = Blog.create(title='foo', user=u)
        last_sql, _ = self.queries()[-1]
        assert not ('pub_date' in last_sql)
        assert b.pub_date == None

        b2 = Blog(title='foo2', user=u)
        b2.save()
        last_sql, _ = self.queries()[-1]
        assert not ('pub_date' in last_sql)
        assert b2.pub_date == None

    def test_reading(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        assert u1 == User.get(username='u1')
        assert u2 == User.get(username='u2')
        assert not (u1 == u2)

        assert u1 == User.get(User.username == 'u1')
        assert u2 == User.get(User.username == 'u2')

    def test_get_exception(self):
        exc = None
        try:
            User.get(User.id == 0)
        except Exception as raised_exc:
            exc = raised_exc
        else:
            assert False

        assert exc.__module__ == 'tests.models'
        assert str(type(exc)) == \
               "<class 'tests.models.UserDoesNotExist'>"
        if sys.version_info[0] < 3:
            assert exc.message.startswith('Instance matching query')
            assert exc.message.endswith('PARAMS: [0]')

    def test_get_or_create(self):
        u1, created = User.get_or_create(username='u1')
        assert created

        u1_x, created = User.get_or_create(username='u1')
        assert not created

        assert u1.id == u1_x.id
        assert User.select().count() == 1

    def test_get_or_create_extended(self):
        gc1, created = GCModel.get_or_create(
            name='huey',
            key='k1',
            value='v1',
            defaults={'number': 3})
        assert created
        assert gc1.name == 'huey'
        assert gc1.key == 'k1'
        assert gc1.value == 'v1'
        assert gc1.number == 3

        gc1_db, created = GCModel.get_or_create(
            name='huey',
            defaults={'key': 'k2', 'value': 'v2'})
        assert not created
        assert gc1_db.id == gc1.id
        assert gc1_db.key == 'k1'

        def integrity_error():
            gc2, created = GCModel.get_or_create(
                name='huey',
                key='kx',
                value='vx')

        with pytest.raises(IntegrityError):
            integrity_error()

        gc2, created = GCModel.get_or_create(
            name__ilike='%nugget%',
            defaults={
                'name': 'foo-nugget',
                'key': 'k2',
                'value': 'v2'})
        assert created
        assert gc2.name == 'foo-nugget'

        gc2_db, created = GCModel.get_or_create(
            name__ilike='%nugg%',
            defaults={'name': 'xx'})
        assert not created
        assert gc2_db.id == gc2.id

        assert GCModel.select().count() == 2

    def test_create_or_get(self):
        assertQC = partial(self.assertQueryCount, ignore_txn=True)

        with assertQC(1):
            user, new = User.create_or_get(username='charlie')

        assert user.id is not None
        assert new

        with assertQC(2):
            user_get, new = User.create_or_get(username='peewee', id=user.id)

        assert not new
        assert user_get.id == user.id
        assert user_get.username == 'charlie'
        assert User.select().count() == 1

        # Test with a unique model.
        with assertQC(1):
            um, new = UniqueMultiField.create_or_get(
                name='baby huey',
                field_a='fielda',
                field_b=1)

        assert new
        assert um.name == 'baby huey'
        assert um.field_a == 'fielda'
        assert um.field_b == 1

        with assertQC(2):
            um_get, new = UniqueMultiField.create_or_get(
                name='baby huey',
                field_a='fielda-modified',
                field_b=2)

        assert not new
        assert um_get.id == um.id
        assert um_get.name == um.name
        assert um_get.field_a == um.field_a
        assert um_get.field_b == um.field_b
        assert UniqueMultiField.select().count() == 1

        # Test with a non-integer primary key model.
        with assertQC(1):
            nm, new = NonIntModel.create_or_get(
                pk='1337',
                data='sweet mickey')

        assert new
        assert nm.pk == '1337'
        assert nm.data == 'sweet mickey'

        with assertQC(2):
            nm_get, new = NonIntModel.create_or_get(
                pk='1337',
                data='michael-nuggie')

        assert not new
        assert nm_get.pk == nm.pk
        assert nm_get.data == nm.data
        assert NonIntModel.select().count() == 1

    def test_peek(self):
        users = User.create_users(3)

        with self.assertQueryCount(1):
            sq = User.select().order_by(User.username)

            # call it once
            u1 = sq.peek()
            assert u1.username == 'u1'

            # check the result cache
            assert len(sq._qr._result_cache) == 1

            # call it again and we get the same result, but not an
            # extra query
            assert sq.peek().username == 'u1'

        with self.assertQueryCount(0):
            # no limit is applied.
            usernames = [u.username for u in sq]
            assert usernames == ['u1', 'u2', 'u3']

    def test_first(self):
        users = User.create_users(3)

        with self.assertQueryCount(1):
            sq = User.select().order_by(User.username)

            # call it once
            first = sq.first()
            assert first.username == 'u1'

            # check the result cache
            assert len(sq._qr._result_cache) == 1

            # call it again and we get the same result, but not an
            # extra query
            assert sq.first().username == 'u1'

        with self.assertQueryCount(0):
            # also note that a limit has been applied.
            all_results = [obj for obj in sq]
            assert all_results == [first]

            usernames = [u.username for u in sq]
            assert usernames == ['u1']

        with self.assertQueryCount(0):
            # call first() after iterating
            assert sq.first().username == 'u1'

            usernames = [u.username for u in sq]
            assert usernames == ['u1']

        # call it with an empty result
        sq = User.select().where(User.username == 'not-here')
        assert sq.first() == None

    def test_deleting(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        assert User.select().count() == 2
        u1.delete_instance()
        assert User.select().count() == 1

        assert u2 == User.get(User.username == 'u2')

    def test_counting(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        for u in [u1, u2]:
            for i in range(5):
                Blog.create(title='b-{0!s}-{1!s}'.format(u.username, i), user=u)

        uc = User.select().where(User.username == 'u1').join(Blog).count()
        assert uc == 5

        uc = User.select().where(User.username == 'u1').join(
            Blog).distinct().count()
        assert uc == 1

        assert Blog.select().limit(4).offset(3).count() == 4
        assert Blog.select().limit(4).offset(3).count(True) == 10

        # Calling `distinct()` will result in a call to wrapped_count().
        uc = User.select().join(Blog).distinct().count()
        assert uc == 2

        # Test with clear limit = True.
        assert User.select().limit(1).count(clear_limit=True) == 2
        assert User.select().limit(1).wrapped_count(clear_limit=True) == 2

        # Test with clear limit = False.
        assert User.select().limit(1).count(clear_limit=False) == 1
        assert User.select().limit(1).wrapped_count(clear_limit=False) == 1

    def test_ordering(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        u3 = User.create(username='u2')
        users = User.select().order_by(User.username.desc(), User.id.desc())
        assert [u._get_pk_value() for u in users] == \
               [u3.id, u2.id, u1.id]

    def test_count_transaction(self):
        for i in range(10):
            User.create(username='u{0:d}'.format(i))

        with test_db.transaction():
            for user in User.select():
                for i in range(20):
                    Blog.create(user=user, title='b-{0:d}-{1:d}'.format(user.id, i))

        count = Blog.select().count()
        assert count == 200

    def test_exists(self):
        u1 = User.create(username='u1')
        assert User.select().where(User.username == 'u1').exists()
        assert not User.select().where(User.username == 'u2').exists()

    def test_unicode(self):
        # create a unicode literal
        ustr = ulit('Lýðveldið Ísland')
        u = User.create(username=ustr)

        # query using the unicode literal
        u_db = User.get(User.username == ustr)

        # the db returns a unicode literal
        assert u_db.username == ustr

        # delete the user
        assert u.delete_instance() == 1

        # convert the unicode to a utf8 string
        utf8_str = ustr.encode('utf-8')

        # create using the utf8 string
        u2 = User.create(username=utf8_str)

        # query using unicode literal
        u2_db = User.get(User.username == ustr)

        # we get unicode back
        assert u2_db.username == ustr

    def test_unicode_issue202(self):
        ustr = ulit('M\u00f6rk')
        user = User.create(username=ustr)
        assert user.username == ustr

    def test_on_conflict(self):
        gc = GCModel.create(name='g1', key='k1', value='v1')
        query = GCModel.insert(
            name='g1',
            key='k2',
            value='v2')
        with pytest.raises(IntegrityError):
            query.execute()

        # Ensure that we can ignore errors.
        res = query.on_conflict('IGNORE').execute()
        assert res == gc.id
        assert GCModel.select().count() == 1

        # Error ignored, no changes.
        gc_db = GCModel.get()
        assert gc_db.name == 'g1'
        assert gc_db.key == 'k1'
        assert gc_db.value == 'v1'

        # Replace the old, conflicting row, with the new data.
        res = query.on_conflict('REPLACE').execute()
        assert res != gc.id
        assert GCModel.select().count() == 1

        gc_db = GCModel.get()
        assert gc_db.name == 'g1'
        assert gc_db.key == 'k2'
        assert gc_db.value == 'v2'

        # Replaces also can occur when violating multi-column indexes.
        query = GCModel.insert(
            name='g2',
            key='k2',
            value='v2').on_conflict('REPLACE')

        res = query.execute()
        assert res != gc_db.id
        assert GCModel.select().count() == 1

        gc_db = GCModel.get()
        assert gc_db.name == 'g2'
        assert gc_db.key == 'k2'
        assert gc_db.value == 'v2'

    def test_on_conflict_many(self):
        if not SqliteDatabase.insert_many:
            return

        for i in range(5):
            key = 'gc{0!s}'.format(i)
            GCModel.create(name=key, key=key, value=key)

        insert = [
            {'name': key, 'key': 'x-{0!s}'.format(key), 'value': key}
            for key in ['gc{0!s}'.format(i) for i in range(10)]]
        res = GCModel.insert_many(insert).on_conflict('IGNORE').execute()
        assert GCModel.select().count() == 10

        gcs = list(GCModel.select().order_by(GCModel.id))
        first_five, last_five = gcs[:5], gcs[5:]

        # The first five should all be "gcI", the last five will have
        # "x-gcI" for their keys.
        assert [gc.key for gc in first_five] == \
               ['gc0', 'gc1', 'gc2', 'gc3', 'gc4']

        assert [gc.key for gc in last_five] == \
               ['x-gc5', 'x-gc6', 'x-gc7', 'x-gc8', 'x-gc9']

    def test_meta_get_field_index(self):
        index = Blog._meta.get_field_index(Blog.content)
        assert index == 3

    def test_meta_remove_field(self):

        class _Model(Model):
            title = CharField(max_length=25)
            content = TextField(default='')

        _Model._meta.remove_field('content')
        assert 'content' not in _Model._meta.fields
        assert 'content' not in _Model._meta.sorted_field_names
        assert [f.name for f in _Model._meta.sorted_fields] == \
               ['id', 'title']

    def test_meta_rel_for_model(self):
        class User(Model):
            pass

        class Category(Model):
            parent = ForeignKeyField('self')

        class Tweet(Model):
            user = ForeignKeyField(User)

        class Relationship(Model):
            from_user = ForeignKeyField(User, related_name='r1')
            to_user = ForeignKeyField(User, related_name='r2')

        UM = User._meta
        CM = Category._meta
        TM = Tweet._meta
        RM = Relationship._meta

        # Simple refs work.
        assert UM.rel_for_model(Tweet) is None
        assert UM.rel_for_model(Tweet, multi=True) == []
        assert UM.reverse_rel_for_model(Tweet) == Tweet.user
        assert UM.reverse_rel_for_model(Tweet, multi=True) == \
               [Tweet.user]

        # Multi fks.
        assert RM.rel_for_model(User) == Relationship.from_user
        assert RM.rel_for_model(User, multi=True) == \
               [Relationship.from_user, Relationship.to_user]

        assert UM.reverse_rel_for_model(Relationship) == \
               Relationship.from_user
        assert UM.reverse_rel_for_model(Relationship, multi=True) == \
               [Relationship.from_user, Relationship.to_user]

        # Self-refs work.
        assert CM.rel_for_model(Category) == Category.parent
        assert CM.reverse_rel_for_model(Category) == Category.parent

        # Field aliases work.
        UA = User.alias()
        assert TM.rel_for_model(UA) == Tweet.user


class TestAggregatesWithModels(ModelTestCase):
    requires = [OrderedModel, User, Blog]

    def create_ordered_models(self):
        return [
            OrderedModel.create(
                title=i, created=datetime.datetime(2013, 1, i + 1))
            for i in range(3)]

    def create_user_blogs(self):
        users = []
        ct = 0
        for i in range(2):
            user = User.create(username='u-{0:d}'.format(i))
            for j in range(2):
                ct += 1
                Blog.create(
                    user=user,
                    title='b-{0:d}-{1:d}'.format(i, j),
                    pub_date=datetime.datetime(2013, 1, ct))
            users.append(user)
        return users

    def test_annotate_int(self):
        users = self.create_user_blogs()
        annotated = User.select().annotate(Blog, fn.Count(Blog.pk).alias('ct'))
        for i, user in enumerate(annotated):
            assert user.ct == 2
            assert user.username == 'u-{0:d}'.format(i)

    def test_annotate_datetime(self):
        users = self.create_user_blogs()
        annotated = (User
                     .select()
                     .annotate(Blog, fn.Max(Blog.pub_date).alias('max_pub')))
        user_0, user_1 = annotated
        assert user_0.max_pub == datetime.datetime(2013, 1, 2)
        assert user_1.max_pub == datetime.datetime(2013, 1, 4)

    def test_aggregate_int(self):
        models = self.create_ordered_models()
        max_id = OrderedModel.select().aggregate(fn.Max(OrderedModel.id))
        assert max_id == models[-1].id

    def test_aggregate_datetime(self):
        models = self.create_ordered_models()
        max_created = (OrderedModel
                       .select()
                       .aggregate(fn.Max(OrderedModel.created)))
        assert max_created == models[-1].created


class TestMultiTableFromClause(ModelTestCase):
    requires = [Blog, Comment, User]

    def setUp(self):
        super(TestMultiTableFromClause, self).setUp()

        for u in range(2):
            user = User.create(username='u{0!s}'.format(u))
            for i in range(3):
                b = Blog.create(user=user, title='b{0!s}-{1!s}'.format(u, i))
                for j in range(i):
                    Comment.create(blog=b, comment='c{0!s}-{1!s}'.format(i, j))

    def test_from_multi_table(self):
        q = (Blog
             .select(Blog, User)
             .from_(Blog, User)
             .where(
            (Blog.user == User.id) &
            (User.username == 'u0'))
             .order_by(Blog.pk)
             .naive())

        with self.assertQueryCount(1):
            blogs = [b.title for b in q]
            assert blogs == ['b0-0', 'b0-1', 'b0-2']

            usernames = [b.username for b in q]
            assert usernames == ['u0', 'u0', 'u0']

    def test_subselect(self):
        inner = User.select(User.username)
        assert [u.username for u in inner.order_by(User.username)] == ['u0',
                                                                       'u1']

        # Have to manually specify the alias as "t1" because the outer query
        # will expect that.
        outer = (User
                 .select(User.username)
                 .from_(inner.alias('t1')))
        sql, params = compiler.generate_select(outer)
        assert sql == (
            'SELECT "users"."username" FROM '
            '(SELECT "users"."username" FROM "users" AS users) AS t1')

        assert [u.username for u in outer.order_by(User.username)] == ['u0',
                                                                       'u1']

    def test_subselect_with_column(self):
        inner = User.select(User.username.alias('name')).alias('t1')
        outer = (User
                 .select(inner.c.name)
                 .from_(inner))
        sql, params = compiler.generate_select(outer)
        assert sql == (
            'SELECT "t1"."name" FROM '
            '(SELECT "users"."username" AS name FROM "users" AS users) AS t1')

        query = outer.order_by(inner.c.name.desc())
        assert [u[0] for u in query.tuples()] == ['u1', 'u0']

    def test_subselect_with_join(self):
        inner = User.select(User.id, User.username).alias('q1')
        outer = (Blog
                 .select(inner.c.id, inner.c.username)
                 .from_(inner)
                 .join(Comment, on=(inner.c.id == Comment.id)))
        sql, params = compiler.generate_select(outer)
        assert sql == (
            'SELECT "q1"."id", "q1"."username" FROM ('
            'SELECT "users"."id", "users"."username" FROM "users" AS users) AS q1 '
            'INNER JOIN "comment" AS comment ON ("q1"."id" = "comment"."id")')

    def test_join_on_query(self):
        u0 = User.get(User.username == 'u0')
        u1 = User.get(User.username == 'u1')

        inner = User.select().alias('j1')
        outer = (Blog
                 .select(Blog.title, Blog.user)
                 .join(inner, on=(Blog.user == inner.c.id))
                 .order_by(Blog.pk))
        res = [row for row in outer.tuples()]
        assert res == [
            ('b0-0', u0.id),
            ('b0-1', u0.id),
            ('b0-2', u0.id),
            ('b1-0', u1.id),
            ('b1-1', u1.id),
            ('b1-2', u1.id),
        ]


class TestDeleteRecursive(ModelTestCase):
    requires = [
        Parent, Child, ChildNullableData, ChildPet, Orphan, OrphanPet, Package,
        PackageItem]

    def setUp(self):
        super(TestDeleteRecursive, self).setUp()
        self.p1 = p1 = Parent.create(data='p1')
        self.p2 = p2 = Parent.create(data='p2')
        c11 = Child.create(parent=p1)
        c12 = Child.create(parent=p1)
        c21 = Child.create(parent=p2)
        c22 = Child.create(parent=p2)
        o11 = Orphan.create(parent=p1)
        o12 = Orphan.create(parent=p1)
        o21 = Orphan.create(parent=p2)
        o22 = Orphan.create(parent=p2)

        for child in [c11, c12, c21, c22]:
            ChildPet.create(child=child)

        for orphan in [o11, o12, o21, o22]:
            OrphanPet.create(orphan=orphan)

        for i, child in enumerate([c11, c12]):
            for j in range(2):
                ChildNullableData.create(
                    child=child,
                    data='{0!s}-{1!s}'.format(i, j))

    def test_recursive_delete_parent_sql(self):
        with self.log_queries() as query_logger:
            with self.assertQueryCount(5):
                self.p1.delete_instance(recursive=True, delete_nullable=False)

        queries = query_logger.queries
        update_cnd = ('UPDATE `childnullabledata` '
                      'SET `child_id` = %% '
                      'WHERE ('
                      '`childnullabledata`.`child_id` IN ('
                      'SELECT `t2`.`id` FROM `child` AS t2 WHERE ('
                      '`t2`.`parent_id` = %%)))')
        delete_cp = ('DELETE FROM `childpet` WHERE ('
                     '`child_id` IN ('
                     'SELECT `t1`.`id` FROM `child` AS t1 WHERE ('
                     '`t1`.`parent_id` = %%)))')
        delete_c = 'DELETE FROM `child` WHERE (`parent_id` = %%)'
        update_o = ('UPDATE `orphan` SET `parent_id` = %% WHERE ('
                    '`orphan`.`parent_id` = %%)')
        delete_p = 'DELETE FROM `parent` WHERE (`id` = %%)'
        sql_params = [
            (update_cnd, [None, self.p1.id]),
            (delete_cp, [self.p1.id]),
            (delete_c, [self.p1.id]),
            (update_o, [None, self.p1.id]),
            (delete_p, [self.p1.id]),
        ]
        self.assertQueriesEqual(queries, sql_params)

    def test_recursive_delete_child_queries(self):
        c2 = self.p1.child_set.order_by(Child.id.desc()).get()
        with self.log_queries() as query_logger:
            with self.assertQueryCount(3):
                c2.delete_instance(recursive=True, delete_nullable=False)

        queries = query_logger.queries

        update_cnd = ('UPDATE `childnullabledata` SET `child_id` = %% WHERE ('
                      '`childnullabledata`.`child_id` = %%)')
        delete_cp = 'DELETE FROM `childpet` WHERE (`child_id` = %%)'
        delete_c = 'DELETE FROM `child` WHERE (`id` = %%)'

        sql_params = [
            (update_cnd, [None, c2.id]),
            (delete_cp, [c2.id]),
            (delete_c, [c2.id]),
        ]
        self.assertQueriesEqual(queries, sql_params)

    def assertQueriesEqual(self, queries, expected):
        queries.sort()
        expected.sort()
        for i in range(len(queries)):
            sql, params = queries[i]
            expected_sql, expected_params = expected[i]
            expected_sql = (expected_sql
                            .replace('`', test_db.quote_char)
                            .replace('%%', test_db.interpolation))
            assert sql == expected_sql
            assert params == expected_params

    def test_recursive_update(self):
        self.p1.delete_instance(recursive=True)
        counts = (
            # query,fk,p1,p2,tot
            (Child.select(), Child.parent, 0, 2, 2),
            (Orphan.select(), Orphan.parent, 0, 2, 4),
            (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
            (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 4),
        )

        for query, fk, p1_ct, p2_ct, tot in counts:
            assert query.where(fk == self.p1).count() == p1_ct
            assert query.where(fk == self.p2).count() == p2_ct
            assert query.count() == tot

    def test_recursive_delete(self):
        self.p1.delete_instance(recursive=True, delete_nullable=True)
        counts = (
            # query,fk,p1,p2,tot
            (Child.select(), Child.parent, 0, 2, 2),
            (Orphan.select(), Orphan.parent, 0, 2, 2),
            (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
            (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 2),
        )

        for query, fk, p1_ct, p2_ct, tot in counts:
            assert query.where(fk == self.p1).count() == p1_ct
            assert query.where(fk == self.p2).count() == p2_ct
            assert query.count() == tot

    def test_recursive_non_pk_fk(self):
        for i in range(3):
            Package.create(barcode=str(i))
            for j in range(4):
                PackageItem.create(package=str(i), title='{0!s}-{1!s}'.format(i, j))

        assert Package.select().count() == 3
        assert PackageItem.select().count() == 12

        Package.get(Package.barcode == '1').delete_instance(recursive=True)

        assert Package.select().count() == 2
        assert PackageItem.select().count() == 8

        items = (PackageItem
                 .select(PackageItem.title)
                 .order_by(PackageItem.id)
                 .tuples())
        assert [i[0] for i in items] == [
            '0-0', '0-1', '0-2', '0-3',
            '2-0', '2-1', '2-2', '2-3',
        ]


@skip_if(lambda: isinstance(test_db, MySQLDatabase))
class TestTruncate(ModelTestCase):
    requires = [User]

    def test_truncate(self):
        for i in range(3):
            User.create(username='u{0!s}'.format(i))

        User.truncate_table(restart_identity=True)
        assert User.select().count() == 0

        u = User.create(username='ux')
        assert u.id == 1


class TestManyToMany(ModelTestCase):
    requires = [User, Category, UserCategory]

    def setUp(self):
        super(TestManyToMany, self).setUp()
        users = ['u1', 'u2', 'u3']
        categories = ['c1', 'c2', 'c3', 'c12', 'c23']
        user_to_cat = {
            'u1': ['c1', 'c12'],
            'u2': ['c2', 'c12', 'c23'],
        }
        for u in users:
            User.create(username=u)
        for c in categories:
            Category.create(name=c)
        for user, categories in user_to_cat.items():
            user = User.get(User.username == user)
            for category in categories:
                UserCategory.create(
                    user=user,
                    category=Category.get(Category.name == category))

    def test_m2m(self):
        def aU(q, exp):
            assert [u.username for u in q.order_by(User.username)] == \
                   exp

        def aC(q, exp):
            assert [c.name for c in q.order_by(Category.name)] == exp

        users = User.select().join(UserCategory).join(Category).where(
            Category.name == 'c1')
        aU(users, ['u1'])

        users = User.select().join(UserCategory).join(Category).where(
            Category.name == 'c3')
        aU(users, [])

        cats = Category.select().join(UserCategory).join(User).where(
            User.username == 'u1')
        aC(cats, ['c1', 'c12'])

        cats = Category.select().join(UserCategory).join(User).where(
            User.username == 'u2')
        aC(cats, ['c12', 'c2', 'c23'])

        cats = Category.select().join(UserCategory).join(User).where(
            User.username == 'u3')
        aC(cats, [])

        cats = Category.select().join(UserCategory).join(User).where(
            Category.name << ['c1', 'c2', 'c3']
        )
        aC(cats, ['c1', 'c2'])

        cats = Category.select().join(UserCategory, JOIN.LEFT_OUTER).join(User,
                                                                          JOIN.LEFT_OUTER).where(
            Category.name << ['c1', 'c2', 'c3']
        )
        aC(cats, ['c1', 'c2', 'c3'])

    def test_many_to_many_prefetch(self):
        categories = Category.select().order_by(Category.name)
        user_categories = UserCategory.select().order_by(UserCategory.id)
        users = User.select().order_by(User.username)
        results = {}
        result_list = []
        with self.assertQueryCount(3):
            query = prefetch(categories, user_categories, users)
            for category in query:
                results.setdefault(category.name, set())
                result_list.append(category.name)
                for user_category in category.usercategory_set_prefetch:
                    results[category.name].add(user_category.user.username)
                    result_list.append(user_category.user.username)

        assert results == {
            'c1': set(['u1']),
            'c12': set(['u1', 'u2']),
            'c2': set(['u2']),
            'c23': set(['u2']),
            'c3': set(),
        }
        assert sorted(result_list) == \
               ['c1', 'c12', 'c2', 'c23', 'c3', 'u1', 'u1', 'u2', 'u2', 'u2']


class TestCustomModelOptionsBase(PeeweeTestCase):
    def test_custom_model_options_base(self):
        db = SqliteDatabase(None)

        class DatabaseDescriptor(object):
            def __init__(self, db):
                self._db = db

            def __get__(self, instance_type, instance):
                if instance is not None:
                    return self._db
                return self

            def __set__(self, instance, value):
                pass

        class TestModelOptions(ModelOptions):
            database = DatabaseDescriptor(db)

        class BaseModel(Model):
            class Meta:
                model_options_base = TestModelOptions

        class TestModel(BaseModel):
            pass

        class TestChildModel(TestModel):
            pass

        assert id(TestModel._meta.database) == id(db)
        assert id(TestChildModel._meta.database) == id(db)


class TestModelOptionInheritance(PeeweeTestCase):
    def test_db_table(self):
        assert User._meta.db_table == 'users'

        class Foo(TestModel):
            pass

        assert Foo._meta.db_table == 'foo'

        class Foo2(TestModel):
            pass

        assert Foo2._meta.db_table == 'foo2'

        class Foo_3(TestModel):
            pass

        assert Foo_3._meta.db_table == 'foo_3'

    def test_custom_options(self):
        class A(Model):
            class Meta:
                a = 'a'

        class B1(A):
            class Meta:
                b = 1

        class B2(A):
            class Meta:
                b = 2

        assert A._meta.a == 'a'
        assert B1._meta.a == 'a'
        assert B2._meta.a == 'a'
        assert B1._meta.b == 1
        assert B2._meta.b == 2

    def test_option_inheritance(self):
        x_test_db = SqliteDatabase('testing.db')
        child2_db = SqliteDatabase('child2.db')

        class FakeUser(Model):
            pass

        class ParentModel(Model):
            title = CharField()
            user = ForeignKeyField(FakeUser)

            class Meta:
                database = x_test_db

        class ChildModel(ParentModel):
            pass

        class ChildModel2(ParentModel):
            special_field = CharField()

            class Meta:
                database = child2_db

        class GrandChildModel(ChildModel):
            pass

        class GrandChildModel2(ChildModel2):
            special_field = TextField()

        assert ParentModel._meta.database.database == 'testing.db'
        assert ParentModel._meta.model_class == ParentModel

        assert ChildModel._meta.database.database == 'testing.db'
        assert ChildModel._meta.model_class == ChildModel
        assert sorted(ChildModel._meta.fields.keys()) == [
            'id', 'title', 'user'
        ]

        assert ChildModel2._meta.database.database == 'child2.db'
        assert ChildModel2._meta.model_class == ChildModel2
        assert sorted(ChildModel2._meta.fields.keys()) == [
            'id', 'special_field', 'title', 'user'
        ]

        assert GrandChildModel._meta.database.database == 'testing.db'
        assert GrandChildModel._meta.model_class == GrandChildModel
        assert sorted(GrandChildModel._meta.fields.keys()) == [
            'id', 'title', 'user'
        ]

        assert GrandChildModel2._meta.database.database == 'child2.db'
        assert GrandChildModel2._meta.model_class == GrandChildModel2
        assert sorted(GrandChildModel2._meta.fields.keys()) == [
            'id', 'special_field', 'title', 'user'
        ]
        assert isinstance(GrandChildModel2._meta.fields['special_field'],
                          TextField)

    def test_order_by_inheritance(self):
        class Base(TestModel):
            created = DateTimeField()

            class Meta:
                order_by = ('-created',)

        class Foo(Base):
            data = CharField()

        class Bar(Base):
            val = IntegerField()

            class Meta:
                order_by = ('-val',)

        foo_order_by = Foo._meta.order_by[0]
        assert isinstance(foo_order_by, Field)
        assert foo_order_by.model_class is Foo
        assert foo_order_by.name == 'created'

        bar_order_by = Bar._meta.order_by[0]
        assert isinstance(bar_order_by, Field)
        assert bar_order_by.model_class is Bar
        assert bar_order_by.name == 'val'

    def test_table_name_function(self):
        class Base(TestModel):
            class Meta:
                def db_table_func(model):
                    return model.__name__.lower() + 's'

        class User(Base):
            pass

        class SuperUser(User):
            class Meta:
                db_table = 'nugget'

        class MegaUser(SuperUser):
            class Meta:
                def db_table_func(model):
                    return 'mega'

        class Bear(Base):
            pass

        assert User._meta.db_table == 'users'
        assert Bear._meta.db_table == 'bears'
        assert SuperUser._meta.db_table == 'nugget'
        assert MegaUser._meta.db_table == 'mega'


class TestModelInheritance(ModelTestCase):
    requires = [Blog, BlogTwo, User]

    def test_model_inheritance_attrs(self):
        assert Blog._meta.sorted_field_names == \
               ['pk', 'user', 'title', 'content', 'pub_date']
        assert BlogTwo._meta.sorted_field_names == \
               ['pk', 'user', 'content', 'pub_date', 'title',
                'extra_field']

        assert Blog._meta.primary_key.name == 'pk'
        assert BlogTwo._meta.primary_key.name == 'pk'

        assert Blog.user.related_name == 'blog_set'
        assert BlogTwo.user.related_name == 'blogtwo_set'

        assert User.blog_set.rel_model == Blog
        assert User.blogtwo_set.rel_model == BlogTwo

        assert not (BlogTwo._meta.db_table == Blog._meta.db_table)

    def test_model_inheritance_flow(self):
        u = User.create(username='u')

        b = Blog.create(title='b', user=u)
        b2 = BlogTwo.create(title='b2', extra_field='foo', user=u)

        assert list(u.blog_set) == [b]
        assert list(u.blogtwo_set) == [b2]

        assert Blog.select().count() == 1
        assert BlogTwo.select().count() == 1

        b_from_db = Blog.get(Blog.pk == b.pk)
        b2_from_db = BlogTwo.get(BlogTwo.pk == b2.pk)

        assert b_from_db.user == u
        assert b2_from_db.user == u
        assert b2_from_db.extra_field == 'foo'

    def test_inheritance_primary_keys(self):
        assert not hasattr(Model, 'id')

        class M1(Model): pass

        assert hasattr(M1, 'id')

        class M2(Model):
            key = CharField(primary_key=True)

        assert not hasattr(M2, 'id')

        class M3(Model):
            id = TextField()
            key = IntegerField(primary_key=True)

        assert hasattr(M3, 'id')
        assert not M3.id.primary_key

        class C1(M1): pass

        assert hasattr(C1, 'id')
        assert C1.id.model_class is C1

        class C2(M2): pass

        assert not hasattr(C2, 'id')
        assert C2.key.primary_key
        assert C2.key.model_class is C2

        class C3(M3): pass

        assert hasattr(C3, 'id')
        assert not C3.id.primary_key
        assert C3.id.model_class is C3


class TestAliasBehavior(ModelTestCase):
    requires = [UpperModel]

    def test_alias_with_coerce(self):
        UpperModel.create(data='test')
        um = UpperModel.get()
        assert um.data == 'TEST'

        Alias = UpperModel.alias()
        normal = (UpperModel.data == 'foo')
        aliased = (Alias.data == 'foo')
        _, normal_p = compiler.parse_node(normal)
        _, aliased_p = compiler.parse_node(aliased)
        assert normal_p == ['FOO']
        assert aliased_p == ['FOO']

        expected = (
            'SELECT "uppermodel"."id", "uppermodel"."data" '
            'FROM "uppermodel" AS uppermodel '
            'WHERE ("uppermodel"."data" = ?)')

        query = UpperModel.select().where(UpperModel.data == 'foo')
        sql, params = compiler.generate_select(query)
        assert sql == expected
        assert params == ['FOO']

        query = Alias.select().where(Alias.data == 'foo')
        sql, params = compiler.generate_select(query)
        assert sql == expected
        assert params == ['FOO']


@skip_unless(lambda: isinstance(test_db, PostgresqlDatabase))
class TestInsertReturningModelAPI(PeeweeTestCase):
    def setUp(self):
        super(TestInsertReturningModelAPI, self).setUp()

        self.db = database_initializer.get_database(
            'postgres',
            PostgresqlDatabase)

        class BaseModel(TestModel):
            class Meta:
                database = self.db

        self.BaseModel = BaseModel
        self.models = []

    def tearDown(self):
        if self.models:
            self.db.drop_tables(self.models, True)
        super(TestInsertReturningModelAPI, self).tearDown()

    def test_insert_returning(self):
        class User(self.BaseModel):
            username = CharField()

            class Meta:
                db_table = 'users'

        self.models.append(User)
        User.create_table()

        query = User.insert(username='charlie')
        sql, params = query.sql()
        assert sql == (
            'INSERT INTO "users" ("username") VALUES (%s) RETURNING "id"')
        assert params == ['charlie']

        result = query.execute()
        charlie = User.get(User.username == 'charlie')
        assert result == charlie.id

        result2 = User.insert(username='huey').execute()
        assert result2 > result
        huey = User.get(User.username == 'huey')
        assert result2 == huey.id

        mickey = User.create(username='mickey')
        assert mickey.id == huey.id + 1
        mickey.save()
        assert User.select().count() == 3

    def test_non_int_pk(self):
        class User(self.BaseModel):
            username = CharField(primary_key=True)
            data = IntegerField()

            class Meta:
                db_table = 'users'

        self.models.append(User)
        User.create_table()

        query = User.insert(username='charlie', data=1337)
        sql, params = query.sql()
        assert sql == (
            'INSERT INTO "users" ("username", "data") '
            'VALUES (%s, %s) RETURNING "username"')
        assert params == ['charlie', 1337]

        assert query.execute() == 'charlie'
        charlie = User.get(User.data == 1337)
        assert charlie.username == 'charlie'

        huey = User.create(username='huey', data=1024)
        assert huey.username == 'huey'
        assert huey.data == 1024

        huey_db = User.get(User.data == 1024)
        assert huey_db.username == 'huey'
        huey_db.save()
        assert huey_db.username == 'huey'

        assert User.select().count() == 2

    def test_composite_key(self):
        class Person(self.BaseModel):
            first = CharField()
            last = CharField()
            data = IntegerField()

            class Meta:
                primary_key = CompositeKey('first', 'last')

        self.models.append(Person)
        Person.create_table()

        query = Person.insert(first='huey', last='leifer', data=3)
        sql, params = query.sql()
        assert sql == (
            'INSERT INTO "person" ("first", "last", "data") '
            'VALUES (%s, %s, %s) RETURNING "first", "last"')
        assert params == ['huey', 'leifer', 3]

        res = query.execute()
        assert res == ['huey', 'leifer']

        huey = Person.get(Person.data == 3)
        assert huey.first == 'huey'
        assert huey.last == 'leifer'

        zaizee = Person.create(first='zaizee', last='owen', data=2)
        assert zaizee.first == 'zaizee'
        assert zaizee.last == 'owen'

        z_db = Person.get(Person.data == 2)
        assert z_db.first == 'zaizee'
        assert z_db.last == 'owen'
        z_db.save()

        assert Person.select().count() == 2

    def test_insert_many(self):
        class User(self.BaseModel):
            username = CharField()

            class Meta:
                db_table = 'users'

        self.models.append(User)
        User.create_table()

        usernames = ['charlie', 'huey', 'zaizee']
        data = [{'username': username} for username in usernames]

        query = User.insert_many(data)
        sql, params = query.sql()
        assert sql == (
            'INSERT INTO "users" ("username") '
            'VALUES (%s), (%s), (%s)')
        assert params == usernames

        res = query.execute()
        assert res is True
        assert User.select().count() == 3
        z = User.select().order_by(-User.username).get()
        assert z.username == 'zaizee'

        usernames = ['foo', 'bar', 'baz']
        data = [{'username': username} for username in usernames]
        query = User.insert_many(data).return_id_list()
        sql, params = query.sql()
        assert sql == (
            'INSERT INTO "users" ("username") '
            'VALUES (%s), (%s), (%s) RETURNING "id"')
        assert params == usernames

        res = list(query.execute())
        assert len(res) == 3
        foo = User.get(User.username == 'foo')
        bar = User.get(User.username == 'bar')
        baz = User.get(User.username == 'baz')
        assert res == [foo.id, bar.id, baz.id]


@skip_unless(lambda: isinstance(test_db, PostgresqlDatabase))
class TestReturningClause(ModelTestCase):
    requires = [User]

    def test_update_returning(self):
        User.create_users(3)
        u1, u2, u3 = [user for user in User.select().order_by(User.id)]

        uq = User.update(username='uII').where(User.id == u2.id)
        res = uq.execute()
        assert res == 1  # Number of rows modified.

        uq = uq.returning(User.username)
        users = [user for user in uq.execute()]
        assert len(users) == 1
        user, = users
        assert user.username == 'uII'
        assert user.id is None  # Was not explicitly selected.

        uq = (User
              .update(username='huey')
              .where(User.username != 'uII')
              .returning(User))
        users = [user for user in uq.execute()]
        assert len(users) == 2
        assert all([user.username == 'huey' for user in users])
        assert all([user.id is not None for user in users])

        uq = uq.dicts().returning(User.username)
        user_data = [data for data in uq.execute()]
        assert user_data == \
               [{'username': 'huey'}, {'username': 'huey'}]

    def test_delete_returning(self):
        User.create_users(10)

        dq = User.delete().where(User.username << ['u9', 'u10'])
        res = dq.execute()
        assert res == 2  # Number of rows modified.

        dq = (User
              .delete()
              .where(User.username << ['u7', 'u8'])
              .returning(User.username))
        users = [user for user in dq.execute()]
        assert len(users) == 2

        usernames = sorted([user.username for user in users])
        assert usernames == ['u7', 'u8']

        ids = [user.id for user in users]
        assert ids == [None, None]  # Was not selected.

        dq = (User
              .delete()
              .where(User.username == 'u1')
              .returning(User))
        users = [user for user in dq.execute()]
        assert len(users) == 1
        user, = users
        assert user.username == 'u1'
        assert user.id is not None

    def test_insert_returning(self):
        iq = User.insert(username='zaizee').returning(User)
        users = [user for user in iq.execute()]
        assert len(users) == 1
        user, = users
        assert user.username == 'zaizee'
        assert user.id is not None

        iq = (User
              .insert_many([
            {'username': 'charlie'},
            {'username': 'huey'},
            {'username': 'connor'},
            {'username': 'leslie'},
            {'username': 'mickey'}])
              .returning(User))
        users = sorted([user for user in iq.tuples().execute()])

        usernames = [username for _, username in users]
        assert usernames == [
            'charlie',
            'huey',
            'connor',
            'leslie',
            'mickey',
        ]

        id_charlie = users[0][0]
        id_mickey = users[-1][0]
        assert id_mickey - id_charlie == 4


class TestModelHash(PeeweeTestCase):
    def test_hash(self):
        class MyUser(User):
            pass

        d = {}
        u1 = User(id=1)
        u2 = User(id=2)
        u3 = User(id=3)
        m1 = MyUser(id=1)
        m2 = MyUser(id=2)
        m3 = MyUser(id=3)

        d[u1] = 'u1'
        d[u2] = 'u2'
        d[m1] = 'm1'
        d[m2] = 'm2'
        assert u1 in d
        assert u2 in d
        assert not (u3 in d)
        assert m1 in d
        assert m2 in d
        assert not (m3 in d)

        assert d[u1] == 'u1'
        assert d[u2] == 'u2'
        assert d[m1] == 'm1'
        assert d[m2] == 'm2'

        un = User()
        mn = MyUser()
        d[un] = 'un'
        d[mn] = 'mn'
        assert un in d  # Hash implementation.
        assert mn in d
        assert d[un] == 'un'
        assert d[mn] == 'mn'


class TestDeleteNullableForeignKeys(ModelTestCase):
    requires = [User, Note, Flag, NoteFlagNullable]

    def test_delete(self):
        u = User.create(username='u')
        n = Note.create(user=u, text='n')
        f = Flag.create(label='f')
        nf1 = NoteFlagNullable.create(note=n, flag=f)
        nf2 = NoteFlagNullable.create(note=n, flag=None)
        nf3 = NoteFlagNullable.create(note=None, flag=f)
        nf4 = NoteFlagNullable.create(note=None, flag=None)

        assert nf1.delete_instance() == 1
        assert nf2.delete_instance() == 1
        assert nf3.delete_instance() == 1
        assert nf4.delete_instance() == 1


class TestJoinNullableForeignKey(ModelTestCase):
    requires = [Parent, Orphan, Child]

    def setUp(self):
        super(TestJoinNullableForeignKey, self).setUp()

        p1 = Parent.create(data='p1')
        p2 = Parent.create(data='p2')
        for i in range(1, 3):
            Child.create(parent=p1, data='child{0!s}-p1'.format(i))
            Child.create(parent=p2, data='child{0!s}-p2'.format(i))
            Orphan.create(parent=p1, data='orphan{0!s}-p1'.format(i))

        Orphan.create(data='orphan1-noparent')
        Orphan.create(data='orphan2-noparent')

    def test_no_empty_instances(self):
        with self.assertQueryCount(1):
            query = (Orphan
                     .select(Orphan, Parent)
                     .join(Parent, JOIN.LEFT_OUTER)
                     .order_by(Orphan.id))
            res = [(orphan.data, orphan.parent is None) for orphan in query]

        assert res == [
            ('orphan1-p1', False),
            ('orphan2-p1', False),
            ('orphan1-noparent', True),
            ('orphan2-noparent', True),
        ]

    def test_unselected_fk_pk(self):
        with self.assertQueryCount(1):
            query = (Orphan
                     .select(Orphan.data, Parent.data)
                     .join(Parent, JOIN.LEFT_OUTER)
                     .order_by(Orphan.id))
            res = [(orphan.data, orphan.parent is None) for orphan in query]

        assert res == [
            ('orphan1-p1', False),
            ('orphan2-p1', False),
            ('orphan1-noparent', False),
            ('orphan2-noparent', False),
        ]

    def test_non_null_fk_unselected_fk(self):
        with self.assertQueryCount(1):
            query = (Child
                     .select(Child.data, Parent.data)
                     .join(Parent, JOIN.LEFT_OUTER)
                     .order_by(Child.id))
            res = [(child.data, child.parent is None) for child in query]

        assert res == [
            ('child1-p1', False),
            ('child1-p2', False),
            ('child2-p1', False),
            ('child2-p2', False),
        ]

        res = [child.parent.data for child in query]
        assert res == ['p1', 'p2', 'p1', 'p2']

        res = [(child._data['parent'], child.parent.id) for child in query]
        assert res == [
            (None, None),
            (None, None),
            (None, None),
            (None, None),
        ]


class TestDefaultDirtyBehavior(PeeweeTestCase):
    def setUp(self):
        super(TestDefaultDirtyBehavior, self).setUp()
        DefaultsModel.drop_table(True)
        DefaultsModel.create_table()

    def test_default_dirty(self):
        DM = DefaultsModel
        DM._meta.only_save_dirty = True

        dm = DM()
        dm.save()

        assert dm.field == 1
        assert dm.control == 1

        dm_db = DM.get((DM.field == 1) & (DM.control == 1))
        assert dm_db.field == 1
        assert dm_db.control == 1

        # No changes.
        assert not dm_db.save()

        dm2 = DM.create()
        assert dm2.field == 3  # One extra when fetched from DB.
        assert dm2.control == 1

        dm._meta.only_save_dirty = False

        dm3 = DM()
        assert dm3.field == 4
        assert dm3.control == 1
        dm3.save()

        dm3_db = DM.get(DM.id == dm3.id)
        assert dm3_db.field == 4
