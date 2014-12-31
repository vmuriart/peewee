# encoding=utf-8

from playhouse.tests.base import ulit
from playhouse.tests.base import compiler
from playhouse.tests.base import ModelTestCase
from playhouse.tests.base import normal_compiler
from playhouse.tests.base import PeeweeTestCase
from playhouse.tests.base import test_db
from playhouse.tests.base import ulit
from playhouse.tests.models import *


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
            u = User.create(username='u%d' % i)
            for j in range(nb):
                b = Blog.create(title='b-%d-%d' % (i, j), content=str(j), user=u)

    def test_select(self):
        self.create_users_blogs()

        users = User.select().where(User.username << ['u0', 'u5']).order_by(User.username)
        self.assertEqual([u.username for u in users], ['u0', 'u5'])

        blogs = Blog.select().join(User).where(
            (User.username << ['u0', 'u3']) &
            (Blog.content == '4')
        ).order_by(Blog.title)
        self.assertEqual([b.title for b in blogs], ['b-0-4', 'b-3-4'])

        users = User.select().paginate(2, 3)
        self.assertEqual([u.username for u in users], ['u3', 'u4', 'u5'])

    def test_select_all(self):
        self.create_users_blogs(2, 2)
        all_cols = SQL('*')
        query = Blog.select(all_cols)
        blogs = [blog for blog in query.order_by(Blog.pk)]
        self.assertEqual(
            [b.title for b in blogs],
            ['b-0-0', 'b-0-1', 'b-1-0', 'b-1-1'])
        self.assertEqual(
            [b.user.username for b in blogs],
            ['u0', 'u0', 'u1', 'u1'])

    def test_select_subquery(self):
        # 10 users, 5 blogs each
        self.create_users_blogs(5, 3)

        # delete user 2's 2nd blog
        Blog.delete().where(Blog.title == 'b-2-2').execute()

        subquery = Blog.select(fn.Count(Blog.pk)).where(Blog.user == User.id).group_by(Blog.user)
        users = User.select(User, subquery.alias('ct')).order_by(R('ct'), User.id)

        self.assertEqual([(x.username, x.ct) for x in users], [
            ('u2', 2),
            ('u0', 3),
            ('u1', 3),
            ('u3', 3),
            ('u4', 3),
        ])

    def test_scalar(self):
        User.create_users(5)

        users = User.select(fn.Count(User.id)).scalar()
        self.assertEqual(users, 5)

        users = User.select(fn.Count(User.id)).where(User.username << ['u1', 'u2'])
        self.assertEqual(users.scalar(), 2)
        self.assertEqual(users.scalar(True), (2,))

        users = User.select(fn.Count(User.id)).where(User.username == 'not-here')
        self.assertEqual(users.scalar(), 0)
        self.assertEqual(users.scalar(True), (0,))

        users = User.select(fn.Count(User.id), fn.Count(User.username))
        self.assertEqual(users.scalar(), 5)
        self.assertEqual(users.scalar(True), (5, 5))

        User.create(username='u1')
        User.create(username='u2')
        User.create(username='u3')
        User.create(username='u99')
        users = User.select(fn.Count(fn.Distinct(User.username))).scalar()
        self.assertEqual(users, 6)

    def test_update(self):
        User.create_users(5)
        uq = User.update(username='u-edited').where(User.username << ['u1', 'u2', 'u3'])
        self.assertEqual([u.username for u in User.select().order_by(User.id)], ['u1', 'u2', 'u3', 'u4', 'u5'])

        uq.execute()
        self.assertEqual([u.username for u in User.select().order_by(User.id)], ['u-edited', 'u-edited', 'u-edited', 'u4', 'u5'])

        self.assertRaises(KeyError, User.update, doesnotexist='invalid')

    def test_update_subquery(self):
        User.create_users(3)
        u1, u2, u3 = [user for user in User.select().order_by(User.id)]
        for i in range(4):
            Blog.create(title='b%s' % i, user=u1)
        for i in range(2):
            Blog.create(title='b%s' % i, user=u3)

        subquery = Blog.select(fn.COUNT(Blog.pk)).where(Blog.user == User.id)
        query = User.update(username=subquery)
        sql, params = normal_compiler.generate_update(query)
        self.assertEqual(sql, (
            'UPDATE "users" SET "username" = ('
            'SELECT COUNT("t2"."pk") FROM "blog" AS t2 '
            'WHERE ("t2"."user_id" = "users"."id"))'))
        self.assertEqual(query.execute(), 3)

        usernames = [u.username for u in User.select().order_by(User.id)]
        self.assertEqual(usernames, ['4', '0', '2'])

    def test_insert(self):
        iq = User.insert(username='u1')
        self.assertEqual(User.select().count(), 0)
        uid = iq.execute()
        self.assertTrue(uid > 0)
        self.assertEqual(User.select().count(), 1)
        u = User.get(User.id==uid)
        self.assertEqual(u.username, 'u1')

        iq = User.insert(doesnotexist='invalid')
        self.assertRaises(KeyError, iq.execute)

    def test_insert_from(self):
        u0, u1, u2 = [User.create(username='U%s' % i) for i in range(3)]

        subquery = (User
                    .select(fn.LOWER(User.username))
                    .where(User.username << ['U0', 'U2']))
        iq = User.insert_from([User.username], subquery)
        sql, params = normal_compiler.generate_insert(iq)
        self.assertEqual(sql, (
            'INSERT INTO "users" ("username") '
            'SELECT LOWER("t2"."username") FROM "users" AS t2 '
            'WHERE ("t2"."username" IN (?, ?))'))
        self.assertEqual(params, ['U0', 'U2'])

        iq.execute()
        usernames = sorted([u.username for u in User.select()])
        self.assertEqual(usernames, ['U0', 'U1', 'U2', 'u0', 'u2'])

    def test_insert_many(self):
        qc = len(self.queries())
        iq = User.insert_many([
            {'username': 'u1'},
            {'username': 'u2'},
            {'username': 'u3'},
            {'username': 'u4'}])
        self.assertTrue(iq.execute())

        qc2 = len(self.queries())
        if test_db.insert_many:
            self.assertEqual(qc2 - qc, 1)
        else:
            self.assertEqual(qc2 - qc, 4)
        self.assertEqual(User.select().count(), 4)

        sq = User.select(User.username).order_by(User.username)
        self.assertEqual([u.username for u in sq], ['u1', 'u2', 'u3', 'u4'])

        iq = User.insert_many([{'username': 'u5'}])
        self.assertTrue(iq.execute())
        self.assertEqual(User.select().count(), 5)

        iq = User.insert_many([
            {User.username: 'u6'},
            {User.username: 'u7'},
            {'username': 'u8'}]).execute()

        sq = User.select(User.username).order_by(User.username)
        self.assertEqual([u.username for u in sq],
                         ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8'])

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
            self.assertTrue(iq.execute())

        self.assertEqual(User.select().count(), 4)

    def test_delete(self):
        User.create_users(5)
        dq = User.delete().where(User.username << ['u1', 'u2', 'u3'])
        self.assertEqual(User.select().count(), 5)
        nr = dq.execute()
        self.assertEqual(nr, 3)
        self.assertEqual([u.username for u in User.select()], ['u4', 'u5'])

    def test_raw(self):
        User.create_users(3)
        interpolation = test_db.interpolation

        with self.assertQueryCount(1):
            query = 'select * from users where username IN (%s, %s)' % (
                interpolation, interpolation)
            rq = User.raw(query, 'u1', 'u3')
            self.assertEqual([u.username for u in rq], ['u1', 'u3'])

            # iterate again
            self.assertEqual([u.username for u in rq], ['u1', 'u3'])

        query = ('select id, username, %s as secret '
                 'from users where username = %s')
        rq = User.raw(
            query % (interpolation, interpolation),
            'sh', 'u2')
        self.assertEqual([u.secret for u in rq], ['sh'])
        self.assertEqual([u.username for u in rq], ['u2'])

        rq = User.raw('select count(id) from users')
        self.assertEqual(rq.scalar(), 3)

        rq = User.raw('select username from users').tuples()
        self.assertEqual([r for r in rq], [
            ('u1',), ('u2',), ('u3',),
        ])

    def test_limits_offsets(self):
        for i in range(10):
            User.create(username='u%d' % i)
        sq = User.select().order_by(User.id)

        offset_no_lim = sq.offset(3)
        self.assertEqual(
            [u.username for u in offset_no_lim],
            ['u%d' % i for i in range(3, 10)]
        )

        offset_with_lim = sq.offset(5).limit(3)
        self.assertEqual(
            [u.username for u in offset_with_lim],
            ['u%d' % i for i in range(5, 8)]
        )

    def test_raw_fn(self):
        self.create_users_blogs(3, 2)  # 3 users, 2 blogs each.
        query = User.raw('select count(1) as ct from blog group by user_id')
        results = [x.ct for x in query]
        self.assertEqual(results, [2, 2, 2])

    def test_model_iter(self):
        self.create_users_blogs(3, 2)
        usernames = [user.username for user in User]
        self.assertEqual(sorted(usernames), ['u0', 'u1', 'u2'])

        blogs = list(Blog)
        self.assertEqual(len(blogs), 6)


class TestModelAPIs(ModelTestCase):
    requires = [User, Blog, Category, UserCategory]

    def test_related_name(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        b11 = Blog.create(user=u1, title='b11')
        b12 = Blog.create(user=u1, title='b12')
        b2 = Blog.create(user=u2, title='b2')

        self.assertEqual(
            [b.title for b in u1.blog_set.order_by(Blog.title)],
            ['b11', 'b12'])
        self.assertEqual(
            [b.title for b in u2.blog_set.order_by(Blog.title)],
            ['b2'])

    def test_related_name_collision(self):
        class Foo(TestModel):
            f1 = CharField()

        def make_klass():
            class FooRel(TestModel):
                foo = ForeignKeyField(Foo, related_name='f1')

        self.assertRaises(AttributeError, make_klass)

    def test_fk_exceptions(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(parent=c1, name='c2')
        self.assertEqual(c1.parent, None)
        self.assertEqual(c2.parent, c1)

        c2_db = Category.get(Category.id == c2.id)
        self.assertEqual(c2_db.parent, c1)

        u = User.create(username='u1')
        b = Blog.create(user=u, title='b')
        b2 = Blog(title='b2')

        self.assertEqual(b.user, u)
        self.assertRaises(User.DoesNotExist, getattr, b2, 'user')

    def test_fk_cache_invalidated(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        b = Blog.create(user=u1, title='b')

        blog = Blog.get(Blog.pk == b)
        with self.assertQueryCount(1):
            self.assertEqual(blog.user.id, u1.id)

        blog.user = u2.id
        with self.assertQueryCount(1):
            self.assertEqual(blog.user.id, u2.id)

        # No additional query.
        blog.user = u2.id
        with self.assertQueryCount(0):
            self.assertEqual(blog.user.id, u2.id)

    def test_fk_ints(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(name='c2', parent=c1.id)
        c2_db = Category.get(Category.id == c2.id)
        self.assertEqual(c2_db.parent, c1)

    def test_fk_caching(self):
        c1 = Category.create(name='c1')
        c2 = Category.create(name='c2', parent=c1)
        c2_db = Category.get(Category.id == c2.id)

        with self.assertQueryCount(1):
            parent = c2_db.parent
            self.assertEqual(parent, c1)

            parent = c2_db.parent

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

            self.assertEqual(
                [(c.name, c.parent.name, c.parent.parent.name) for c in sq],
                [('c1', 'p1', 'g1'), ('c11', 'p1', 'g1')])

    def test_creation(self):
        User.create_users(10)
        self.assertEqual(User.select().count(), 10)

    def test_saving(self):
        self.assertEqual(User.select().count(), 0)

        u = User(username='u1')
        self.assertEqual(u.save(), 1)
        u.username = 'u2'
        self.assertEqual(u.save(), 1)

        self.assertEqual(User.select().count(), 1)

        self.assertEqual(u.delete_instance(), 1)
        self.assertEqual(u.save(), 0)

    def test_modify_model_cause_it_dirty(self):
        u = User(username='u1')
        u.save()
        self.assertFalse(u.is_dirty())

        u.username = 'u2'
        self.assertTrue(u.is_dirty())
        self.assertEqual(u.dirty_fields, [User.username])

        u.save()
        self.assertFalse(u.is_dirty())

        b = Blog.create(user=u, title='b1')
        self.assertFalse(b.is_dirty())

        b.user = u
        self.assertTrue(b.is_dirty())
        self.assertEqual(b.dirty_fields, [Blog.user])

    def test_dirty_from_query(self):
        u1 = User.create(username='u1')
        b1 = Blog.create(title='b1', user=u1)
        b2 = Blog.create(title='b2', user=u1)

        u_db = User.get()
        self.assertFalse(u_db.is_dirty())

        b_with_u = (Blog
                    .select(Blog, User)
                    .join(User)
                    .where(Blog.title == 'b2')
                    .get())
        self.assertFalse(b_with_u.is_dirty())
        self.assertFalse(b_with_u.user.is_dirty())

        u_with_blogs = (User
                        .select(User, Blog)
                        .join(Blog)
                        .order_by(Blog.title)
                        .aggregate_rows())[0]
        self.assertFalse(u_with_blogs.is_dirty())
        for blog in u_with_blogs.blog_set:
            self.assertFalse(blog.is_dirty())

        b_with_users = (Blog
                        .select(Blog, User)
                        .join(User)
                        .order_by(Blog.title)
                        .aggregate_rows())
        b1, b2 = b_with_users
        self.assertFalse(b1.is_dirty())
        self.assertFalse(b1.user.is_dirty())
        self.assertFalse(b2.is_dirty())
        self.assertFalse(b2.user.is_dirty())

    def test_save_only(self):
        u = User.create(username='u')
        b = Blog.create(user=u, title='b1', content='ct')
        b.title = 'b1-edit'
        b.content = 'ct-edit'

        b.save(only=[Blog.title])

        b_db = Blog.get(Blog.pk == b.pk)
        self.assertEqual(b_db.title, 'b1-edit')
        self.assertEqual(b_db.content, 'ct')

        b = Blog(user=u, title='b2', content='foo')
        b.save(only=[Blog.user, Blog.title])

        b_db = Blog.get(Blog.pk == b.pk)

        self.assertEqual(b_db.title, 'b2')
        self.assertEqual(b_db.content, '')

    def test_save_only_dirty_fields(self):
        u = User.create(username='u1')
        b = Blog.create(title='b1', user=u, content='huey')
        b_db = Blog.get(Blog.pk == b.pk)
        b.title = 'baby huey'
        b.save(only=b.dirty_fields)
        b_db.content = 'mickey-nugget'
        b_db.save(only=b_db.dirty_fields)
        saved = Blog.get(Blog.pk == b.pk)
        self.assertEqual(saved.title, 'baby huey')
        self.assertEqual(saved.content, 'mickey-nugget')

    def test_zero_id(self):
        if isinstance(test_db, MySQLDatabase):
            # Need to explicitly tell MySQL it's OK to use zero.
            test_db.execute_sql("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'")
        query = 'insert into users (id, username) values (%s, %s)' % (
            test_db.interpolation, test_db.interpolation)
        test_db.execute_sql(query, (0, 'foo'))
        Blog.insert(title='foo2', user=0).execute()

        u = User.get(User.id == 0)
        b = Blog.get(Blog.user == u)

        self.assertTrue(u == u)
        self.assertTrue(u == b.user)

    def test_saving_via_create_gh111(self):
        u = User.create(username='u')
        b = Blog.create(title='foo', user=u)
        last_sql, _ = self.queries()[-1]
        self.assertFalse('pub_date' in last_sql)
        self.assertEqual(b.pub_date, None)

        b2 = Blog(title='foo2', user=u)
        b2.save()
        last_sql, _ = self.queries()[-1]
        self.assertFalse('pub_date' in last_sql)
        self.assertEqual(b2.pub_date, None)

    def test_reading(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        self.assertEqual(u1, User.get(username='u1'))
        self.assertEqual(u2, User.get(username='u2'))
        self.assertFalse(u1 == u2)

        self.assertEqual(u1, User.get(User.username == 'u1'))
        self.assertEqual(u2, User.get(User.username == 'u2'))

    def test_get_or_create(self):
        u1 = User.get_or_create(username='u1')
        u1_x = User.get_or_create(username='u1')
        self.assertEqual(u1.id, u1_x.id)
        self.assertEqual(User.select().count(), 1)

    def test_first(self):
        users = User.create_users(5)

        with self.assertQueryCount(1):
            sq = User.select().order_by(User.username)
            qr = sq.execute()

            # call it once
            first = sq.first()
            self.assertEqual(first.username, 'u1')

            # check the result cache
            self.assertEqual(len(qr._result_cache), 1)

            # call it again and we get the same result, but not an
            # extra query
            self.assertEqual(sq.first().username, 'u1')

        with self.assertQueryCount(0):
            usernames = [u.username for u in sq]
            self.assertEqual(usernames, ['u1', 'u2', 'u3', 'u4', 'u5'])

        with self.assertQueryCount(0):
            # call after iterating
            self.assertEqual(sq.first().username, 'u1')

            usernames = [u.username for u in sq]
            self.assertEqual(usernames, ['u1', 'u2', 'u3', 'u4', 'u5'])

        # call it with an empty result
        sq = User.select().where(User.username == 'not-here')
        self.assertEqual(sq.first(), None)

    def test_deleting(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        self.assertEqual(User.select().count(), 2)
        u1.delete_instance()
        self.assertEqual(User.select().count(), 1)

        self.assertEqual(u2, User.get(User.username=='u2'))

    def test_counting(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')

        for u in [u1, u2]:
            for i in range(5):
                Blog.create(title='b-%s-%s' % (u.username, i), user=u)

        uc = User.select().where(User.username == 'u1').join(Blog).count()
        self.assertEqual(uc, 5)

        uc = User.select().where(User.username == 'u1').join(Blog).distinct().count()
        self.assertEqual(uc, 1)

        self.assertEqual(Blog.select().limit(4).offset(3).count(), 4)
        self.assertEqual(Blog.select().limit(4).offset(3).count(True), 10)

        # Calling `distinct()` will result in a call to wrapped_count().
        uc = User.select().join(Blog).distinct().count()
        self.assertEqual(uc, 2)

        # Test with clear limit = True.
        self.assertEqual(User.select().limit(1).count(clear_limit=True), 2)
        self.assertEqual(
            User.select().limit(1).wrapped_count(clear_limit=True), 2)

        # Test with clear limit = False.
        self.assertEqual(User.select().limit(1).count(clear_limit=False), 1)
        self.assertEqual(
            User.select().limit(1).wrapped_count(clear_limit=False), 1)

    def test_ordering(self):
        u1 = User.create(username='u1')
        u2 = User.create(username='u2')
        u3 = User.create(username='u2')
        users = User.select().order_by(User.username.desc(), User.id.desc())
        self.assertEqual([u._get_pk_value() for u in users], [u3.id, u2.id, u1.id])

    def test_count_transaction(self):
        for i in range(10):
            User.create(username='u%d' % i)

        with test_db.transaction():
            for user in User.select():
                for i in range(20):
                    Blog.create(user=user, title='b-%d-%d' % (user.id, i))

        count = Blog.select().count()
        self.assertEqual(count, 200)

    def test_exists(self):
        u1 = User.create(username='u1')
        self.assertTrue(User.select().where(User.username == 'u1').exists())
        self.assertFalse(User.select().where(User.username == 'u2').exists())

    def test_unicode(self):
        # create a unicode literal
        ustr = ulit('Lýðveldið Ísland')
        u = User.create(username=ustr)

        # query using the unicode literal
        u_db = User.get(User.username == ustr)

        # the db returns a unicode literal
        self.assertEqual(u_db.username, ustr)

        # delete the user
        self.assertEqual(u.delete_instance(), 1)

        # convert the unicode to a utf8 string
        utf8_str = ustr.encode('utf-8')

        # create using the utf8 string
        u2 = User.create(username=utf8_str)

        # query using unicode literal
        u2_db = User.get(User.username == ustr)

        # we get unicode back
        self.assertEqual(u2_db.username, ustr)

    def test_unicode_issue202(self):
        ustr = ulit('M\u00f6rk')
        user = User.create(username=ustr)
        self.assertEqual(user.username, ustr)


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
            user = User.create(username='u-%d' % i)
            for j in range(2):
                ct += 1
                Blog.create(
                    user=user,
                    title='b-%d-%d' % (i, j),
                    pub_date=datetime.datetime(2013, 1, ct))
            users.append(user)
        return users

    def test_annotate_int(self):
        users = self.create_user_blogs()
        annotated = User.select().annotate(Blog, fn.Count(Blog.pk).alias('ct'))
        for i, user in enumerate(annotated):
            self.assertEqual(user.ct, 2)
            self.assertEqual(user.username, 'u-%d' % i)

    def test_annotate_datetime(self):
        users = self.create_user_blogs()
        annotated = (User
                     .select()
                     .annotate(Blog, fn.Max(Blog.pub_date).alias('max_pub')))
        user_0, user_1 = annotated
        self.assertEqual(user_0.max_pub, datetime.datetime(2013, 1, 2))
        self.assertEqual(user_1.max_pub, datetime.datetime(2013, 1, 4))

    def test_aggregate_int(self):
        models = self.create_ordered_models()
        max_id = OrderedModel.select().aggregate(fn.Max(OrderedModel.id))
        self.assertEqual(max_id, models[-1].id)

    def test_aggregate_datetime(self):
        models = self.create_ordered_models()
        max_created = (OrderedModel
                       .select()
                       .aggregate(fn.Max(OrderedModel.created)))
        self.assertEqual(max_created, models[-1].created)


class TestMultiTableFromClause(ModelTestCase):
    requires = [Blog, Comment, User]

    def setUp(self):
        super(TestMultiTableFromClause, self).setUp()

        for u in range(2):
            user = User.create(username='u%s' % u)
            for i in range(3):
                b = Blog.create(user=user, title='b%s-%s' % (u, i))
                for j in range(i):
                    Comment.create(blog=b, comment='c%s-%s' % (i, j))

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
            self.assertEqual(blogs, ['b0-0', 'b0-1', 'b0-2'])

            usernames = [b.username for b in q]
            self.assertEqual(usernames, ['u0', 'u0', 'u0'])

    def test_subselect(self):
        inner = User.select(User.username)
        self.assertEqual(
            [u.username for u in inner.order_by(User.username)], ['u0', 'u1'])

        # Have to manually specify the alias as "t1" because the outer query
        # will expect that.
        outer = (User
                 .select(User.username)
                 .from_(inner.alias('t1')))
        sql, params = compiler.generate_select(outer)
        self.assertEqual(sql, (
            'SELECT "users"."username" FROM '
            '(SELECT "users"."username" FROM "users" AS users) AS t1'))

        self.assertEqual(
            [u.username for u in outer.order_by(User.username)], ['u0', 'u1'])

    def test_subselect_with_column(self):
        inner = User.select(User.username.alias('name')).alias('t1')
        outer = (User
                 .select(inner.c.name)
                 .from_(inner))
        sql, params = compiler.generate_select(outer)
        self.assertEqual(sql, (
            'SELECT "t1"."name" FROM '
            '(SELECT "users"."username" AS name FROM "users" AS users) AS t1'))

        query = outer.order_by(inner.c.name.desc())
        self.assertEqual([u[0] for u in query.tuples()], ['u1', 'u0'])

    def test_subselect_with_join(self):
        inner = User.select(User.id, User.username).alias('q1')
        outer = (Blog
                 .select(inner.c.id, inner.c.username)
                 .from_(inner)
                 .join(Comment, on=(inner.c.id == Comment.id)))
        sql, params = compiler.generate_select(outer)
        self.assertEqual(sql, (
            'SELECT "q1"."id", "q1"."username" FROM ('
            'SELECT "users"."id", "users"."username" FROM "users" AS users) AS q1 '
            'INNER JOIN "comment" AS comment ON ("q1"."id" = "comment"."id")'))

    def test_join_on_query(self):
        u0 = User.get(User.username == 'u0')
        u1 = User.get(User.username == 'u1')

        inner = User.select().alias('j1')
        outer = (Blog
                 .select(Blog.title, Blog.user)
                 .join(inner, on=(Blog.user == inner.c.id))
                 .order_by(Blog.pk))
        res = [row for row in outer.tuples()]
        self.assertEqual(res, [
            ('b0-0', u0.id),
            ('b0-1', u0.id),
            ('b0-2', u0.id),
            ('b1-0', u1.id),
            ('b1-1', u1.id),
            ('b1-2', u1.id),
        ])

class TestDeleteRecursive(ModelTestCase):
    requires = [
        Parent, Child, Orphan, ChildPet, OrphanPet, Package, PackageItem]

    def setUp(self):
        super(TestDeleteRecursive, self).setUp()
        p1 = Parent.create(data='p1')
        p2 = Parent.create(data='p2')
        c11 = Child.create(parent=p1)
        c12 = Child.create(parent=p1)
        c21 = Child.create(parent=p2)
        c22 = Child.create(parent=p2)
        o11 = Orphan.create(parent=p1)
        o12 = Orphan.create(parent=p1)
        o21 = Orphan.create(parent=p2)
        o22 = Orphan.create(parent=p2)
        ChildPet.create(child=c11)
        ChildPet.create(child=c12)
        ChildPet.create(child=c21)
        ChildPet.create(child=c22)
        OrphanPet.create(orphan=o11)
        OrphanPet.create(orphan=o12)
        OrphanPet.create(orphan=o21)
        OrphanPet.create(orphan=o22)
        self.p1 = p1
        self.p2 = p2

    def test_recursive_update(self):
        self.p1.delete_instance(recursive=True)
        counts = (
            #query,fk,p1,p2,tot
            (Child.select(), Child.parent, 0, 2, 2),
            (Orphan.select(), Orphan.parent, 0, 2, 4),
            (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
            (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 4),
        )

        for query, fk, p1_ct, p2_ct, tot in counts:
            self.assertEqual(query.where(fk == self.p1).count(), p1_ct)
            self.assertEqual(query.where(fk == self.p2).count(), p2_ct)
            self.assertEqual(query.count(), tot)

    def test_recursive_delete(self):
        self.p1.delete_instance(recursive=True, delete_nullable=True)
        counts = (
            #query,fk,p1,p2,tot
            (Child.select(), Child.parent, 0, 2, 2),
            (Orphan.select(), Orphan.parent, 0, 2, 2),
            (ChildPet.select().join(Child), Child.parent, 0, 2, 2),
            (OrphanPet.select().join(Orphan), Orphan.parent, 0, 2, 2),
        )

        for query, fk, p1_ct, p2_ct, tot in counts:
            self.assertEqual(query.where(fk == self.p1).count(), p1_ct)
            self.assertEqual(query.where(fk == self.p2).count(), p2_ct)
            self.assertEqual(query.count(), tot)

    def test_recursive_non_pk_fk(self):
        for i in range(3):
            Package.create(barcode=str(i))
            for j in range(4):
                PackageItem.create(package=str(i), title='%s-%s' % (i, j))

        self.assertEqual(Package.select().count(), 3)
        self.assertEqual(PackageItem.select().count(), 12)

        Package.get(Package.barcode == '1').delete_instance(recursive=True)

        self.assertEqual(Package.select().count(), 2)
        self.assertEqual(PackageItem.select().count(), 8)

        items = (PackageItem
                 .select(PackageItem.title)
                 .order_by(PackageItem.id)
                 .tuples())
        self.assertEqual([i[0] for i in items], [
            '0-0', '0-1', '0-2', '0-3',
            '2-0', '2-1', '2-2', '2-3',
        ])


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
            self.assertEqual([u.username for u in q.order_by(User.username)], exp)
        def aC(q, exp):
            self.assertEqual([c.name for c in q.order_by(Category.name)], exp)

        users = User.select().join(UserCategory).join(Category).where(Category.name == 'c1')
        aU(users, ['u1'])

        users = User.select().join(UserCategory).join(Category).where(Category.name == 'c3')
        aU(users, [])

        cats = Category.select().join(UserCategory).join(User).where(User.username == 'u1')
        aC(cats, ['c1', 'c12'])

        cats = Category.select().join(UserCategory).join(User).where(User.username == 'u2')
        aC(cats, ['c12', 'c2', 'c23'])

        cats = Category.select().join(UserCategory).join(User).where(User.username == 'u3')
        aC(cats, [])

        cats = Category.select().join(UserCategory).join(User).where(
            Category.name << ['c1', 'c2', 'c3']
        )
        aC(cats, ['c1', 'c2'])

        cats = Category.select().join(UserCategory, JOIN_LEFT_OUTER).join(User, JOIN_LEFT_OUTER).where(
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

        self.assertEqual(results, {
            'c1': set(['u1']),
            'c12': set(['u1', 'u2']),
            'c2': set(['u2']),
            'c23': set(['u2']),
            'c3': set(),
        })
        self.assertEqual(
            sorted(result_list),
            ['c1', 'c12', 'c2', 'c23', 'c3', 'u1', 'u1', 'u2', 'u2', 'u2'])


class TestModelOptionInheritance(PeeweeTestCase):
    def test_db_table(self):
        self.assertEqual(User._meta.db_table, 'users')

        class Foo(TestModel):
            pass
        self.assertEqual(Foo._meta.db_table, 'foo')

        class Foo2(TestModel):
            pass
        self.assertEqual(Foo2._meta.db_table, 'foo2')

        class Foo_3(TestModel):
            pass
        self.assertEqual(Foo_3._meta.db_table, 'foo_3')

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

        self.assertEqual(A._meta.a, 'a')
        self.assertEqual(B1._meta.a, 'a')
        self.assertEqual(B2._meta.a, 'a')
        self.assertEqual(B1._meta.b, 1)
        self.assertEqual(B2._meta.b, 2)

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

        self.assertEqual(ParentModel._meta.database.database, 'testing.db')
        self.assertEqual(ParentModel._meta.model_class, ParentModel)

        self.assertEqual(ChildModel._meta.database.database, 'testing.db')
        self.assertEqual(ChildModel._meta.model_class, ChildModel)
        self.assertEqual(sorted(ChildModel._meta.fields.keys()), [
            'id', 'title', 'user'
        ])

        self.assertEqual(ChildModel2._meta.database.database, 'child2.db')
        self.assertEqual(ChildModel2._meta.model_class, ChildModel2)
        self.assertEqual(sorted(ChildModel2._meta.fields.keys()), [
            'id', 'special_field', 'title', 'user'
        ])

        self.assertEqual(GrandChildModel._meta.database.database, 'testing.db')
        self.assertEqual(GrandChildModel._meta.model_class, GrandChildModel)
        self.assertEqual(sorted(GrandChildModel._meta.fields.keys()), [
            'id', 'title', 'user'
        ])

        self.assertEqual(GrandChildModel2._meta.database.database, 'child2.db')
        self.assertEqual(GrandChildModel2._meta.model_class, GrandChildModel2)
        self.assertEqual(sorted(GrandChildModel2._meta.fields.keys()), [
            'id', 'special_field', 'title', 'user'
        ])
        self.assertTrue(isinstance(GrandChildModel2._meta.fields['special_field'], TextField))

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
        self.assertTrue(isinstance(foo_order_by, Field))
        self.assertTrue(foo_order_by.model_class is Foo)
        self.assertEqual(foo_order_by.name, 'created')

        bar_order_by = Bar._meta.order_by[0]
        self.assertTrue(isinstance(bar_order_by, Field))
        self.assertTrue(bar_order_by.model_class is Bar)
        self.assertEqual(bar_order_by.name, 'val')


class TestModelInheritance(ModelTestCase):
    requires = [Blog, BlogTwo, User]

    def test_model_inheritance_attrs(self):
        self.assertEqual(Blog._meta.get_field_names(), ['pk', 'user', 'title', 'content', 'pub_date'])
        self.assertEqual(BlogTwo._meta.get_field_names(), ['pk', 'user', 'content', 'pub_date', 'title', 'extra_field'])

        self.assertEqual(Blog._meta.primary_key.name, 'pk')
        self.assertEqual(BlogTwo._meta.primary_key.name, 'pk')

        self.assertEqual(Blog.user.related_name, 'blog_set')
        self.assertEqual(BlogTwo.user.related_name, 'blogtwo_set')

        self.assertEqual(User.blog_set.rel_model, Blog)
        self.assertEqual(User.blogtwo_set.rel_model, BlogTwo)

        self.assertFalse(BlogTwo._meta.db_table == Blog._meta.db_table)

    def test_model_inheritance_flow(self):
        u = User.create(username='u')

        b = Blog.create(title='b', user=u)
        b2 = BlogTwo.create(title='b2', extra_field='foo', user=u)

        self.assertEqual(list(u.blog_set), [b])
        self.assertEqual(list(u.blogtwo_set), [b2])

        self.assertEqual(Blog.select().count(), 1)
        self.assertEqual(BlogTwo.select().count(), 1)

        b_from_db = Blog.get(Blog.pk==b.pk)
        b2_from_db = BlogTwo.get(BlogTwo.pk==b2.pk)

        self.assertEqual(b_from_db.user, u)
        self.assertEqual(b2_from_db.user, u)
        self.assertEqual(b2_from_db.extra_field, 'foo')