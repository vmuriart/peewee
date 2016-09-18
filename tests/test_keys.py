# -*- coding: utf-8 -*-

import pytest

from peewee import (CharField, DecimalField, DeferredRelation, ForeignKeyField,
                    IntegrityError, JOIN, Model, OperationalError,
                    SqliteDatabase, TextField, fn)
from tests.base import (ModelTestCase, PeeweeTestCase, TestModel, compiler,
                        database_initializer, test_db)
from tests.models import (Blog, Component, CompositeKeyModel, Computer,
                          Language, Manufacturer, Package, PackageItem, Post,
                          Relationship, Snippet, Tag, TagPostThrough, User,
                          UserThing)


class TestForeignKeyToNonPrimaryKey(ModelTestCase):
    requires = [Package, PackageItem]

    def setUp(self):
        super(TestForeignKeyToNonPrimaryKey, self).setUp()

        for barcode in ['101', '102']:
            Package.create(barcode=barcode)
            for i in range(2):
                PackageItem.create(
                    package=barcode,
                    title='{0!s}-{1!s}'.format(barcode, i))

    def test_fk_resolution(self):
        pi = PackageItem.get(PackageItem.title == '101-0')
        assert pi._data['package'] == '101'
        assert pi.package == Package.get(Package.barcode == '101')

    def test_select_generation(self):
        p = Package.get(Package.barcode == '101')
        assert ([item.title for item in p.items.order_by(PackageItem.title)] ==
                ['101-0', '101-1'])


class TestMultipleForeignKey(ModelTestCase):
    requires = [Manufacturer, Component, Computer]
    test_values = [
        ['3TB', '16GB', 'i7'],
        ['128GB', '1GB', 'ARM']]

    def setUp(self):
        super(TestMultipleForeignKey, self).setUp()
        intel = Manufacturer.create(name='Intel')
        amd = Manufacturer.create(name='AMD')
        kingston = Manufacturer.create(name='Kingston')
        for hard_drive, memory, processor in self.test_values:
            c = Computer.create(
                hard_drive=Component.create(name=hard_drive),
                memory=Component.create(name=memory, manufacturer=kingston),
                processor=Component.create(name=processor, manufacturer=intel))

        # The 2nd computer has an AMD processor.
        c.processor.manufacturer = amd
        c.processor.save()

    def test_multi_join(self):
        HDD = Component.alias()
        HDDMf = Manufacturer.alias()
        Memory = Component.alias()
        MemoryMf = Manufacturer.alias()
        Processor = Component.alias()
        ProcessorMf = Manufacturer.alias()
        query = (Computer.select(Computer,
                                 HDD,
                                 Memory,
                                 Processor,
                                 HDDMf,
                                 MemoryMf,
                                 ProcessorMf)
                 .join(HDD,
                       on=(Computer.hard_drive == HDD.id).alias('hard_drive'))
                 .join(HDDMf, JOIN.LEFT_OUTER,
                       on=(HDD.manufacturer == HDDMf.id))
                 .switch(Computer)
                 .join(Memory,
                       on=(Computer.memory == Memory.id).alias('memory'))
                 .join(MemoryMf, JOIN.LEFT_OUTER,
                       on=(Memory.manufacturer == MemoryMf.id))
                 .switch(Computer)
                 .join(Processor,
                       on=(Computer.processor == Processor.id).alias(
                           'processor'))
                 .join(ProcessorMf, JOIN.LEFT_OUTER,
                       on=(Processor.manufacturer == ProcessorMf.id))
                 .order_by(Computer.id))

        with self.assertQueryCount(1):
            vals = []
            manufacturers = []
            for computer in query:
                components = [computer.hard_drive,
                              computer.memory,
                              computer.processor]
                vals.append([component.name for component in components])
                for component in components:
                    if component.manufacturer:
                        manufacturers.append(component.manufacturer.name)
                    else:
                        manufacturers.append(None)

            assert vals == self.test_values
            assert manufacturers == [None, 'Kingston', 'Intel',
                                     None, 'Kingston', 'AMD']


class TestMultipleForeignKeysJoining(ModelTestCase):
    requires = [User, Relationship]

    def test_multiple_fks(self):
        a = User.create(username='a')
        b = User.create(username='b')
        c = User.create(username='c')

        assert list(a.relationships) == []
        assert list(a.related_to) == []

        r_ab = Relationship.create(from_user=a, to_user=b)
        assert list(a.relationships) == [r_ab]
        assert list(a.related_to) == []
        assert list(b.relationships) == []
        assert list(b.related_to) == [r_ab]

        Relationship.create(from_user=b, to_user=c)

        following = User.select().join(Relationship, on=Relationship.to_user
                                       ).where(Relationship.from_user == a)
        assert list(following) == [b]

        followers = User.select().join(Relationship, on=Relationship.from_user
                                       ).where(Relationship.to_user == a.id)
        assert list(followers) == []

        following = User.select().join(Relationship, on=Relationship.to_user
                                       ).where(Relationship.from_user == b.id)
        assert list(following) == [c]

        followers = User.select().join(Relationship, on=Relationship.from_user
                                       ).where(Relationship.to_user == b.id)
        assert list(followers) == [a]

        following = User.select().join(Relationship, on=Relationship.to_user
                                       ).where(Relationship.from_user == c.id)
        assert list(following) == []

        followers = User.select().join(Relationship, on=Relationship.from_user
                                       ).where(Relationship.to_user == c.id)
        assert list(followers) == [b]


class TestCompositePrimaryKey(ModelTestCase):
    requires = [Tag, Post, TagPostThrough, CompositeKeyModel, User, UserThing]

    def setUp(self):
        super(TestCompositePrimaryKey, self).setUp()
        tags = [Tag.create(tag='t{0:d}'.format(i)) for i in range(1, 4)]
        posts = [Post.create(title='p{0:d}'.format(i)) for i in range(1, 4)]
        p12 = Post.create(title='p12')
        for t, p in zip(tags, posts):
            TagPostThrough.create(tag=t, post=p)
        TagPostThrough.create(tag=tags[0], post=p12)
        TagPostThrough.create(tag=tags[1], post=p12)

    def test_create_table_query(self):
        query, params = compiler.create_table(TagPostThrough)
        assert (query == 'CREATE TABLE "tagpostthrough" '
                         '("tag_id" INTEGER NOT NULL, '
                         '"post_id" INTEGER NOT NULL, '
                         'PRIMARY KEY ("tag_id", "post_id"), '
                         'FOREIGN KEY ("tag_id") REFERENCES "tag" ("id"), '
                         'FOREIGN KEY ("post_id") REFERENCES "post" ("id")'
                         ')')

    def test_get_set_id(self):
        tpt = (TagPostThrough.select()
               .join(Tag)
               .switch(TagPostThrough)
               .join(Post)
               .order_by(Tag.tag, Post.title)).get()
        # Sanity check.
        assert tpt.tag.tag == 't1'
        assert tpt.post.title == 'p1'

        tag = Tag.select().where(Tag.tag == 't1').get()
        post = Post.select().where(Post.title == 'p1').get()
        assert tpt._get_pk_value() == (tag, post)

        # set_id is a no-op.
        tpt._set_pk_value(None)
        assert tpt._get_pk_value() == (tag, post)

    def test_querying(self):
        posts = (Post.select()
                 .join(TagPostThrough)
                 .join(Tag)
                 .where(Tag.tag == 't1')
                 .order_by(Post.title))
        assert [p.title for p in posts] == ['p1', 'p12']

        tags = (Tag.select()
                .join(TagPostThrough)
                .join(Post)
                .where(Post.title == 'p12')
                .order_by(Tag.tag))
        assert [t.tag for t in tags] == ['t1', 't2']

    def test_composite_key_model(self):
        CKM = CompositeKeyModel
        values = [('a', 1, 1.0),
                  ('a', 2, 2.0),
                  ('b', 1, 1.0),
                  ('b', 2, 2.0)]
        c1, c2, c3, c4 = [CKM.create(f1=f1, f2=f2, f3=f3) for f1, f2, f3 in
                          values]

        # Update a single row, giving it a new value for `f3`.
        CKM.update(f3=3.0).where((CKM.f1 == 'a') & (CKM.f2 == 2)).execute()

        c = CKM.get((CKM.f1 == 'a') & (CKM.f2 == 2))
        assert c.f3 == 3.0

        # Update the `f3` value and call `save()`, triggering an update.
        c3.f3 = 4.0
        c3.save()

        c = CKM.get((CKM.f1 == 'b') & (CKM.f2 == 1))
        assert c.f3 == 4.0

        # Only 1 row updated.
        query = CKM.select().where(CKM.f3 == 4.0)
        assert query.wrapped_count() == 1

        # Unfortunately this does not work since the original value of the
        # PK is lost (and hence cannot be used to update).
        c4.f1 = 'c'
        c4.save()
        with pytest.raises(CKM.DoesNotExist):
            CKM.get((CKM.f1 == 'c') & (CKM.f2 == 2))

    def test_count_composite_key(self):
        CKM = CompositeKeyModel
        values = [('a', 1, 1.0),
                  ('a', 2, 2.0),
                  ('b', 1, 1.0),
                  ('b', 2, 1.0)]
        for f1, f2, f3 in values:
            CKM.create(f1=f1, f2=f2, f3=f3)

        assert CKM.select().wrapped_count() == 4
        assert CKM.select().count() == 4
        assert CKM.select().where((CKM.f1 == 'a') &
                                  (CKM.f2 == 1)).exists()
        assert not CKM.select().where((CKM.f1 == 'a') &
                                      (CKM.f2 == 3)).exists()

    def test_delete_instance(self):
        u1, u2 = [User.create(username='u{0!s}'.format(i)) for i in range(2)]
        ut1 = UserThing.create(thing='t1', user=u1)
        UserThing.create(thing='t2', user=u1)
        UserThing.create(thing='t1', user=u2)
        UserThing.create(thing='t3', user=u2)

        res = ut1.delete_instance()
        assert res == 1
        assert ([x.thing for x in (UserThing.select()
                                   .order_by(UserThing.thing))] ==
                ['t1', 't2', 't3'])


class TestForeignKeyNonPrimaryKeyCreateTable(PeeweeTestCase):
    def test_create_table(self):
        class A(TestModel):
            cf = CharField(max_length=100, unique=True)
            df = DecimalField(max_digits=4,
                              decimal_places=2,
                              auto_round=True,
                              unique=True)

        class CF(TestModel):
            a = ForeignKeyField(A, to_field='cf')

        class DF(TestModel):
            a = ForeignKeyField(A, to_field='df')

        cf_create, _ = compiler.create_table(CF)
        assert (cf_create == 'CREATE TABLE "cf" ('
                             '"id" INTEGER NOT NULL PRIMARY KEY, '
                             '"a_id" VARCHAR(100) NOT NULL, '
                             'FOREIGN KEY ("a_id") REFERENCES "a" ("cf"))')

        df_create, _ = compiler.create_table(DF)
        assert (df_create == 'CREATE TABLE "df" ('
                             '"id" INTEGER NOT NULL PRIMARY KEY, '
                             '"a_id" DECIMAL(4, 2) NOT NULL, '
                             'FOREIGN KEY ("a_id") REFERENCES "a" ("df"))')


class TestDeferredForeignKey(ModelTestCase):
    # requires = [Language, Snippet]

    def setUp(self):
        super(TestDeferredForeignKey, self).setUp()
        Snippet.drop_table(True)
        Language.drop_table(True)
        Language.create_table()
        Snippet.create_table()

    def tearDown(self):
        super(TestDeferredForeignKey, self).tearDown()
        Snippet.drop_table(True)
        Language.drop_table(True)

    def test_field_definitions(self):
        assert Snippet._meta.fields['language'].rel_model == Language
        assert Language._meta.fields['selected_snippet'].rel_model == Snippet

    def test_deferred_relation_resolution(self):
        orig = len(DeferredRelation._unresolved)

        class CircularRef1(Model):
            circ_ref2 = ForeignKeyField(DeferredRelation('circularref2'),
                                        null=True)

        assert len(DeferredRelation._unresolved) == orig + 1

        class CircularRef2(Model):
            circ_ref1 = ForeignKeyField(CircularRef1, null=True)

        assert CircularRef1.circ_ref2.rel_model == CircularRef2
        assert CircularRef2.circ_ref1.rel_model == CircularRef1
        assert len(DeferredRelation._unresolved) == orig

    def test_create_table_query(self):
        query, params = compiler.create_table(Snippet)
        assert (query == 'CREATE TABLE "snippet" '
                         '("id" INTEGER NOT NULL PRIMARY KEY, '
                         '"code" TEXT NOT NULL, '
                         '"language_id" INTEGER NOT NULL, '
                         'FOREIGN KEY ("language_id") '
                         'REFERENCES "language" ("id"))')

        query, params = compiler.create_table(Language)
        assert (query == 'CREATE TABLE "language" '
                         '("id" INTEGER NOT NULL PRIMARY KEY, '
                         '"name" VARCHAR(255) NOT NULL, '
                         '"selected_snippet_id" INTEGER)')

    def test_storage_retrieval(self):
        python = Language.create(name='python')
        javascript = Language.create(name='javascript')
        p1 = Snippet.create(code="print 'Hello world'", language=python)
        p2 = Snippet.create(code="print 'Goodbye world'", language=python)
        j1 = Snippet.create(code="alert('Hello world')", language=javascript)

        assert Snippet.get(Snippet.id == p1.id).language == python
        assert Snippet.get(Snippet.id == j1.id).language == javascript

        python.selected_snippet = p2
        python.save()

        assert Language.get(Language.id == python.id).selected_snippet == p2
        assert Language.get(
            Language.id == javascript.id).selected_snippet is None


class TestSQLiteDeferredForeignKey(PeeweeTestCase):
    def test_doc_example(self):
        db = database_initializer.get_in_memory_database()
        TweetDeferred = DeferredRelation()

        class Base(Model):
            class Meta:
                database = db

        class User(Base):
            username = CharField()
            favorite_tweet = ForeignKeyField(TweetDeferred, null=True)

        class Tweet(Base):
            user = ForeignKeyField(User)
            message = TextField()

        TweetDeferred.set_model(Tweet)
        with db.transaction():
            User.create_table()
            Tweet.create_table()

        # SQLite does not support alter + add constraint.
        with pytest.raises(OperationalError):
            db.create_foreign_key(User, User.favorite_tweet)


class TestForeignKeyConstraints(ModelTestCase):
    requires = [User, Blog]

    def setUp(self):
        self.set_foreign_key_pragma(True)
        super(TestForeignKeyConstraints, self).setUp()

    def tearDown(self):
        self.set_foreign_key_pragma(False)
        super(TestForeignKeyConstraints, self).tearDown()

    def set_foreign_key_pragma(self, is_enabled):
        if not isinstance(test_db, SqliteDatabase):
            return

        state = 'on' if is_enabled else 'off'
        test_db.execute_sql('PRAGMA foreign_keys = {0!s}'.format(state))

    def test_constraint_exists(self):
        # IntegrityError is raised when we specify a non-existent user_id.
        max_id = User.select(fn.Max(User.id)).scalar() or 0

        def will_fail():
            with test_db.transaction():
                Blog.create(user=max_id + 1, title='testing')

        with pytest.raises(IntegrityError):
            will_fail()
