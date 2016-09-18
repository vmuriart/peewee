# -*- coding: utf-8 -*-

from peewee import SqliteDatabase
from tests.base import ModelTestCase, database_class, test_db
from tests.models import (Blog, Category, Comment, CompositeKeyModel,
                          MultiIndexModel, UniqueModel, User)


class TestMetadataIntrospection(ModelTestCase):
    requires = [User, Blog, Comment, CompositeKeyModel, MultiIndexModel,
                UniqueModel, Category]

    def setUp(self):
        super(TestMetadataIntrospection, self).setUp()
        self.pk_index = database_class is not SqliteDatabase

    def test_get_tables(self):
        tables = test_db.get_tables()
        for model in self.requires:
            assert model._meta.db_table in tables

        UniqueModel.drop_table()
        assert not (UniqueModel._meta.db_table in test_db.get_tables())

    def test_get_indexes(self):
        indexes = test_db.get_indexes(UniqueModel._meta.db_table)
        num_indexes = self.pk_index and 2 or 1
        assert len(indexes) == num_indexes

        idx, = [idx for idx in indexes if idx.name == 'uniquemodel_name']
        assert idx.columns == ['name']
        assert idx.unique

        indexes = dict((idx.name, idx) for idx in
                       test_db.get_indexes(MultiIndexModel._meta.db_table))
        num_indexes = self.pk_index and 3 or 2
        assert len(indexes) == num_indexes

        idx_f1f2 = indexes['multiindexmodel_f1_f2']
        assert sorted(idx_f1f2.columns) == ['f1', 'f2']
        assert idx_f1f2.unique

        idx_f2f3 = indexes['multiindexmodel_f2_f3']
        assert sorted(idx_f2f3.columns) == ['f2', 'f3']
        assert not idx_f2f3.unique
        assert idx_f2f3.table == 'multiindexmodel'

        # SQLite *will* create an index here, so we will always have one.
        indexes = test_db.get_indexes(CompositeKeyModel._meta.db_table)
        assert len(indexes) == 1
        assert sorted(indexes[0].columns) == ['f1', 'f2']
        assert indexes[0].unique

    def test_get_columns(self):
        def get_columns(model):
            return dict((column.name, column) for column in
                        test_db.get_columns(model._meta.db_table))

        def assertColumns(model, col_names, nullable, pks):
            columns = get_columns(model)
            assert sorted(columns) == col_names
            for column, metadata in columns.items():
                assert metadata.null == (column in nullable)
                assert metadata.table == model._meta.db_table
                assert metadata.primary_key == (column in pks)

        assertColumns(User, ['id', 'username'], [], ['id'])
        assertColumns(Blog, ['content', 'pk', 'pub_date', 'title', 'user_id'],
                      ['pub_date'], ['pk'])
        assertColumns(UniqueModel, ['id', 'name'], [], ['id'])
        assertColumns(MultiIndexModel, ['f1', 'f2', 'f3', 'id'], [], ['id'])
        assertColumns(CompositeKeyModel, ['f1', 'f2', 'f3'], [], ['f1', 'f2'])
        assertColumns(Category, ['id', 'name', 'parent_id'], ['parent_id'],
                      ['id'])

    def test_get_primary_keys(self):
        def assertPKs(model_class, expected):
            assert (test_db.get_primary_keys(model_class._meta.db_table) ==
                    expected)

        assertPKs(User, ['id'])
        assertPKs(Blog, ['pk'])
        assertPKs(MultiIndexModel, ['id'])
        assertPKs(CompositeKeyModel, ['f1', 'f2'])
        assertPKs(UniqueModel, ['id'])
        assertPKs(Category, ['id'])

    def test_get_foreign_keys(self):
        def assertFKs(model_class, expected):
            foreign_keys = test_db.get_foreign_keys(model_class._meta.db_table)
            assert len(foreign_keys) == len(expected)
            assert [(fk.column, fk.dest_table, fk.dest_column)
                    for fk in foreign_keys] == expected

        assertFKs(Category, [('parent_id', 'category', 'id')])
        assertFKs(User, [])
        assertFKs(Blog, [('user_id', 'users', 'id')])
        assertFKs(Comment, [('blog_id', 'blog', 'pk')])
