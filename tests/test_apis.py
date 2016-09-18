# -*- coding: utf-8 -*-

from peewee import Node
from peewee import *
from tests.base import PeeweeTestCase
import pytest


class TestNodeAPI(PeeweeTestCase):
    def test_extend(self):
        @Node.extend()
        def add(self, lhs, rhs):
            return lhs + rhs

        n = Node()
        assert n.add(4, 2) == 6
        delattr(Node, 'add')
        with pytest.raises(AttributeError):
            n.add(2, 4)

    def test_clone(self):
        @Node.extend(clone=True)
        def hack(self, alias):
            self._negated = True
            self._alias = alias

        n = Node()
        c = n.hack('magic!')
        assert not n._negated
        assert n._alias == None
        assert c._negated
        assert c._alias == 'magic!'

        class TestModel(Model):
            data = CharField()

        hacked = TestModel.data.hack('nugget')
        assert not TestModel.data._negated
        assert TestModel.data._alias == None
        assert hacked._negated
        assert hacked._alias == 'nugget'

        delattr(Node, 'hack')
        with pytest.raises(AttributeError):
            TestModel.data.hack()
