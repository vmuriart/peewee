# -*- coding: utf-8 -*-

import sys

# Python 2/3 compatibility helpers. These helpers are used internally and are
# not exported.
_METACLASS_ = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(_METACLASS_, (base,), {})


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    from collections import Callable
    from functools import reduce

    ulit = lambda s: s
    callable = lambda c: isinstance(c, Callable)
    unicode_type = str
    string_type = bytes
    basestring = str
    binary_construct = lambda s: bytes(s.encode('raw_unicode_escape'))
    binary_types = (bytes, memoryview)
    long = int
    reduce = reduce


    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

elif PY2:
    import codecs

    ulit = lambda s: codecs.unicode_escape_decode(s)[0]
    callable = callable
    unicode_type = unicode
    string_type = basestring
    basestring = basestring
    reduce = reduce
    binary_construct = buffer
    binary_types = buffer
    long = long

    exec('def reraise(tp, value, tb=None): raise tp, value, tb')

else:
    raise RuntimeError('Unsupported python version.')
