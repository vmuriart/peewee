# -*- coding: utf-8 -*-

import sys

# Python 2/3 compatibility helpers. These helpers are used internally and are
# not exported.
_METACLASS_ = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(_METACLASS_, (base,), {})


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
PY26 = sys.version_info[:2] == (2, 6)

if PY3:
    import builtins
    from collections import Callable

    callable = lambda c: isinstance(c, Callable)
    unicode_type = str
    string_type = bytes
    basestring = str
    print_ = getattr(builtins, 'print')
    binary_construct = lambda s: bytes(s.encode('raw_unicode_escape'))
    long = int


    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

elif PY2:
    callable = callable
    unicode_type = unicode
    string_type = basestring
    basestring = basestring


    def print_(s):
        sys.stdout.write(s)
        sys.stdout.write('\n')


    binary_construct = buffer
    long = long

    exec('def reraise(tp, value, tb=None): raise tp, value, tb')

else:
    raise RuntimeError('Unsupported python version.')

if PY26:
    _M = 10 ** 6
    total_seconds = lambda t: (t.microseconds + (
        t.seconds + t.days * 24 * 3600) * _M) / _M
else:
    total_seconds = lambda t: t.total_seconds()
