import datetime
import re


def format_date_time(value, formats, post_process=None):
    post_process = post_process or (lambda x: x)
    for fmt in formats:
        try:
            return post_process(datetime.datetime.strptime(value, fmt))
        except ValueError:
            pass
    return value


def sort_models_topologically(models):
    """Sort models topologically so that parents will precede children."""
    models = set(models)
    seen = set()
    ordering = []

    def dfs(model):
        if model in models and model not in seen:
            seen.add(model)
            for foreign_key in model._meta.reverse_rel.values():
                dfs(foreign_key.model_class)
            ordering.append(model)  # parent will follow descendants

    # Order models by name and table initially to guarantee total ordering.
    names = lambda m: (m._meta.name, m._meta.db_table)
    for m in sorted(models, key=names, reverse=True):
        dfs(m)
    return list(reversed(ordering))


def strip_parens(s):
    # Quick sanity check.
    if not s or s[0] != '(':
        return s

    ct = i = 0
    l = len(s)
    while i < l:
        if s[i] == '(' and s[l - 1] == ')':
            ct += 1
            i += 1
            l -= 1
        else:
            break
    if ct:
        # If we ever end up with negatively-balanced parentheses, then we
        # know that one of the outer parentheses was required.
        unbalanced_ct = 0
        required = 0
        for i in range(ct, l - ct):
            if s[i] == '(':
                unbalanced_ct += 1
            elif s[i] == ')':
                unbalanced_ct -= 1
            if unbalanced_ct < 0:
                required += 1
                unbalanced_ct = 0
            if required == ct:
                break
        ct -= required
    if ct > 0:
        return s[ct:-ct]
    return s


DATETIME_PARTS = ['year', 'month', 'day', 'hour', 'minute', 'second']
DATETIME_LOOKUPS = set(DATETIME_PARTS)

# Sqlite does not support the `date_part` SQL function, so we will define an
# implementation in python.
SQLITE_DATETIME_FORMATS = ('%Y-%m-%d %H:%M:%S',
                           '%Y-%m-%d %H:%M:%S.%f',
                           '%Y-%m-%d',
                           '%H:%M:%S',
                           '%H:%M:%S.%f',
                           '%H:%M')


def _sqlite_date_part(lookup_type, datetime_string):
    assert lookup_type in DATETIME_LOOKUPS
    if not datetime_string:
        return
    dt = format_date_time(datetime_string, SQLITE_DATETIME_FORMATS)
    return getattr(dt, lookup_type)


SQLITE_DATE_TRUNC_MAPPING = {'year': '%Y',
                             'month': '%Y-%m',
                             'day': '%Y-%m-%d',
                             'hour': '%Y-%m-%d %H',
                             'minute': '%Y-%m-%d %H:%M',
                             'second': '%Y-%m-%d %H:%M:%S'}


def _sqlite_date_trunc(lookup_type, datetime_string):
    assert lookup_type in SQLITE_DATE_TRUNC_MAPPING
    if not datetime_string:
        return
    dt = format_date_time(datetime_string, SQLITE_DATETIME_FORMATS)
    return dt.strftime(SQLITE_DATE_TRUNC_MAPPING[lookup_type])


def _sqlite_regexp(regex, value):
    return re.search(regex, value, re.I) is not None


class attrdict(dict):
    def __getattr__(self, attr):
        return self[attr]


# Operators used in binary expressions.
OP = attrdict(AND='and',
              OR='or',
              ADD='+',
              SUB='-',
              MUL='*',
              DIV='/',
              BIN_AND='&',
              BIN_OR='|',
              XOR='^',
              MOD='%',
              EQ='=',
              LT='<',
              LTE='<=',
              GT='>',
              GTE='>=',
              NE='!=',
              IN='in',
              NOT_IN='not in',
              IS='is',
              IS_NOT='is not',
              LIKE='like',
              ILIKE='ilike',
              BETWEEN='between',
              REGEXP='regexp',
              CONCAT='||',
              )

JOIN = attrdict(INNER='INNER',
                LEFT_OUTER='LEFT OUTER',
                RIGHT_OUTER='RIGHT OUTER',
                FULL='FULL',
                )
JOIN_INNER = JOIN.INNER
JOIN_LEFT_OUTER = JOIN.LEFT_OUTER
JOIN_FULL = JOIN.FULL

RESULTS_NAIVE = 1
RESULTS_MODELS = 2
RESULTS_TUPLES = 3
RESULTS_DICTS = 4
RESULTS_AGGREGATE_MODELS = 5

# To support "django-style" double-underscore filters, create a mapping between
# operation name and operation code, e.g. "__eq" == OP.EQ.
DJANGO_MAP = {'eq': OP.EQ,
              'lt': OP.LT,
              'lte': OP.LTE,
              'gt': OP.GT,
              'gte': OP.GTE,
              'ne': OP.NE,
              'in': OP.IN,
              'is': OP.IS,
              'like': OP.LIKE,
              'ilike': OP.ILIKE,
              'regexp': OP.REGEXP,
              }


# Helper functions that are used in various parts of the codebase.
def merge_dict(source, overrides):
    merged = source.copy()
    merged.update(overrides)
    return merged


def returns_clone(func):
    """
    Method decorator that will "clone" the object before applying the given
    method.  This ensures that state is mutated in a more predictable fashion,
    and promotes the use of method-chaining.
    """

    def inner(self, *args, **kwargs):
        clone = self.clone()  # Assumes object implements `clone`.
        func(clone, *args, **kwargs)
        return clone

    inner.call_local = func  # Provide a way to call without cloning.
    return inner


def not_allowed(func):
    """
    Method decorator to indicate a method is not allowed to be called.  Will
    raise a `NotImplementedError`.
    """

    def inner(self, *args, **kwargs):
        raise NotImplementedError(
            '{0!s} is not allowed on {1!s} instances'.format(
                func, type(self).__name__))

    return inner
