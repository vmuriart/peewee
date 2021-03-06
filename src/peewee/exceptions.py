# -*- coding: utf-8 -*-

from peewee._compat import reraise


class DoesNotExist(Exception):
    pass


class PeeweeException(Exception):
    pass


class ImproperlyConfigured(PeeweeException):
    pass


class DatabaseError(PeeweeException):
    pass


class DataError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InterfaceError(PeeweeException):
    pass


class InternalError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class ExceptionWrapper(object):
    __slots__ = ['exceptions']

    def __init__(self, exceptions):
        self.exceptions = exceptions

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return
        if exc_type.__name__ in self.exceptions:
            new_type = self.exceptions[exc_type.__name__]

            exc_args = exc_value.args
            reraise(new_type, new_type(*exc_args), traceback)
