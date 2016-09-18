#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import unittest

import tests_core


def runtests(suite, verbosity=1):
    results = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return results.failures, results.errors


def collect_modules():
    modules = []
    modules.insert(0, tests_core)
    return modules


if __name__ == '__main__':

    suite = unittest.TestSuite()
    for module in collect_modules():
        module_suite = unittest.TestLoader().loadTestsFromModule(module)
        suite.addTest(module_suite)

    failures, errors = runtests(suite)

    if errors:
        sys.exit(2)
    elif failures:
        sys.exit(1)

    sys.exit(0)
