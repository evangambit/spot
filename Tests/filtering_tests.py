"""
From top-level spot directory, run:

$ pip install .; python Tests/filtering_tests.py
"""

import unittest

import spot
from spot.filtering import *

class TokenFilteringTest(unittest.TestCase):
  def test_union(self):
    A = (0, 0)
    B = (1, 1)
    C = (2, 2)
    D = (3, 3)
    node = union(iter([A, B, C]), iter([A, C, D]))
    self.assertEqual(list(node), [A, B, C, D])

  def test_intersect(self):
    A = (0, 0)
    B = (1, 1)
    C = (2, 2)
    D = (3, 3)
    node = intersect(iter([A, B, C]), iter([A, C, D]))
    self.assertEqual(list(node), [A, C])

class RangeFilteringTest(unittest.TestCase):
  def test_union(self):
    index = spot.Index.create(':memory:', rankings=["score"], ranges=["date_created"])
    index.insert(0, tokens=["foo", "bar"], jsondata={
      "score": 10,
      "date_created": 10,
      "document_text": "foo bar"
    })
    index.insert(1, tokens=["foo", "baz"], jsondata={
      "score": 3,
      "date_created": 11,
      "document_text": "foo baz"
    })
    index.insert(2, tokens=["bar", "baz"], jsondata={
      "score": 7,
      "date_created": 12,
      "document_text": "bar baz"
    })
    results = index.token_iterator("foo", ranking="-score", range_requirements=[("date_created", ">", 10)])
    results = list(results)
    self.assertEqual(results, [(3.0, 1)])

if __name__ == '__main__':
  unittest.main()
