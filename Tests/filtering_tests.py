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
    index.insert({
      "docid": 0,
      "score": 10,
      "date_created": 10,
      "document_text": "foo bar",
      "tokens": ["foo", "bar"]
    })
    index.insert({
      "docid": 1,
      "score": 3,
      "date_created": 11,
      "document_text": "foo baz",
      "tokens": ["foo", "baz"]
    })
    index.insert({
      "docid": 2,
      "score": 7,
      "date_created": 12,
      "document_text": "bar baz",
      "tokens": ["bar", "baz"]
    })
    results = index.token_iterator("foo", ranking="-score", range_requirements=[("date_created", ">", 10)])
    results = list(results)
    self.assertEqual(results, [(-3.0, -1)])

class TokenCountTests(unittest.TestCase):
  def test1(self):
    # self, docid, postid, created_utc, tokens, jsondata
    index = spot.Index.create(':memory:', rankings=["score"], ranges=["date_created"])
    index.insert({
      "docid": 0,
      "score": 10,
      "date_created": 10,
      "tokens": ["foo", "bar"],
    })
    index.insert({
      "docid": 1,
      "score": 3,
      "date_created": 11,
      "tokens": ["foo"],
    })
    index.insert({
      "docid": 2,
      "score": 7,
      "date_created": 12,
      "tokens": ["foo", "bar", "baz"],
    })
    self.assertEqual(index.num_occurrences('foo'), 3)
    self.assertEqual(index.num_occurrences('bar'), 2)
    self.assertEqual(index.num_occurrences('baz'), 1)
    self.arr_eq(index.common_tokens(5), [('foo', 3), ('', 3), ('bar', 2), ('baz', 1)])
    self.arr_eq(index.common_tokens(5, 'b'), [('bar', 2), ('baz', 1)])

    # Make sure recompute_token_counts is correct.
    index.recompute_token_counts()
    self.arr_eq(index.common_tokens(5), [('', 3), ('foo', 3), ('bar', 2), ('baz', 1)])
    self.arr_eq(index.common_tokens(5, 'b'), [('bar', 2), ('baz', 1)])

    # Insert 'baz' at docid=0
    index.replace({
      "docid": 0,
      "score": 10,
      "date_created": 10,
      "tokens": ["foo", "bar", "baz"],
    })

    self.arr_eq(index.common_tokens(5), [('', 3), ('foo', 3), ('bar', 2), ('baz', 2)])

    # Delete 'foo' at docid=0
    index.replace({
      "docid": 0,
      "score": 10,
      "date_created": 10,
      "tokens": ["bar", "baz"],
    })

    self.arr_eq(index.common_tokens(5), [('', 3), ('foo', 2), ('bar', 2), ('baz', 2)])

  def arr_eq(self, A, B):
    A.sort()
    B.sort()
    self.assertEqual(A, B)


if __name__ == '__main__':
  unittest.main()
