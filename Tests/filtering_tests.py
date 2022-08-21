"""
From top-level spot directory, run:

$ pip install .; python3 -m unittest filtering_tests.py
"""

import os
import shutil
import unittest

import spot

class IndexTest(unittest.TestCase):
  def test1(self):
    if os.path.exists('tmp'):
      shutil.rmtree('tmp')
    index = spot.Index.create('tmp', ['score', 'age'])

    index.add_range('score', 0, 15)

    index.insert_document({
      'rankings': { 'score': 3.0, 'age': 7.0 },
      'tags': ['foo', 'bar'],
    })
    index.insert_document({
      'rankings': { 'score': 1.0, 'age': 7.0 },
      'tags': ['foo'],
    })
    index.insert_document({
      'rankings': { 'score': 4.0, 'age': 7.0 },
      'tags': ['bar'],
    })
    index.insert_document({
      'rankings': { 'score': 1.0, 'age': 7.0 },
      'tags': ['foo', 'bar', 'baz'],
    })

    index.save()

    ctx = index.expression_context('age')
    node = index.intersect('foo', 'bar')
    node.start(ctx)
    A = []
    while node.current != ctx.last:
      A.append(node.current)
      node.next(ctx)
    self.assertEqual([a[1] for a in A], [1, 4])

    index = spot.Index('tmp')
    ctx = index.expression_context('score')
    node = index.intersect('foo', 'bar')
    node.start(ctx)
    A = []
    while node.current != ctx.last:
      A.append(node.current)
      node.next(ctx)
    self.assertEqual([a[1] for a in A], [4, 1])
