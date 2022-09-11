"""
From top-level spot directory, run:

$ pip install .; python3 -m unittest filtering_tests.py
"""

import os
import shutil
import sqlite3
import unittest

import spot

class IndexTest(unittest.TestCase):
  def test1(self):
    if os.path.exists('tmp'):
      shutil.rmtree('tmp')
    os.mkdir('tmp')
    conn = sqlite3.connect("tmp/db.sqlite")
    index = spot.Index.create(conn, 'tmp', ['score', 'age'])

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
    A = [ctx.first]
    while A[-1] != ctx.last:
      A.append(node.next(ctx, A[-1]))
    A = A[1:-1]
    self.assertEqual([a[1] for a in A], [1, 4])

    index = spot.Index(conn, 'tmp')
    ctx = index.expression_context('score')
    node = index.intersect('foo', 'bar')
    A = [ctx.first]
    while A[-1] != ctx.last:
      A.append(node.next(ctx, A[-1]))
    A = A[1:-1]
    self.assertEqual([a[1] for a in A], [4, 1])
