"""
From top-level spot directory, run:

$ pip install .; python Tests/filtering_tests.py
"""

import unittest

from spot.filtering import *

class AndTest(unittest.TestCase):
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

if __name__ == '__main__':
  unittest.main()
