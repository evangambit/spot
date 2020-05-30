"""
Run all tests:

$ python setup.py install; python Tests/node_test.py
"""

import unittest

from spot.filtering import *

Expression.kFirstVal = ''
Expression.kLastVal = '~~~~~~~~~~'

class AndTest(unittest.TestCase):
  def identity_test1(self):
    node = And(ListINode(['a', 'b', 'c']))
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'b', 'c', Expression.kLastVal]:
      assert node.step() == val
      assert node.currentValue == val

  def identity_test2(self):
    node = And(
      ListINode(['a', 'b', 'c']),
      ListINode(['a', 'b', 'c'])
    )
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'b', 'c', Expression.kLastVal]:
      assert node.step() == val
      assert node.currentValue == val

  def test_normal(self):
    node = And(
      ListINode(['a', 'b', 'c', 'd', 'e']),
      ListINode(['a', 'c', 'd'])
    )
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'c', 'd']:
      assert node.step() == val, f'Expected {val} but got {node.currentValue}'
      assert node.currentValue == val, f'Expected {val} but got {node.currentValue}'

  def test_encode(self):
    node1 = And(
      ListINode(['a', 'b', 'c']),
      ListINode(['a', 'c', 'd'])
    )
    node2 = Expression.decode(node1.encode())
    while node1.currentValue != Expression.kLastVal:
    	assert node1.currentValue == node2.currentValue
    	assert node1.step() == node2.step()

class OrTest(unittest.TestCase):
  def identity_test1(self):
    node = Or(ListINode(['a', 'b', 'c']))
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'b', 'c', Expression.kLastVal]:
      assert node.step() == val
      assert node.currentValue == val

  def identity_test2(self):
    node = Or(
      ListINode(['a', 'b', 'c']),
      ListINode(['a', 'b', 'c'])
    )
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'b', 'c', Expression.kLastVal]:
      assert node.step() == val
      assert node.currentValue == val

  def test_normal(self):
    node = Or(
      ListINode(['a', 'b', 'd', 'e']),
      ListINode(['a', 'c', 'd'])
    )
    assert node.currentValue == Expression.kFirstVal
    for val in ['a', 'b', 'c', 'd', 'e']:
      assert node.step() == val, f'Expected {val} but got {node.currentValue}'
      assert node.currentValue == val, f'Expected {val} but got {node.currentValue}'

  def test_encode(self):
    node1 = Or(
      ListINode(['a', 'b', 'c', 'd']),
      ListINode(['a', 'c', 'd'])
    )
    node2 = Expression.decode(node1.encode())
    while node1.currentValue != Expression.kLastVal:
    	assert node1.currentValue == node2.currentValue
    	assert node1.step() == node2.step()

if __name__ == '__main__':
  unittest.main()
