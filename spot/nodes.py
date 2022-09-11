import sqlite3, time

from collections import deque

kBigNumber = 9e99

class ExpressionContext:
  def __init__(
      self,
      c : sqlite3.Cursor,
      r : int,
      pageLength : int,
      index,
      first = (-kBigNumber, kBigNumber),
      last = (kBigNumber, kBigNumber)
    ):
    self.c = c
    self.r = r  # (how long it takes to check a doc for a token) / (how long it takes to yield the next docid in a posting list)
    self.pageLength = pageLength
    self.first = first
    self.last = last
    self.index = index

  def count(self, token):
    self.c.execute("SELECT count FROM pair_counts WHERE id1 = ? AND id2 = ?", (token, token))
    return self.c.fetchone()[0]

  def pair_count(self, token1, token2):
    if token2 > token1:
      token1, token2 = token2, token1
    self.c.execute("SELECT count FROM pair_counts WHERE id1 = ? AND id2 = ?", (token1, token2))
    return self.c.fetchone()[0]


class Node:
  def __init__(self):
    self.current = None

  def next(self, ctx, x):
    """
    Return the smallest value that is greater than x
    """
    raise NotImplementedError('')


class NotNode(Node):
  def __new__(cls, token):
    return AndNode(
      [
        (TokenNode(token), True),
        (TokenNode(0), False),
      ],
    )


class AndNode(Node):
  def __init__(self, children):
    assert len(children) > 1
    self.children = [child[0] for child in children]
    self.negated = [child[1] for child in children]

  def is_satisfied(self, vals, x):
    for v, n in zip(vals, self.negated):
      if n == (v == x):
        return False
    return True

  def next(self, ctx, x):
    vals = [child.next(ctx, x) for child in self.children]
    x = max(v for (v, n) in zip(vals, self.negated) if not n)

    while True:
      # increment up to x
      vals = [child.next(ctx, (x[0], x[1] - 1)) for child in self.children]
      if x == ctx.last or self.is_satisfied(vals, x):
        return x

      # increment beyond x
      vals = [child.next(ctx, x) for child in self.children]
      x = max(v for (v, n) in zip(vals, self.negated) if not n)


class TokenNode(Node):
  def __init__(self, token : int, node_type = 'TokenNode'):
    assert node_type == 'TokenNode'
    super().__init__()
    self.token = token
    self._cache = deque()

  def next(self, ctx, x):
    if len(self._cache) == 0:
      self._cache += ctx.index.docids(
        c = ctx.c,
        token = self.token,
        n = ctx.pageLength,
        where = x,
      )
    if len(self._cache) == 0:
      self._cache.append(ctx.last)
    if self._cache[0] == ctx.last:
      return ctx.last
    while self._cache[0] <= x:
      if len(self._cache) == 1:
        self._cache += ctx.index.docids(
          c = ctx.c,
          token = self.token,
          n = ctx.pageLength,
          where = self._cache[0],
        )
      self._cache.popleft()
      if len(self._cache) == 0:
        self._cache.append(ctx.last)
        break
    return self._cache[0]


class EmptyNode(Node):
  def next(self, ctx):
    return ctx.last


class ListNode(Node):
  def __init__(self, A):
    super().__init__()
    self.A = A

  def next(self, ctx, x):
    for a in self.A:
      if a > x:
        return a
    return ctx.last
