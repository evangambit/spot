import sqlite3

from collections import deque

kBigNumber = 9e99

class ExpressionContext:
  def __init__(self, c : sqlite3.Cursor, n : int, r : int, pageLength : int, index, first = (-kBigNumber, kBigNumber), last = (kBigNumber, kBigNumber)):
    self.c = c
    self.n = n
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

  def state(self):
    raise NotImplementedError('')

  def start(self, ctx):
    """
    Sets "self.current" to either a real value or ctx.last
    """
    raise NotImplementedError('')

  def next(self):
    raise NotImplementedError('')


class NotNode(Node):
  def __new__(cls, token):
    return AndNode(
      [
        (TokenNode(token), True),
        (TokenNode(0), False),
      ],
    )


class OrNode(Node):
  def __init__(self, children, current = None, node_type = 'OrNode'):
    assert node_type == 'OrNode'
    assert len(children) > 1

    # Load from json state if necessary
    if isinstance(children[0][0], dict):
      children = [(load_node(child[0]), child[1]) for child in children]

    self.children = [child[0] for child in children]
    self.negated = [child[1] for child in children]
    self.current = tuple(current) if current is not None else None

    assert sum(self.negated) == 0, 'negation not supported for "OR"'

  def state(self):
    return {
      'children': list(zip([child.state() for child in self.children], self.negated)),
      'current': self.current,
      'node_type': 'OrNode',
    }

  def _low(self):
    return min(child.current for child in self.children)

  def _increment(self, ctx):
    low = self._low()
    for child in self.children:
      if child.current == low:
        child.next(ctx)
    return low

  def start(self, ctx):
    for child in self.children:
      child.start(ctx)
    if self.current is not None:
      return
    self.current = self._increment(ctx)

  def next(self, ctx):
    self.current = self._increment(ctx)
    return self.current

class AndNode(Node):
  def __init__(self, children, current = None, node_type = 'AndNode'):
    assert node_type == 'AndNode'
    assert len(children) > 1

    # Load from json state if necessary
    if isinstance(children[0][0], dict):
      children = [(load_node(child[0]), child[1]) for child in children]

    self.children = [child[0] for child in children]
    self.negated = [child[1] for child in children]
    self.current = tuple(current) if current is not None else None

  def state(self):
    return {
      'children': list(zip([child.state() for child in self.children], self.negated)),
      'current': self.current,
      'node_type': 'AndNode',
    }

  def _low(self):
    return min(child.current for child, neg in zip(self.children, self.negated) if not neg)

  def _increment(self, ctx):
    changed = False
    low = self._low()
    high = None
    # Find lowest non-negated token and increment it
    for child, neg in zip(self.children, self.negated):
      if not neg and child.current == low:
        child.next(ctx)
        if high is None or child.current > high:
          high = child.current
    # Then increment the other tokens until they're >= to its new value
    for child in self.children:
      while child.current < high and child.current != ctx.last:
        child.next(ctx)
        changed = True
    return changed

  def _satisfied(self):
    A = set([c.current for c, n in zip(self.children, self.negated) if not n])
    B = set([c.current for c, n in zip(self.children, self.negated) if n])
    if len(A) == 1 and len(A - B) == len(A):
      return True, list(A)[0]
    else:
      return False, None

  def start(self, ctx):
    for child in self.children:
      child.start(ctx)
    if self.current is not None:
      return
    is_satisfied, val = self._satisfied()
    if is_satisfied:
      self.current = val
      self._increment(ctx)
    else:
      self.next(ctx)

  def next(self, ctx):
    if self.current == ctx.last:
      return self.current
    satisfied, val = self._satisfied()
    while not satisfied and self._low() != ctx.last:
      c = self._increment(ctx)
      satisfied, val = self._satisfied()
    self.current = val if val is not None else ctx.last
    self._increment(ctx)
    return self.current


class TokenNode(Node):
  def __init__(self, token : int, rank = -kBigNumber, offset = -1, current = None, node_type = 'TokenNode'):
    assert node_type == 'TokenNode'
    super().__init__()
    self.token = token
    self._cache = None
    self.rank = rank
    self.offset = offset
    self.current = tuple(current) if current is not None else None

  def state(self):
    return {
      'token': self.token,
      'rank': self.current[0],
      'offset': self.offset,
      'current': self.current,
      'node_type': 'TokenNode',
    }


  def start(self, ctx):
    assert self._cache is None
    self._cache = deque(maxlen = ctx.pageLength)
    if self.current is None:
      self.next(ctx)


  def next(self, ctx):
    if len(self._cache) == 0:
      self._cache += ctx.index.docids(
        c = ctx.c,
        token = self.token,
        n = ctx.pageLength,
        where = self.current if self.current is not None else ctx.first,
      )
    if len(self._cache) == 0:
      self.current = ctx.last
      return self.current

    rank, docid = self._cache.popleft()
    if rank > self.rank:
      self.rank = rank
      self.offset = 0
    else:
      self.offset += 1

    self.current = (rank, docid)
    return self.current


kNodeNameToType = {
  'TokenNode': TokenNode,
  'AndNode': AndNode,
  'OrNode': OrNode,
}
def load_node(state):
  return kNodeNameToType[state['node_type']](**state)

class EmptyNode(Node):
  def state(self):
    return {}
  def start(self, ctx):
    self.current = ctx.last
  def next(self, ctx):
    return ctx.last

class ListNode(Node):
  def __init__(self, A, idx = None):
    super().__init__()
    self.A = A
    self.idx = idx

  def state(self):
    return {
      'A': self.A,
      'idx': self.idx,
    }

  def start(self, ctx):
    if self.idx is None:
      self.idx = 0
    self.current = self.A[self.idx]

  def next(self, ctx):
    self.idx += 1
    if self.idx >= len(self.A):
      self.current = ctx.last
      return self.current
    self.current = self.A[self.idx]
    return self.current
