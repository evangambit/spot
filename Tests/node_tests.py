from spot import *

import unittest

class NodeTest(unittest.TestCase):
  def simple_token_test():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    index = TokenIndex.create(c, 'foo')
    index.create_indices(c)
    documents = []
    for i in range(100):
      tokens = [0]
      for j in range(1, 10):
        if i % j == 0:
          tokens.append(j)
      documents.append(tokens)

    for doc_id, doc in enumerate(documents):
      rank = 0
      for token in doc:
        index.insert(c, rank=rank, doc_id=doc_id, token=token)

    for negated in [False]:
      for pageLength in range(1, 5):
        ctx = ExpressionContext(
          c = c,
          r = 1000,
          pageLength = pageLength,
          index = index
        )

        for token in range(1, 10):
          t = TokenNode(token)
          if negated:
            t = NotNode(t)
          A = [ctx.first]
          while A[-1] != ctx.last:
            A.append(t.next(ctx, A[-1]))
          A = [a[1] for a in A[1:-1]]
          if negated:
            assert A == [x for x in range(100) if x % token != 0]
          else:
            assert A == [x for x in range(100) if x % token == 0]

  def and_node_test():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    index = TokenIndex.create(c, 'foo')
    index.create_indices(c)
    documents = []
    for i in range(50):
      tokens = []
      for j in range(1, 10):
        if i % j == 0:
          tokens.append(j)
      documents.append(tokens)

    for doc_id, doc in enumerate(documents):
      rank = 0
      for token in doc:
        index.insert(c, rank=rank, doc_id=doc_id, token=token)

    for pageLength in range(1, 5):
      ctx = ExpressionContext(
        c = c,
        r = 1000,
        pageLength = pageLength,
        index = index
      )

      for token in range(1, 10):
        t1 = TokenNode(token)
        t2 = TokenNode(3)
        a = AndNode([(t1, False), (t2, False)])

        A = [ctx.first]
        while A[-1] != ctx.last:
          A.append(a.next(ctx, A[-1]))
        A = [a[1] for a in A[1:-1]]

        assert A == [d for d in range(50) if d % token == 0 and d % 3 == 0], A

  def and_not_node_test():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    index = TokenIndex.create(c, 'foo')
    index.create_indices(c)
    documents = []
    for i in range(50):
      tokens = []
      for j in range(1, 10):
        if i % j == 0:
          tokens.append(j)
      documents.append(tokens)

    for doc_id, doc in enumerate(documents):
      rank = 0
      for token in doc:
        index.insert(c, rank=rank, doc_id=doc_id, token=token)

    for pageLength in range(1, 5):
      ctx = ExpressionContext(
        c = c,
        r = 1000,
        pageLength = pageLength,
        index = index
      )

      for token in range(1, 10):
        t1 = TokenNode(token)
        t2 = TokenNode(3)
        a = AndNode([(t1, False), (t2, True)])

        A = [ctx.first]
        while A[-1] != ctx.last:
          A.append(a.next(ctx, A[-1]))
        A = [a[1] for a in A[1:-1]]

        assert A == [d for d in range(50) if d % token == 0 and d % 3 != 0], (token, A)


  def or_node_test():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    index = TokenIndex.create(c, 'foo')
    index.create_indices(c)
    documents = []
    for i in range(50):
      tokens = []
      for j in range(1, 10):
        if i % j == 0:
          tokens.append(j)
      documents.append(tokens)

    for doc_id, doc in enumerate(documents):
      rank = 0
      for token in doc:
        index.insert(c, rank=rank, doc_id=doc_id, token=token)

    for pageLength in range(1, 5):
      ctx = ExpressionContext(
        c = c,
        r = 1000,
        pageLength = pageLength,
        index = index
      )

      for token in range(1, 10):
        t1 = TokenNode(token)
        t2 = TokenNode(3)
        a = OrNode([(t1, False), (t2, False)])

        A = [ctx.first]
        while A[-1] != ctx.last:
          A.append(a.next(ctx, A[-1]))
        A = [a[1] for a in A[1:-1]]

        assert A == [d for d in range(50) if d % token == 0 or d % 3 == 0]


  def save_and_load_or_test():
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
    index = TokenIndex.create(c, 'foo')
    index.create_indices(c)
    documents = []
    for i in range(50):
      tokens = []
      for j in range(1, 10):
        if i % j == 0:
          tokens.append(j)
      documents.append(tokens)

    for doc_id, doc in enumerate(documents):
      rank = 0
      for token in doc:
        index.insert(c, rank=rank, doc_id=doc_id, token=token)

    for pageLength in range(1, 5):
      ctx = ExpressionContext(
        c = c,
        r = 1000,
        pageLength = pageLength,
        index = index
      )

      for token in range(1, 10):
        t1 = TokenNode(token)
        t2 = TokenNode(3)
        a = OrNode([(t1, False), (t2, False)])
        a.start(ctx)

        A = []
        while a.current != ctx.last:
          A.append(a.current[1])
          a.next(ctx)
          break

        b = load_node(a.state())
        b.start(ctx)

        B = []
        while a.current != ctx.last:
          A.append(a.current[1])
          B.append(b.current[1])
          a.next(ctx)
          b.next(ctx)

        assert A == [d for d in range(50) if d % token == 0 or d % 3 == 0]
        assert A[1:] == B

