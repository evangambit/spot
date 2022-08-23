import json
import os
import re
import shutil
import sqlite3
from functools import lru_cache

from .token_index import TokenIndex
from .nodes import TokenNode, AndNode, ExpressionContext, OrNode, EmptyNode, load_node

kIntRangePrefix = '#'

def num_bits(low, high):
  bits = 0
  r = high - low
  while r > 0:
    r >>= 1
    bits += 1
  return bits


class IntRange:
  """
  Represents an integer with (inclusive) range [low, high].

  Given a range (e.g. "[4, 15]") returns a list of tokens which, when OR-ed together, represent it.

  Given an integer, returns the tokens that should be associated with the document
  """
  def __init__(self, name, low, high):
    self.name = name
    self.low = low
    self.high = high
    self.num_bits = num_bits(low, high)

  def less_than(self, x : int):
    return self.range(self.low, x - 1)

  def greater_than(self, x : int):
    return self.range(x + 1, self.high)

  def range(self, low, high, inclusive = True):
    low -= self.low
    high -= self.low
    if not inclusive:
      low += 1
      high -= 1
    return self._range(low, high, 1 << (self.num_bits - 1))

  def _range(self, low, high, step):
    if low > high:
      return []
    if step == 1:
      assert low == high
      return [f"#{self.name}:{low}:{low}"]
    r = []

    # todo: simplify
    a0 = (low // step) * step
    if a0 < low:
      a0 += step

    a = a0
    while a + step - 1 <= high:
      r.append(f"#{self.name}:{a}:{a+step-1}")
      a += step

    if len(r) == 0:
      return self._range(low, high, step // 2)
    return self._range(low, a0 - 1, step // 2) + r + self._range(a, high, step // 2)

  def tokens(self, x : int):
    r = []
    for bit in range(self.num_bits):
      step = 1 << bit
      a = x // step * step
      r.append(f"#{self.name}:{a}:{a + step - 1}")
    return r

  def json(self):
    return {
      'name': self.name,
      'low': self.low,
      'high': self.high,
    }

  @staticmethod
  def valid_token(x : str):
    return re.match(r"^#\w+:\d+:\d+$", x)


def is_valid_token(name : str) -> bool:
  if name[:len(kIntRangePrefix)] == kIntRangePrefix:
    return re.match(r"\d+:\d+", name[kIntRangePrefix:])
  return re.match(r"^[^#\s\"][^\s\"]*$", name) is not None

class TokenMapper:
  def __init__(self, c : sqlite3.Cursor):
    c.execute("""CREATE TABLE IF NOT EXISTS tokens  (
      token_str BLOB,
      count INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS tokenIndex ON tokens(token_str, count)")

  def exists(self, token_str : str, c : sqlite3.Cursor):
    if token_str == '':
      return True
    if token_str[:len(kIntRangePrefix)] == kIntRangePrefix:
      assert IntRange.valid_token(token_str), f"Invalid int token \"{token_str}\""
    else:
      assert is_valid_token(token_str), f"Invalid tag \"{token_str}\""
    c.execute("SELECT rowid FROM tokens WHERE token_str = ?", (token_str,))
    return c.fetchone() is not None

  def increment(self, token_str : str, c : sqlite3.Cursor):
    if self.exists(token_str, c):
      c.execute("UPDATE tokens SET count = count + 1 WHERE token_str = ?", (token_str,))
      return self(token_str, c)
    else:
      c.execute("INSERT INTO tokens (token_str, count) VALUES (?, ?)", (token_str, 1))
      return c.lastrowid

  def decrement(self, token_str : str, c : sqlite3.Cursor):
    assert self.exists(token_str, c)
    c.execute("UPDATE tokens SET count = count - 1 WHERE token_str = ?", (token_str,))
    return self(token_str, c)

  def search(self, query, c : sqlite3.Cursor):
    assert '"' not in query
    c.execute(f'SELECT token_str, count FROM tokens WHERE token_str LIKE "{query}%" LIMIT 20')
    return c.fetchall()

  @lru_cache(maxsize = 512)
  def __call__(self, token_str : str, c : sqlite3.Cursor):
    if token_str == '':
      return 0
    if token_str[:len(kIntRangePrefix)] == kIntRangePrefix:
      assert IntRange.valid_token(token_str), f"Invalid int token \"{token_str}\""
    else:
      assert is_valid_token(token_str), f"Invalid tag \"{token_str}\""
    c.execute("SELECT rowid FROM tokens WHERE token_str = ?", (token_str,))
    r = c.fetchone()
    if r is not None:
      return r[0]
    c.execute("INSERT INTO tokens (token_str, count) VALUES (?, ?)", (token_str, 0))
    return c.lastrowid


class Index:
  @staticmethod
  def create(conn : sqlite3.Connection, path : str, rankings : [str] = [], force = False):
    ctx = conn.cursor()
    ctx.execute("""
      CREATE TABLE documents (
        data BLOB
    )
    """)

    token_indices = []
    for ranking in rankings:
      token_indices.append(TokenIndex.create(ctx, ranking))

    conn.commit()

    metadata = {
      'token_indices': [ti.json() for ti in token_indices],
      'token_mapper': {},
      'ranges': {},
      'n': 0,
    }

    with open(os.path.join(path, 'metadata.json'), 'w+') as f:
      json.dump(metadata, f, indent=2)

    return Index(conn, path)

  def doc2ranges(self, doc):
    # Subclass this to automatically add ranges based on the document.
    return doc.get('ranges', [])

  def doc2tokens(self, doc):
    # Subclass this to automatically add tokens based on the document.
    tokens = doc.get('tags', [])
    ranges = self.doc2ranges(doc)
    for name in ranges:
      tokens += self.ranges[name].tokens(ranges[name])
    return tokens

  def insert_document(self, doc : dict):
    self.n += 1

    tokens = self.doc2tokens(doc)
    rankings = doc['rankings']

    self.ctx.execute("INSERT INTO documents (data) VALUES (?)", (json.dumps(doc),))
    docid = self.ctx.lastrowid

    token2int = {}
    for token in tokens:
      token2int[token] = self.token_mapper.increment(token, self.ctx)
    for token_index in self.token_indices:
      rank = int(rankings[token_index.name])
      token_index.insert(self.ctx, rank, docid, 0)
      for token in tokens:
        token_index.insert(self.ctx, rank, docid, token2int[token])
    self.conn.commit()
    return docid

  def modify_document(self, docid : int, doc : dict):
    olddoc = self.fetch(docid)
    A = set(self.doc2tokens(olddoc))
    B = set(self.doc2tokens(doc))
    addedTokens = list(B - A)
    deletedTokens = list(A - B)
    unchangedTokens = list(A.intersection(B))

    token2int = {}
    for token in addedTokens:
      token2int[token] = self.token_mapper.increment(token, self.ctx)
    for token in deletedTokens:
      token2int[token] = self.token_mapper.decrement(token, self.ctx)

    rankings = doc['rankings']
    for token_index in self.token_indices:
      rank = int(rankings[token_index.name])
      for token in addedTokens:
        token_index.insert(self.ctx, rank, docid, token2int[token])
      for token in deletedTokens:
        token_index.delete(self.ctx, docid, token2int[token])

    self.ctx.execute("UPDATE documents SET data = ? WHERE rowid = ?", (json.dumps(doc), docid))

    self.conn.commit()

  def fetch(self, docid):
    self.ctx.execute("SELECT data FROM documents WHERE rowid = ?", (docid,))
    return json.loads(self.ctx.fetchone()[0])

  def create_indices(self):
    for token_index in self.token_indices:
      token_index.create_indices(self.ctx)

  def intersect(self, *tags, invert=None):
    if invert is not None:
      assert isinstance(invert, list) or isinstance(invert, tuple)
      assert len(invert) == len(tags)
    else:
      invert = [False] * len(tags)

    token_nodes = []
    range_tokens = {}
    for neg, tag in zip(invert, tags):
      if isinstance(tag, str):
        if self.token_mapper.exists(tag, self.ctx):
          token_nodes.append((TokenNode(self.token_mapper(tag, self.ctx)), neg))
          continue
        elif neg:
          # Skip non-existent, negated tokens.
          continue
        else:
          # A non-existent token automatically implies 0 results.
          return EmptyNode()
      range_name, op, value = tag
      assert neg is False # todo: support negated range queries?
      assert op in ['>', '<']
      if op == '<':
        T = self.ranges[range_name].less_than(value)
      else:
        T = self.ranges[range_name].greater_than(value)
      range_tokens[range_name] = []
      for t in T:
        if self.token_mapper.exists(t, self.ctx):
          range_tokens[range_name].append(self.token_mapper(t, self.ctx))

    range_nodes = {}
    for k in range_tokens:
      T = [(TokenNode(token), False) for token in range_tokens[k]]
      if len(T) == 1:
        range_nodes[k] = [T[0][0]]
      else:
        range_nodes[k] = [OrNode(T)]

    all_nodes = token_nodes + [ (node, False) for node in range_nodes.values() ]

    if sum(x[1] for x in all_nodes) == len(all_nodes):
      # all nodes are inverted
      all_nodes.append((TokenNode(0), False))

    if len(all_nodes) == 1:
      assert not all_nodes[0][1]
      return all_nodes[0][0]
    return AndNode(all_nodes)

  def __len__(self):
    return self.n

  def save(self):
    ranges = {}
    for k in self.ranges:
      ranges[k] = self.ranges[k].json()
    with open(os.path.join(self.path, 'metadata.json'), 'w+') as f:
      json.dump({
        'token_indices': [ti.json() for ti in self.token_indices],
        'token_mapper': {},
        'ranges': ranges,
        'n': self.n,
      }, f, indent=2)

  def add_range(self, name, low, high):
    self.ranges[name] = IntRange(name, low, high)

  def token_search(self, query : str):
    return self.token_mapper.search(query, self.ctx)

  def expression_context(self, ranking : str):
    index = [i for i in self.token_indices if i.name == ranking]
    assert len(index) == 1
    return ExpressionContext(
      self.ctx,
      n = 1,
      r = 1,
      pageLength = 1000,
      index = index[0],
    )

  def __init__(self, conn, path):
    metadata = json.load(open(os.path.join(path, 'metadata.json'), 'r'))
    self.conn = conn
    self.ctx = self.conn.cursor()
    self.path = path
    self.token_indices = [TokenIndex(**data) for data in metadata['token_indices']]
    self.ranges = {}
    for k in metadata['ranges']:
      self.ranges[k] = IntRange(**metadata['ranges'][k])
    self.token_mapper = TokenMapper(self.ctx)
    self.n = metadata['n']

