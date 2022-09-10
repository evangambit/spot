import sqlite3


class TokenIndex:
  """
  This class is the main reason this file exists. It manages a SQL
  table of (rank, docid, token) tuples that can be queries via a tree
  of Expression Nodes.
  """
  @staticmethod
  def create(c : sqlite3.Cursor, name : str):
    assert isinstance(c, sqlite3.Cursor)
    assert isinstance(name, str)
    c.execute(f"""CREATE TABLE tokens_{name} (
      rank REAL,
      doc_id INTEGER,
      token INTEGER
    )""")
    index = TokenIndex(name)
    return index

  def json(self):
    return { "name": self.name }
  
  def create_indices(self, c):
    c.execute(f"CREATE INDEX {self.tableName}_index ON {self.tableName}(token, rank, doc_id)")

  def __init__(self, name : str):
    self.name = name
    self.tableName = f"tokens_{name}"

  def docids(self, c : sqlite3.Cursor, token : int, n : int, where : float):
    query = f"SELECT rank, doc_id FROM {self.tableName} WHERE token = ? AND (rank, doc_id) > (?, ?) ORDER BY token ASC, rank ASC, doc_id ASC LIMIT ?"
    c.execute(query, (
      token, where[0], where[1], n
    ))
    return c.fetchall()

  def intersect(self, c : sqlite3.Cursor, tokens : [int], n : int, where : float = None):
    assert len(tokens) > 0
    queries = []
    args = []
    for token in tokens:
      assert type(token) is int
      if where is None:
        queries.append(f"SELECT rank, doc_id FROM {self.tableName} WHERE token = ?")
        args.append(token)
      else:
        queries.append(f"SELECT rank, doc_id FROM {self.tableName} WHERE token = ? AND (rank, doc_id) > (?, ?)")
        args += (token, where[0], where[1])
    c.execute(' intersect '.join(queries), args)
    return c.fetchmany(n)

  # TODO: intersect

  def tokens(self, c : sqlite3.Cursor, doc_id : int):
    assert isinstance(doc_id, int)
    c.execute(f"SELECT token FROM {self.tableName} WHERE doc_id={doc_id}")
    return self.c.fetchall()

  def insert(self, c : sqlite3.Cursor, rank : float, doc_id : int, token : int):
    assert isinstance(c, sqlite3.Cursor)
    assert isinstance(rank, int)
    assert isinstance(doc_id, int)
    assert isinstance(token, int)
    c.execute(f"INSERT INTO {self.tableName} (rank, doc_id, token) VALUES (?, ?, ?)", (
      rank,
      doc_id,
      token,
    ))

  def print(self, c : sqlite3.Cursor):
    for row in c.execute(f"SELECT * FROM {self.tableName}"):
      print(row)

  def delete(self, c : sqlite3.Cursor, doc_id : int, token : int):
    assert isinstance(c, sqlite3.Cursor)
    assert isinstance(doc_id, int)
    assert isinstance(token, int)
    c.execute(f"DELETE FROM {self.tableName} WHERE doc_id = ? AND token = ?", (
      doc_id,
      token,
    ))
