import bisect, hashlib, json, os, random, sqlite3, time
pjoin = os.path.join

class Hash64:
  def __init__(self):
    pass
  def __call__(self, x):
    h = hashlib.sha256()
    h.update(x.encode())
    return int(h.hexdigest()[-16:], 16) % ((1 << 63) - 1)
hashfn = Hash64()

kReservedHash = (1 << 63) - 1

kDefaultChunkSize = 1024

# Memory is O(tokens * (rankings + ranges))
class Index:
  @staticmethod
  def create(path, rankings=['score'], ranges=['date_created']):
    assert not os.path.exists(path)
    for rank in rankings:
      assert type(rank) == str
      assert len(rank) > 0
      assert rank[0] != '-'
    columns = [
      "docid INTEGER",
      "token_hash INTEGER"
    ] + [
      f"{r}_rank REAL" for r in rankings
    ] + [
      f"{r}_range REAL" for r in ranges
    ]
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("CREATE TABLE documents (docid INTEGER PRIMARY KEY, key INTEGER, json string) WITHOUT ROWID")
    c.execute(f"CREATE TABLE tokens ({', '.join(columns)})")
    c.execute("CREATE TABLE token_counts (token_hash INTEGER, token string, count INTEGER)")
    conn.commit()
    return Index(path, conn)
  
  # Typically call this *after* constructing your initial index, since
  # it's faster to compute indices *after* inserting your rows.
  def create_indices(self):
    # Lets us quickly find all comments for a given post.
    self.c.execute(f"CREATE INDEX postid_index ON documents(postid, docid)")

    # Lets us quickly find all tokens for a given document.
    self.c.execute(f"CREATE INDEX docid_index ON tokens(docid)")

    self.c.execute(f"CREATE INDEX token_index ON token_counts(count, token_hash)")

    # Lets us quickly iterate over all documents with a token, ordered by a
    # ranking.
    for r in self.rankings:
      self.c.execute(f"CREATE INDEX {r}_index ON tokens(token_hash, {r}_rank, docid)")
    self.commit()

  def __init__(self, path, conn=None):
    if conn:
      self.conn = conn
    else:
      self.conn = sqlite3.connect(path)
    self.c = self.conn.cursor()

    self.c.execute("PRAGMA table_info(tokens)")
    cols = [c[1] for c in self.c.fetchall()]
    assert cols[0] == 'docid'
    assert cols[1] == 'token_hash'
    self.rankings = [c[:-5] for c in cols if c[-5:] == '_rank']
    self.ranges = [c[:-6] for c in cols if c[-6:] == '_range']

  def commit(self):
    self.conn.commit()

  def delete(self, docid):
    self.c.execute(f'DELETE FROM documents WHERE docid = {docid}')
    # TODO: update tokens and token_counts tables.
    return self.c.fetchall()

  def tokens(self, docid):
    self.c.execute(f"SELECT token_hash FROM tokens WHERE docid={docid}")
    return self.c.fetchall()

  def replace(self, jsondata):
    self.insert(jsondata, _command='REPLACE')

  def dump(self, fn):
    with self.conn:
      self.c.execute('SELECT json FROM documents')
      result = self.c.fetchone()
      with open(fn, 'w') as f:
        while result is not None:
          f.write(result[0] + '\n')
          result = self.c.fetchone()

  """
  Special keys:
    docid (auto-populated)
    tokens
  """
  def insert(self, jsondata, _command='INSERT'):
    with self.conn:
      if _command == "REPLACE":
        # Make sure the document already exists.
        self.c.execute(f'SELECT json FROM documents WHERE docid={jsondata["docid"]}')
        oldjson = json.loads(self.c.fetchone()[0])
      else:
        oldjson = None

      # If the user doesn't provide a docid, create our own.
      if 'docid' not in jsondata:
        assert _command != 'REPLACE'
        self.c.execute("SELECT COALESCE(MAX(docid), 0) FROM documents")
        jsondata['docid'] = self.c.fetchone()[0] + 1

      self.c.execute(
        f"{_command} INTO documents VALUES (?, ?, ?)",
        (jsondata['docid'], jsondata.get('key', 0), json.dumps(jsondata))
      )

    # Delete existing tokens
    if _command == "REPLACE":
      self.c.execute(f"DELETE FROM tokens WHERE docid={jsondata['docid']}")
      for token in oldjson["tokens"] + ['']:
        if token == '':
          tokenHash = kReservedHash
        else:
          tokenHash = hashfn(token)
        print('-', repr(token), tokenHash)
        self.c.execute(f'SELECT count FROM token_counts WHERE token="{token}"')
        count = self.c.fetchone()[0]
        print(count, '->', count - 1)
        self.c.execute(f'DELETE FROM token_counts WHERE token="{token}"')
        self.c.execute('INSERT INTO token_counts VALUES (?, ?, ?)', (tokenHash, token, count - 1))

    columnValues = [
      jsondata['docid'],
      0,  # Placeholder for token hash
    ] + [
      jsondata[r] for r in self.rankings
    ] + [
      jsondata[r] for r in self.ranges
    ]
    insertionString = "INSERT INTO tokens VALUES (" + ",".join(["?"] * len(columnValues)) + ")"

    # tokens.add(kReservedHash)  # All documents get this hash (so we can support negated search)
    for token in jsondata["tokens"] + ['']:
      if token == '':
        tokenHash = kReservedHash
      else:
        tokenHash = hashfn(token)
      columnValues[1] = tokenHash
      self.c.execute(insertionString, columnValues)
      # Update token_counts
      self.c.execute(f'SELECT count FROM token_counts WHERE token="{token}"')
      result = self.c.fetchone()
      if result is not None:
        self.c.execute(f'DELETE FROM token_counts WHERE token="{token}"')
        count = result[0]
      else:
        count = 0
      print('+', repr(token), tokenHash, count + 1)
      self.c.execute('INSERT INTO token_counts VALUES (?, ?, ?)', (tokenHash, token, count + 1))
    self.conn.commit()

  def tokens_counts(self):
    self.c.execute('SELECT token, count FROM token_counts')
    return self.c.fetchall()

  # NOTE: this will lose information regarding
  def recompute_token_counts(self):
    self.c.execute('DROP TABLE token_counts')
    self.c.execute("CREATE TABLE token_counts (token_hash INTEGER, token string, count INTEGER)")
    # NOTE: we can't just do
    # "SELECT token_hash, COUNT(token_hash) FROM tokens GROUP BY token_hash"
    # Since we won't know what tokens correspond to each token_hash.
    C = {'': 0}
    self.c.execute('SELECT json FROM documents')
    for j, in self.c.fetchall():
      tokens = json.loads(j)['tokens']
      C[''] += 1
      for token in tokens:
        C[token] = C.get(token, 0) + 1
    for token in C:
      self.c.execute('INSERT INTO token_counts VALUES (?, ?, ?)', (
        kReservedHash if token == '' else hashfn(token), token, C[token]
      ))
    self.commit()

  def num_occurrences(self, token):
    self.c.execute(f'SELECT count FROM token_counts WHERE token_hash={hashfn(token)}')
    return self.c.fetchone()[0]

  def common_tokens(self, n, prefix=None):
    if prefix:
      self.c.execute(f'SELECT token, count FROM token_counts WHERE token LIKE "{prefix}%" ORDER BY -count LIMIT {n}')
    else:
      self.c.execute(f'SELECT token, count FROM token_counts ORDER BY -count LIMIT {n}')
    return self.c.fetchall()

  def json_from_docid(self, docid):
    if docid < 0:
      docid *= -1
    self.c.execute(f"SELECT json FROM documents WHERE docid={docid}")
    r = self.c.fetchone()
    if r is None:
      return None
    return json.loads(r[0])

  def all_iterator(self, ranking, range_requirements=[], chunksize=kDefaultChunkSize, limit=float('inf'), offset=0):
    return self._token_iterator(kReservedHash, ranking, range_requirements, chunksize, limit, offset)

  def token_iterator(self, token, ranking, range_requirements=[], chunksize=kDefaultChunkSize, limit=float('inf'), offset=0):
    return self._token_iterator(hashfn(token), ranking, range_requirements, chunksize, limit, offset)

  def not_token_iterator(self, token, ranking, range_requirements=[], chunksize=kDefaultChunkSize, limit=float('inf'), offset=0):
    all_it = self.all_iterator(ranking, chunksize, limit, offset)
    token_it = self._token_iterator(hashfn(token), ranking, range_requirements, chunksize, limit, offset)

    try:
      a = next(token_it)
    except StopIteration:
      return all_it

    try:
      b = next(all_it)
    except StopIteration:
      return

    while True:
      while a == b:
        try:
          a = next(token_it)
        except StopIteration:
          return all_it
        try:
          b = next(all_it)
        except StopIteration:
          return
      yield b
      try:
        b = next(all_it)
      except StopIteration:
        return

  def _token_iterator(self, hashedToken, ranking, range_requirements, chunksize, limit, offset):
    if ranking[0] == '-':
      order = 'DESC'
      ranking = ranking[1:]
      uop = '-'
    else:
      order = 'ASC'
      uop = ''

    ranges = []
    for name, op, val in range_requirements:
      assert name in self.ranges
      ranges.append(f'{name}_range {op} {val}')
    ranges = ' AND '.join(ranges)
    if len(ranges) > 0:
      ranges = 'AND ' + ranges

    assert ranking in self.rankings
    r = []
    i = 0
    num_returned = 0
    while True:
      if i >= len(r):
        i -= len(r)
        offset += len(r)
        sql_command = f"""
          SELECT {uop}{ranking}_rank, {uop}docid
          FROM tokens
          WHERE token_hash={hashedToken}
          {ranges}
          ORDER BY {ranking}_rank {order}, docid {order}
          LIMIT {chunksize}
          OFFSET {offset}"""
        print(sql_command)
        # try:
        r = self.c.execute(sql_command).fetchall()
        # except:
        #   print(sql_command)
        #   raise Exception('eek')
      if i >= len(r):
        return
      yield r[i]
      num_returned += 1
      if num_returned >= limit:
        return
      i += 1

