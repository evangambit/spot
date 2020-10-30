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
    c.execute("CREATE TABLE documents (docid INTEGER PRIMARY KEY, created_utc REAL, last_modified REAL, json string) WITHOUT ROWID")
    c.execute(f"CREATE TABLE tokens ({', '.join(columns)})")
    conn.commit()
    return Index(path, conn)
  
  # Typically call this *after* constructing your initial index, since
  # it's faster to compute indices *after* inserting your rows.
  def create_indices(self):
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
    return self.c.fetchall()

  def tokens(self, docid):
    self.c.execute(f"SELECT token_hash FROM tokens WHERE docid={docid}")
    return self.c.fetchall()

  def replace(self, docid, created_utc, tokens, jsondata):
    self.insert(docid, created_utc, tokens, jsondata, _command='REPLACE')

  def insert(self, docid, created_utc, tokens, jsondata, _command='INSERT'):
    self.c.execute(
      f"{_command} INTO documents VALUES (?, ?, ?, ?)",
      (docid, created_utc, time.time(), json.dumps(jsondata))
    )

    columnValues = [
      docid,
      0,  # Placeholder for token hash
    ] + [
      jsondata[r] for r in self.rankings
    ] + [
      jsondata[r] for r in self.ranges
    ]
    insertionString = "INSERT INTO tokens VALUES (" + ",".join(["?"] * len(columnValues)) + ")"
    tokens = set([hashfn(t) for t in tokens])
    tokens.add(kReservedHash)  # All documents get this hash (so we can support negated search)
    for token in tokens:
      columnValues[1] = token
      self.c.execute(insertionString, columnValues)

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

