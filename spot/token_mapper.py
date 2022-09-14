import re
import sqlite3
from functools import lru_cache

kIntRangePrefix = '#'


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
    c.execute(f'SELECT token_str, count FROM tokens WHERE token_str LIKE "{query}%" ORDER BY count DESC LIMIT 20')
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
