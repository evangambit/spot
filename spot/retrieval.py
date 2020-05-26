import bisect, hashlib, json, os, random, resource, shutil
pjoin = os.path.join

def pad(t, n, c=' '):
	if type(t) is not str:
		t = str(t)
	return max(n - len(t), 0) * c + t

class Hash64:
  def __init__(self):
    pass
  def __call__(self, x):
    h = hashlib.sha256()
    h.update(x.encode())
    return int(h.hexdigest()[-16:], 16)
hashfn = Hash64()

kPageSize = resource.getpagesize()
kPageHeaderSize = 16

kLineLength = 16
kValueLength = 7
kDocidLength = 7
kDisambiguatorLength = 1
assert kValueLength + kDocidLength + kDisambiguatorLength + 1 == kLineLength

kMaxValue = 64**kValueLength
kMaxDocid = 64**kDocidLength
kMaxDisambiguator = 64**kDisambiguatorLength

# The bigger this is, the faster index construction is.
kMaxPagesInMemory = 8000  # ~32 MB

_base64 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz{|'

# Converts an int to a 7-character string
def encode_int(x):
	return _base64[(x >> 36) % 64] + _base64[(x >> 30) % 64] + _base64[(x >> 24) % 64] + _base64[(x >> 18) % 64] + _base64[(x >> 12) % 64] + _base64[(x >> 6) % 64] + _base64[x % 64]

# Converts a 7-character string to an int
def decode_int(text):
	r = 0
	for c in text:
		r *= 64
		assert c in _base64, f'"{c}" not found'
		r += _base64.index(c)
	return r

assert decode_int(encode_int(0)) == 0
assert decode_int(encode_int(10)) == 10
assert decode_int(encode_int(100)) == 100
assert decode_int(encode_int(1000)) == 1000

try:
  from .filtering import Expression
except(ImportError):
  from filtering import Expression


def decode_line(line):
	value = decode_int(line[:kValueLength])
	docid = decode_int(line[kValueLength:kValueLength+kDocidLength])
	disambiguator = decode_int(line[-kDisambiguatorLength:])
	return value, docid, disambiguator

class TokenINode(Expression):
	def __init__(self, index, offsets, disambiguator, page_idx=0, line_idx=-1):
		super().__init__()
		self.index = index
		self.offsets = offsets
		self.disambiguator = disambiguator

		self.page_idx = page_idx
		self.page = self.index.fetch_page(self.offsets[self.page_idx])
		self.line_idx = line_idx

	def _is_valid(self, line):
		return line[-1:] == self.disambiguator

	def step(self):
		if self.currentValue == Expression.kLastVal:
			return self.currentValue
		while True:
			if self.line_idx + 1 < len(self.page.lines):
				self.line_idx += 1
				if self._is_valid(self.page.lines[self.line_idx]):
					self.currentValue = decode_line(self.page.lines[self.line_idx])[:-1]
					return self.currentValue
				continue

			if self.page_idx == len(self.offsets) - 1:
				self.currentValue = Expression.kLastVal
				return self.currentValue

			self.page_idx += 1
			self.line_idx = -1
			self.page = self.index.fetch_page(self.offsets[self.page_idx])

	def encode(self):
		return json.dumps({
			'index_id': self.index.id,
			'offsets': self.offsets,
			'disambiguator': self.disambiguator,
			'page_idx': self.page_idx,
			'line_idx': self.line_idx,
			'type': 'TokenINode'
		})

	@staticmethod
	def decode(J):
		return TokenINode(
			index = indices[J['index_id']],
			offsets = J['offsets'],
			disambiguator = J['disambiguator'],
			page_idx = J['page_idx'],
			line_idx = J['line_idx']
		)
Expression.register('TokenINode', TokenINode)


# A mapping from index IDs to indices.
indices = {}

"""
TODO

Let's be a little smarter with how in-memory pages work.  For
instance right now we just give pages to TokenINode and pray
that the offsets/page-splitting works.

We need to guarantee that every page on disk exists in exactly
on place in memory, that all pages are written when the index
is saved, and that when pages are split we don't break querying.
"""
class Index:
	def __init__(self, path):
		self.id = Index.id
		Index.id += 1
		indices[self.id] = self

		if not os.path.exists(path):
			os.mkdir(path)
			with open(pjoin(path, 'header.json'), 'w+') as f:
				json.dump({
					"num_buckets": 4096,
					"num_insertions": 0,
					"buckets": {}
				}, f, indent=1)
			with open(pjoin(path, 'body.txt'), 'w+') as f:
				f.write('')

		self.path = path
		with open(pjoin(path, 'header.json'), 'r') as f:
			self.header = json.load(f)
		self.body = open(pjoin(path, 'body.txt'), 'r+')

		self.bodysize = os.path.getsize(self.body.name)

		self.pages_in_memory = {}

	def _add_page_to_memory(self, offset, page):
		if len(self.pages_in_memory) >= kMaxPagesInMemory:
			# TODO: right now we assume the pages in memory used
			# during retrieval won't ever trigger a page deletion.
			# It's unclear what would happen during retrieval if a
			# page was deleted... I should think about this.
			key = random.choice(list(self.pages_in_memory.keys()))
			self.pages_in_memory[key].save(self.body)
			del self.pages_in_memory[key]
		self.pages_in_memory[offset] = page

	def _newpage(self):
		self.body.seek(self.bodysize)
		self.body.write('x' * kPageSize)
		page = Page('', self.bodysize)
		self._add_page_to_memory(self.bodysize, page)
		self.bodysize += kPageSize
		return page

	def documents_with_token(self, token):
		token = hashfn(token)
		bucket_id = str(token % self.header['num_buckets'])
		if bucket_id not in self.header['buckets']:
			return
		bucket = self.header['buckets'][bucket_id]
		if token not in bucket['tokens']:
			return
		disambiguator = encode_int(bucket['tokens'].index(token))[-kDisambiguatorLength:]
		offsets = bucket['page_offsets']
		return TokenINode(self, offsets, disambiguator)

	def fetch_page(self, offset):
		self.body.seek(offset)
		return Page(self.body.read(kPageSize), offset)

	def save(self):
		for page in self.pages_in_memory.values():
			page.save(self.body)
		self.pages_in_memory = {}
		self.body.flush()

		with open(pjoin(self.path, 'header.json'), 'w') as f:
			json.dump(self.header, f, indent=1)

	def num_insertions(self):
		return self.header['num_insertions']

	def add(self, token, docid, value):
		self.header['num_insertions'] += 1
		token = hashfn(token)
		# JSON doesn't like integer keys in their maps, so our bucket_ids are strings :/
		bucket_id = str(token % self.header['num_buckets'])
		if bucket_id not in self.header['buckets']:
			self.header['buckets'][bucket_id] = {
				'tokens': [],

				# Offset (in bytes) from beginning of file.
				'page_offsets': [self._newpage().offset],

				# The value of first element of page.
				# Since the page is currently empty we make this ' '.
				'page_values': [' '],
			}
		bucket = self.header['buckets'][bucket_id]

		assert value < kMaxValue
		assert docid < kMaxDocid

		# Construct the "line" we wish to insert.
		value = encode_int(value)
		docid = encode_int(docid)
		if token not in bucket['tokens']:
			assert len(bucket['tokens']) + 1 < kMaxDisambiguator
			bucket['tokens'].append(token)
		disambiguator = encode_int(bucket['tokens'].index(token))[-kDisambiguatorLength:]
		line = value + docid + disambiguator
		assert len(line) == kLineLength - 1

		page_idx = bisect.bisect(bucket['page_values'], line) - 1
		offset = bucket['page_offsets'][page_idx]

		if offset in self.pages_in_memory:
			page = self.pages_in_memory[offset]
		else:
			# Read page into RAM
			self.body.seek(offset)
			page = Page(self.body.read(kPageSize), offset)
			self._add_page_to_memory(offset, page)

		# Split page if it is full
		if (len(page.lines) + 1) * kLineLength + kPageHeaderSize > kPageSize:
			loc1 = offset

			page2 = self._newpage()

			n = len(page.lines)
			left = page.lines[:n//2]
			right = page.lines[n//2:]

			page2.lines = right
			page2.next_page = page.next_page

			page.lines = left
			page.next_page = page2.offset

			bucket['page_offsets'] = bucket['page_offsets'][:page_idx+1] + [page2.offset] + bucket['page_offsets'][page_idx+1:]
			bucket['page_values'] = bucket['page_values'][:page_idx+1] + [page2.lines[0]] + bucket['page_values'][page_idx+1:]

			page.is_modified = True
			page2.is_modified = True

		# Insert new entry, and then write the page to disk.
		page.add_line(line)

Index.id = 0

"""
The fact that a Page object exists means it has been allocated.
"""
class Page:
	def __init__(self, text, offset):
		if len(text) == 0:
			self.next_page = 0
			self.lines = []
			self.offset = offset
			return
		length = decode_int(text[0:7])
		self.next_page = decode_int(text[8:15])
		self.lines = text[16:].split('\n')[:-1]
		if '~' in self.lines[-1] or len(self.lines) == 0:
			self.lines.pop()
		assert kLineLength * len(self.lines) == length, f'Expected {kLineLength} * {len(self.lines)} == {length}'
		self.offset = offset
		self.is_modified = False

	def save(self, file):
		if self.is_modified:
			file.seek(self.offset)
			file.write(self._encode())
		self.is_modified = False

	def add_line(self, line):
		bisect.insort(self.lines, line)
		self.is_modified = True

	def _encode(self):
		length = len(self.lines) * kLineLength
		header = encode_int(length) + ' ' + encode_int(self.next_page) + '\n'
		body = '\n'.join(self.lines) + '\n'
		amount_of_padding = kPageSize - len(header) - len(body)
		if amount_of_padding > 0:
			padding = '~' * (amount_of_padding - 1) + '\n'
		else:
			padding = ''
		result = header + body + padding
		assert len(result) == kPageSize
		return result
