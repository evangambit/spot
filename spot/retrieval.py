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

_base64 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz{|'

"""
TODO: while keeping files somewhat readable is nice, expanding
to use all 256 values in a byte will let us use a wider range of
values.  This is especially nice for disambiguation.
"""

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
An index consists of a 'header' json and a large 'body' file.

The 'body' file consists of a number of linked-lists, with each
node (aka "Page") of a linked list containing (typically)
hundreds of elements.  An "element" is a (value, docid) pair, as
well as an extra (small) integer that helps disambiguate hash
collisons.

There are 4096 linked lists in the body, with their nodes
interweaving with one another.  Each linked list is a union of
many posting lists.

Insertion into a sorted linked list is typically O(n).  Spot gets
around this by containing the locations of each node of the
linked list in a sorted list of integers.  We perform binary
search to find which page to add a document to, read the entire
page from memory, insert our document, then write the page back.

Unfortunately every 255 insertions (on average, assuming a page
size of 4096) we need to insert a new integer into one of these
arrays, which technically makes insertion linear.  But since
these arrays are in RAM and are of size n/255, the expected
run time is quite fast (in some sense "n / 65025").

We use about 0.16 bytes per token-document pair in the header.
Since the header is stored in RAM, this has some implications
on the effective size of a Spot index.  For instance, 1 million
documents with 1K tokens each requires 160MB of RAM for just the
header (with, ideally, a lot of additional RAM to cache pages).

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
					# Calling os.path.getsize leads to bugs where we've expanded
					# the file but the file hasn't been flushed.  So instead we
					# keep track of 'bodysize' manually.
					"bodysize": 0,
					"buckets": {}
				}, f, indent=1)
			with open(pjoin(path, 'body.txt'), 'w+') as f:
				f.write('')

		self.path = path
		with open(pjoin(path, 'header.json'), 'r') as f:
			self.header = json.load(f)
		self.body = open(pjoin(path, 'body.txt'), 'r+')

		self.pageManager = PageManager(self, self.body, self.header['bodysize'])

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
		return self.pageManager.fetch_page(offset)

	def save(self):
		self.pageManager.save()
		self.body.flush()

		self.header['bodysize'] = self.pageManager.filesize
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
				'page_offsets': [self.pageManager.allocate_page().offset],

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

		page = self.pageManager.fetch_page(offset)

		# Split page if it is full
		if (len(page.lines) + 1) * kLineLength + kPageHeaderSize > kPageSize:
			loc1 = offset

			page2 = self.pageManager.allocate_page()

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
The PageManager is responsible for allocating new pages and
fetching existing pages.  It is also responsible for caching
pages in RAM (see PageManager.kMaxPagesInMemory).

Caching is very primitive right now: when the cache is full, a
random page is removed from the cache.  This is decent
asymptotically (common queries will tend to fill the cache) but
clearly suboptimal.
"""
class PageManager:
	def __init__(self, index, file, filesize):
		self.index = index
		self.file = file
		self.filesize = filesize
		self.pages_in_memory = {}

	# Read the page into RAM if it isn't cached.  Then return
	# the page.
	def fetch_page(self, offset):
		if offset not in self.pages_in_memory:
			self.file.seek(offset)
			page = Page(self, self.file.read(kPageSize), offset)
			self._add_page_to_memory(offset, page)
		return self.pages_in_memory[offset]

	# Allocates a new page on disk.
	def allocate_page(self):
		header = encode_int(0) + ' ' + encode_int(0) + '\n'
		page = Page(self, header + 'x' * (kPageSize - kPageHeaderSize), self.filesize)

		self.index.body.seek(self.filesize)
		self.index.body.write(page._encode())
		self._add_page_to_memory(self.index.pageManager.filesize, page)
		self.index.pageManager.filesize += kPageSize
		return page

	def _add_page_to_memory(self, offset, page):
		if len(self.pages_in_memory) >= PageManager.kMaxPagesInMemory:
			key = random.choice(list(self.pages_in_memory.keys()))
			self.pages_in_memory[key].mark_dead(self.file)
			del self.pages_in_memory[key]
		self.pages_in_memory[offset] = page

	# WARNING: It is crucial that pages be saved to disk before
	# being deleted.
	def save(self):
		for page in self.pages_in_memory.values():
			page.save(self.file)

# The bigger this is, the faster index construction is.  When
# testing this should be small (to catch caching errors), but
# when performance is important it should be large.
PageManager.kMaxPagesInMemory = 32000  # ~128 MB

"""
Invariant: if a 'Page' object exists, it has been allocated.

A 'Page' object represents a contiguous chunk of disk space in
the index's "body" file.  It represents a list of documents and
a pointer to the next Page in the linked list.

The format of the file is:
- the first 8 bytes is the number of entries in the page
- the second 8 bytes is the location of the next page
- every succeeding 16 bytes represents a document
"""
class Page:
	def __init__(self, manager, text, offset):
		self.manager = manager
		self.is_dead = False
		if len(text) == 0:
			self.next_page = 0
			self.lines = []
			self.offset = offset
			return
		length = decode_int(text[0:7])
		self.next_page = decode_int(text[8:15])
		self.lines = [text[i:i+kLineLength-1] for i in range(kPageHeaderSize, kPageHeaderSize + length, kLineLength)]
		assert kLineLength * len(self.lines) == length, f'Expected {kLineLength} * {len(self.lines)} == {length}'
		self.offset = offset
		self.is_modified = False

	# Writes this page to disk (if it has been modified).
	def save(self, file):
		if self.is_modified:
			file.seek(self.offset)
			file.write(self._encode())
		self.is_modified = False

	# While we can delete pages from cache, we cannot delete
	# external references to pages these deleted pages.  Instead
	# we mark these pages as "dead" as an indicator that the page
	# may no longer be accurate (since a new Page object may have
	# been created with the same offset).
	#
	# Fortunately this fails fairly gracefully, since pages don't
	# move around, so any page that is pointing to valid next page
	# will, at worst, skip over some pages accidentally.  If we
	# ever start moving pages around (e.g. to get better locality
	# while reading, or if we start supporting deletion) we may
	# need to revisit how to make Page objects safe.
	def mark_dead(self, file):
		self.save()
		self.is_dead = True

	def add_line(self, line):
		assert kPageHeaderSize + (len(self.lines) + 1) * kLineLength <= kPageSize
		bisect.insort(self.lines, line)
		self.is_modified = True

	def _encode(self):
		length = len(self.lines) * kLineLength
		header = encode_int(length) + ' ' + encode_int(self.next_page) + '\n'
		body = '\n'.join(self.lines) + '\n'
		result = header + body
		result += '~' * (kPageSize - len(result))
		return result
