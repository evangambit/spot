import bisect, hashlib, json, os, random, resource, shutil
pjoin = os.path.join

try:
  from .filtering import Expression, And, AndWithNegations
  from .header import *
except(ImportError):
  from filtering import Expression, And, AndWithNegations
  from header import *

__all__ = [
	'TokenINode',
	'Index',
	'hashfn'
]

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

# Every 16 bytes (besides the header) of a page represents
# a (value, docid, disambiguator) tuple, encoded using 7
# bytes for the value and docid, and 2 for the disambiguator.
kLineLength = 16
kMaxValue = (256**7)// 2
kMinValue = (256**7)//-2
kMaxDocid = 256**7
kMaxDisambiguator = 256**2

def decode_line(line):
	value = int.from_bytes(line[:7], 'big', signed=False)
	docid = int.from_bytes(line[7:14], 'big', signed=False)
	disambiguator = int.from_bytes(line[14:], 'big', signed=False)
	return value, docid, disambiguator

def encode_line(value, docid, disambiguator):
	value = value.to_bytes(7, 'big', signed=False)
	docid = docid.to_bytes(7, 'big', signed=False)
	disambiguator = disambiguator.to_bytes(2, 'big', signed=False)
	line = value + docid + disambiguator
	return line

def encode_page_header(length, next_page):
	assert length <= kPageSize - kPageHeaderSize
	return length.to_bytes(7, 'big') + b' ' + next_page.to_bytes(7, 'big') + b'\n'

def decode_page_header(data):
	return int.from_bytes(data[:7], 'big', signed=False), int.from_bytes(data[8:15], 'big', signed=False)

class TokenINode(Expression):
	def __init__(self, index, offsets, disambiguator, page_idx=0, line_idx=-1):
		super().__init__()
		self.index = index
		self.offsets = offsets
		self.disambiguator = disambiguator
		self.page_idx = page_idx
		self.page = self.index.fetch_page(self.offsets[self.page_idx])
		self.line_idx = line_idx

	def step(self):
		if self.currentValue == Expression.kLastVal:
			return self.currentValue
		while True:
			if self.line_idx + 1 < len(self.page.lines):
				self.line_idx += 1
				value, docid, disambiguator = decode_line(self.page.lines[self.line_idx])
				if disambiguator == self.disambiguator:
					self.currentValue = value, docid
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
			"index_id": self.index.id,
			"offsets": self.offsets,
			"disambiguator": self.disambiguator,
			"page_idx": self.page_idx,
			"line_idx": self.line_idx,
			"type": "TokenINode"
		})

	@staticmethod
	def decode(J):
		return TokenINode(
			index = indices[J["index_id"]],
			offsets = J["offsets"],
			disambiguator = J["disambiguator"],
			page_idx = J["page_idx"],
			line_idx = J["line_idx"]
		)
Expression.register("TokenINode", TokenINode)

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
			self.header = Header()
			with open(pjoin(path, 'body.spot'), "w+b") as f:
				f.write(b"")
			with open(pjoin(path, "header.spot"), "w+b") as f:
				self.header.save(f)

		self.path = path
		with open(pjoin(path, "header.spot"), "rb") as f:
			self.header = Header.load(f)
		self.body = open(pjoin(path, "body.spot"), "r+b")

		self.pageManager = PageManager(self, self.body, self.header.bodysize)

	def documents_with_token(self, token):
		token, bucket_id = self._hash(token)
		return self._documents_with_token(token, bucket_id)

	def _documents_with_token(self, token, bucket_id):
		if bucket_id not in self.header.buckets:
			return
		bucket = self.header.buckets[bucket_id]
		if token not in bucket.tokens:
			return
		disambiguator = bucket.tokens.index(token)
		offsets = bucket.page_offsets
		node = TokenINode(self, offsets, disambiguator)
		return node

	# Returns a TokenINode that iterates over all documents.
	def all_documents(self):
		return self._documents_with_token(0, 0)

	def fetch_page(self, offset):
		return self.pageManager.fetch_page(offset)

	# Wrappers around "And" and "Or" nodes.  They automatically
	# add "all_documents" if all inputs are negated.
	def AND(self, *nodes, negation=None):
		if negation is None:
			negation = [0] * len(nodes)
		if sum(negation) == len(nodes):
			return AndWithNegations(self.all_documents(), *nodes, negation=[0] + list(negation))
		elif sum(negation) > 0:
			return AndWithNegations(*nodes, negation=negation)
		return And(*nodes)

	def OR(self, *nodes, negation=None):
		if negation is None:
			negation = [0] * len(nodes)
		if sum(negation) == len(nodes):
			return Or(self.all_documents(), *nodes, negation)
		return Or(*nodes, negation)

	def save(self):
		self.pageManager.save()
		self.body.flush()

		self.header.bodysize = self.pageManager.filesize
		with open(pjoin(self.path, 'header.spot'), "wb") as f:
			self.header.save(f)

	def num_insertions(self):
		return self.header.num_insertions

	def _hash(self, token):
		token = hashfn(token)
		# We reserve bucket 0 for 'all docs'.
		bucket_id = (token % (self.header.num_buckets - 1)) + 1
		return token, bucket_id

	def add_doc(self, docid, value):
		self._add(0, 0, docid, value)

	def add(self, token, docid, value):
		self.header.num_insertions += 1
		token, bucket_id = self._hash(token)
		self._add(token, bucket_id, docid, value)

	def _add(self, token, bucket_id, docid, value):
		if bucket_id not in self.header.buckets:
			self.header.buckets[bucket_id] = Bucket(bucket_id)
			bucket = self.header.buckets[bucket_id]
			bucket.page_offsets.append(self.pageManager.allocate_page().offset)
			bucket.page_values.append(bytearray([0]*16))
		bucket = self.header.buckets[bucket_id]

		# We reserve "kMinValue" and "kMaxValue - 1" for the filter
		# nodes.
		assert value > kMinValue
		assert value < kMaxValue - 1

		assert docid < kMaxDocid

		# Construct the "line" we wish to insert.
		if token not in bucket.tokens:
			if len(bucket.tokens) + 1 > kMaxDisambiguator:
				raise Exception(f"You had more than {kMaxDisambiguator} collisions for one bucket!")
			bucket.tokens.append(token)
		disambiguator = bucket.tokens.index(token)
		line = encode_line(value, docid, disambiguator)

		page_idx = max(bisect.bisect(bucket.page_values, line) - 1, 0)

		offset = bucket.page_offsets[page_idx]

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

			# TODO: use '.insert'
			bucket.page_offsets = UInt64List(bucket.page_offsets[:page_idx+1] + [page2.offset] + bucket.page_offsets[page_idx+1:])
			bucket.page_values = ByteArray16List(bucket.page_values[:page_idx+1] + [page2.lines[0]] + bucket.page_values[page_idx+1:])

			bucket.page_values[page_idx + 0] =  page.lines[0]
			bucket.page_values[page_idx + 1] = page2.lines[0]

			page.is_modified = True
			page2.is_modified = True

			if page2.lines[0] < line:
				page = page2

		# Insert new entry, and then write the page to disk.
		page.add_line(line)
		bucket.page_values[page_idx] = page.lines[0]

Index.id = 0

"""
The PageManager is responsible for allocating new pages and
fetching existing pages.  It is also responsible for caching
pages in RAM (see PageManager.kMaxPagesInMemory).

When the cache is full we just pick a random page to drop.
Hilariously this outperforms using an LRU cache by a large
margin.
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
		header = encode_page_header(length=0, next_page=0)
		assert len(header) == kPageHeaderSize
		page = Page(self, header + b'~' * (kPageSize - kPageHeaderSize), self.filesize)
		assert len(page.lines) == 0
		assert page.next_page == 0

		self.index.body.seek(self.filesize)
		self.index.body.write(page._encode())
		self._add_page_to_memory(self.filesize, page)
		self.filesize += kPageSize
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
Invariant: if a "Page" object exists, it has been allocated.

A "Page" object represents a contiguous chunk of disk space in
the index's "body" file.  It represents a list of documents and
a pointer to the next Page in the linked list.

The format of the file is:
- the first 8 bytes is the number of entries in the page
- the second 8 bytes is the location of the next page
- every succeeding 16 bytes represents a document
"""
class Page:
	def __init__(self, manager, text, offset):
		assert offset % kPageSize == 0
		self.manager = manager
		self.is_dead = False
		self.offset = offset
		if len(text) == 0:
			self.next_page = 0
			self.lines = []
			self.is_modified = True
			return
		length, self.next_page = decode_page_header(text[:kPageHeaderSize])
		self.lines = [text[i:i+kLineLength] for i in range(kPageHeaderSize, kPageHeaderSize + length, kLineLength)]
		self.is_modified = False

	# Writes this page to disk (if it has been modified).
	def save(self, file):
		self.is_modified = True  # TODO: remove
		if self.is_modified:
			file.seek(self.offset)
			code = self._encode()
			file.write(code)

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
		assert len(line) == kLineLength
		assert kPageHeaderSize + (len(self.lines) + 1) * kLineLength <= kPageSize
		bisect.insort(self.lines, line)
		v, i, d = decode_line(line)
		self.is_modified = True

	def _encode(self):
		length = len(self.lines) * kLineLength
		header = encode_page_header(length, self.next_page)
		assert len(header) == kPageHeaderSize, f'{len(header)} != {kPageHeaderSize}; ({length} : {self.next_page})'
		body = b"".join(self.lines)
		result = header + body
		result += b'~' * (kPageSize - len(result))
		return result
