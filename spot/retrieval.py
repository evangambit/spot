"""
The index consists of two files: a JSON header and file that
contains linked lists of pages.
"""

import bisect, hashlib, json, os, resource, shutil
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

class Index:
	def __init__(self, path):
		if not os.path.exists(path):
			os.mkdir(path)
			with open(pjoin(path, 'header.json'), 'w+') as f:
				json.dump({
					"num_buckets": 4096,
					"buckets": {}
				}, f, indent=1)
			with open(pjoin(path, 'body.txt'), 'w+') as f:
				f.write('')

		self.path = path
		with open(pjoin(path, 'header.json'), 'r') as f:
			self.header = json.load(f)
		self.body = open(pjoin(path, 'body.txt'), 'r+')

		self.bodysize = os.path.getsize(self.body.name)

		self.modified_pages = {}

	def newpage(self):
		self.body.seek(self.bodysize)
		self.body.write('x' * kPageSize)
		page = Page('', self.bodysize)
		self.modified_pages[self.bodysize] = page
		self.bodysize += kPageSize
		return page

	def decode_line(self, line):
		value = decode_int(line[:kValueLength])
		docid = decode_int(line[kValueLength:kValueLength+kDocidLength])
		disambiguator = decode_int(line[-kDisambiguatorLength:])
		return value, docid, disambiguator

	def decode_page_header(self, line):
		assert line[-1] == '\n'
		page_length = decode_int(line[0:7])
		next_page = decode_int(line[8:15])
		return page_length, next_page

	def encode_page_header(self, page_length, next_page):
		return encode_int(page_length) + ' ' + encode_int(next_page) + '\n'

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
		for offset in offsets:
			self.body.seek(offset)
			page = Page(self.body.read(kPageSize), offset)
			for line in page.lines:
				if line[-1:] == disambiguator:
					value, docid, _ = self.decode_line(line)
					yield value, docid

	def save(self):
		for page in self.modified_pages.values():
			page.save(self.body)
		self.modified_pages = {}
		self.body.flush()

		with open(pjoin(self.path, 'header.json'), 'w') as f:
			json.dump(self.header, f, indent=1)

	def add(self, token, docid, value):
		token = hashfn(token)
		# JSON doesn't like integer keys in their maps, so our bucket_ids are strings :/
		bucket_id = str(token % self.header['num_buckets'])
		if bucket_id not in self.header['buckets']:
			self.header['buckets'][bucket_id] = {
				'tokens': [],

				# Offset (in bytes) from beginning of file.
				'page_offsets': [self.newpage().offset],

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

		if offset in self.modified_pages:
			page = self.modified_pages[offset]
		else:
			# Read page into RAM
			self.body.seek(offset)
			page = Page(self.body.read(kPageSize), offset)
			self.modified_pages[offset] = page

		# Split page if it is full
		if (len(page.lines) + 1) * kLineLength + kPageHeaderSize > kPageSize:
			loc1 = offset

			page2 = self.newpage()

			n = len(page.lines)
			left = page.lines[:n//2]
			right = page.lines[n//2:]

			page2.lines = right
			page2.next_page = page.next_page

			page.lines = left
			page.next_page = page2.offset

			bucket['page_offsets'] = bucket['page_offsets'][:page_idx+1] + [page2.offset] + bucket['page_offsets'][page_idx+1:]
			bucket['page_values'] = bucket['page_values'][:page_idx+1] + [page2.lines[0]] + bucket['page_values'][page_idx+1:]

		# Insert new entry, and then write the page to disk.
		bisect.insort(page.lines, line)

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

	def save(self, file):
		file.seek(self.offset)
		file.write(self.encode())

	def encode(self):
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
