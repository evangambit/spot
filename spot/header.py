
# Encode an integer as 8 bytes.
def encode_int8(x):
	return x.to_bytes(8, 'big')

# Decode an integer from a byte array of length 7 (or 8).
def decode_int8(text):
	return int.from_bytes(text, 'big', signed=False)

"""
In a desparate attempt to KISS, we just write the entire header
to file every time it is saved.  Some time in the future we may
try to make this more efficient.

We can't just use json.dump/load because we have binary arrays...
"""
class UInt64List(list):
	def encode(self):
		return encode_int8(len(self)) + b''.join([encode_int8(x) for x in self])

	def append(self, x):
		assert type(x) is int, x
		super().append(x)

	@staticmethod
	def decode(data):
		r = UInt64List()
		length = decode_int8(data[0:8])
		assert length < 1000000, length
		for i in range(length):
			r.append(decode_int8(data[8*i+8:8*i+16]))
		return r, 8 * (length + 1)

# Encodes array of byte arrays, each of length 16.
class ByteArray16List(list):
	def encode(self):
		return encode_int8(len(self)) + b''.join(self)

	@staticmethod
	def decode(data):
		r = ByteArray16List()
		length = decode_int8(data[0:8])

		offset = 8
		for i in range(length):
			r.append(bytearray(data[offset:offset+16]))
			offset += 16

		return r, offset

class Bucket:
	def __init__(self, id_):
		self.id = id_
		# List of tokens stored in this bucket.
		self.tokens = UInt64List()
		# Number of bytes each page is from the front of the body file
		self.page_offsets = UInt64List()
		# The first value of each page
		self.page_values = ByteArray16List()

	def __eq__(a, b):
		if a.id != b.id:
			return False
		if a.tokens != b.tokens:
			return False
		if a.page_offsets != b.page_offsets:
			return False
		if a.page_values != b.page_values:
			return False
		return True

	@staticmethod
	def decode(data):
		bucket = Bucket(decode_int8(data[0:8]))
		offset = 8

		tokens, delta = UInt64List.decode(data[offset:])
		offset += delta

		page_offsets, delta = UInt64List.decode(data[offset:])
		offset += delta

		page_values, delta = ByteArray16List.decode(data[offset:])
		offset += delta

		bucket.tokens = tokens
		bucket.page_offsets = page_offsets
		bucket.page_values = page_values

		return bucket, offset

	def encode(self):
		return encode_int8(self.id) + self.tokens.encode() + self.page_offsets.encode() + self.page_values.encode()

class BucketList(list):
	def encode(self):
		for x in self:
			assert type(x) is Bucket
		return encode_int8(len(self)) + b''.join([x.encode() for x in self])

	@staticmethod
	def decode(data):
		r = BucketList()
		length = decode_int8(data[0:8])

		offset = 8
		for i in range(length):
			bucket, delta = Bucket.decode(data[offset:])
			r.append(bucket)
			offset += delta

		return r, offset

class Uint64Dict(dict):
	def encode(self):
		return encode_int8(len(self)) + UInt64List(self.keys()).encode() + b''.join([x.encode() for x in self.values()])

	def __eq__(a, b):
		for k in a:
			if k not in b:
				return False
			if b[k] != a[k]:
				return False
		return True

	@staticmethod
	def decode(data):
		r = Uint64Dict()
		length = decode_int8(data[0:8])
		offset = 8

		keys, delta = UInt64List.decode(data[offset:])
		offset += delta

		vals = []
		for i in range(length):
			v, delta = Bucket.decode(data[offset:])
			vals.append(v)
			offset += delta

		r = Uint64Dict()
		for k, v in zip(keys, vals):
			r[k] = v
		return r, offset

class Header:
	def __init__(self, num_buckets=4096):
		self.num_buckets = num_buckets
		self.num_insertions = 0
		# Calling os.path.getsize leads to bugs where we've expanded
		# the file but the file hasn't been flushed.  So instead we
		# keep track of 'bodysize' manually.
		self.bodysize = 0
		self.buckets = Uint64Dict()

	def encode(self):
		return encode_int8(self.num_buckets) + encode_int8(self.num_insertions) + encode_int8(self.bodysize) + self.buckets.encode()

	@staticmethod
	def decode(data):
		header = Header(num_buckets=decode_int8(data[:8]))
		header.num_insertions = decode_int8(data[8:16])
		header.bodysize = decode_int8(data[16:24])
		header.buckets, delta = Uint64Dict.decode(data[24:])
		return header, delta + 24

	@staticmethod
	def load(file):
		header, _ = Header.decode(file.read())
		return header

	def __eq__(a, b):
		assert type(a) is type(b)
		if a.num_buckets != b.num_buckets:
			return False
		if a.num_insertions != b.num_insertions:
			return False
		if a.bodysize != b.bodysize:
			return False
		if a.buckets != b.buckets:
			return False
		return True

	def save(self, file):
		file.seek(0)
		file.write(self.encode())

header = Header()

