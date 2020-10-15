"""
An example that creates an index that contains every verse in
the Bible.

390k words are inserted at around 100k words/second (on my
solid-state laptop).  The index's disk usage is 22MB.
"""

import os, re, shutil, time
pjoin = os.path.join

def lpad(t, n, c=' '):
  if type(t) is not str:
    t = str(t)
  return max(n - len(t), 0) * c + t

from spot import Index, intersect

stopwords = ['the', 'a', 'an']
def get_tokens(verse):
	tokens = set(verse.lower().split(' '))
	for stopword in stopwords:
		if stopword in tokens:
			tokens.remove(stopword)
	return list(tokens)

# index = Index.create(':memory:', rankings=["score"], ranges=["date_created"])
# index.insert(0, tokens=["foo", "bar"], jsondata={
#   "score": 10,
#   "date_created": 10,
#   "document_text": "foo bar"
# })
# index.insert(1, tokens=["foo", "baz"], jsondata={
#   "score": 3,
#   "date_created": 11,
#   "document_text": "foo baz"
# })
# index.insert(2, tokens=["bar", "baz"], jsondata={
#   "score": 7,
#   "date_created": 12,
#   "document_text": "bar baz"
# })

if __name__ == '__main__':
	# Put all verses from the Bible into a list.
	with open(pjoin('Examples', 'Bible', 'bible.txt'), 'r') as f:
		lines = f.read().split('\n\n')
		lines = [re.sub(r"\n", " ", line) for line in lines]
		lines = [line.strip() for line in lines]
		lines = [line for line in lines if len(line) > 0]
	verses = [line for line in lines if re.match(r"^\d+:\d+", line)]
	verses = [verse.strip() for verse in verses]
	# Remove numbers at the front of the verse.
	verses = [' '.join(v.split(' ')[1:]) for v in verses]

	# Delete index if it already exists.
	index_path = pjoin('Examples', 'Bible', 'index')
	if os.path.exists(index_path):
		os.remove(index_path)

	# Find all verses that contain 'clean' and 'beasts'
	groundtruth = []
	for i, verse in enumerate(verses):
		tokens = get_tokens(verse)
		if 'beasts' in tokens and 'clean' in tokens:
			groundtruth.append(i)

	start_time = time.time()
	index = Index.create(index_path, rankings=["length"], ranges=[])

	# Insert every word from every verse into the index.
	num_tokens_inserted = 0
	for i, verse in enumerate(verses):
		tokens = get_tokens(verse)
		index.insert(i, tokens, {
			"verse": verse,
			"length": len(verse)
		})
		num_tokens_inserted += len(tokens)

	index.commit()

	end_time = time.time()

	print('Index constructed in %.2f seconds' % (end_time - start_time))
	print('Inserted %i token-verse pairs and %i verses' % (num_tokens_inserted, len(verses)))

	# Reload index from disk.
	index = None
	index = Index(index_path)

	start_time = time.time()
	# Query for verses that contain the word "beasts" and the word "clean"
	iterator = intersect(
		index.token_iterator(token='clean', ranking='length'),
		index.token_iterator(token='beasts', ranking='length')
	)
	results = list(iterator)
	end_time = time.time()

	print(f"Query completed in %.4f seconds" % (end_time - start_time))
	print(f"{len(results)}/{len(groundtruth)} results found!")

	for i, (score, docid) in enumerate(results):
		assert docid in groundtruth
		verse = verses[docid]
		if len(verse) > 100:
			print(i, lpad(docid, 4), verse[:97] + '...')
		else:
			print(i, lpad(docid, 4), verse)





