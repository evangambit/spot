"""
An example that creates an index that contains every verse in
the Bible.

390k words are inserted at around 100k words/second (on my
solid-state laptop).  The index's disk usage is 22MB.
"""

import os, re, shutil, time
pjoin = os.path.join

import spot

stopwords = ['the', 'a', 'an']
def get_tokens(verse):
	tokens = set(verse.lower().split(' '))
	for stopword in stopwords:
		if stopword in tokens:
			tokens.remove(stopword)
	return list(tokens)

if __name__ == '__main__':
	# Put all verses from the Bible into a list.
	with open(pjoin('Examples', 'Bible', 'bible.txt'), 'r') as f:
		lines = f.read().split('\n\n')
		lines = [re.sub(r"\n", " ", line) for line in lines]
		lines = [line.strip() for line in lines]
		lines = [line for line in lines if len(line) > 0]
	verses = [line for line in lines if re.match(r"^\d+:\d+", line)]

	# Delete index if it already exists.
	index_directory = pjoin('Examples', 'Bible', 'index')
	if os.path.exists(index_directory):
		shutil.rmtree(index_directory)

	start_time = time.time()

	# Create index.
	index = spot.Index(index_directory)

	# Insert every word from every verse into the index.
	groundtruth = []
	for i, verse in enumerate(verses):
		tokens = get_tokens(verse)
		if 'beasts' in tokens and 'clean' in tokens:
			groundtruth.append(i)
		index.add_doc(i, value=len(verse))
		for token in tokens:
			index.add(token, docid=i, value=len(verse))
	index.save()

	end_time = time.time()

	print('Index constructed in %.2f seconds' % (end_time - start_time))
	print('Inserted %i token-verse pairs and %i verses' % (index.num_insertions(), len(verses)))

	# Load index from disk.
	index = None
	index = spot.Index(index_directory)

	start_time = time.time()
	# Query for verses that contain the word "beasts" and the word "clean"
	fetcher = index.AND(
		index.documents_with_token('clean'),
		index.documents_with_token('beasts')
	)
	results = fetcher.retrieve()
	end_time = time.time()

	print(f"Query completed in %.4f seconds" % (end_time - start_time))
	print(f"{len(results)}/{len(groundtruth)} results found!")

	for i, (score, docid) in enumerate(results):
		assert docid in groundtruth
		verse = verses[docid]
		if len(verse) > 100:
			print(i, docid, verse[:97] + '...')
		else:
			print(i, docid, verse)





