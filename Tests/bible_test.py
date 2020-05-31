import os, re, shutil, time
pjoin = os.path.join

import spot

def get_tokens(verse):
	return list(set(verse.lower().split(' ')))

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

	words = [
		'clean', 'beasts', 'multiply', 'commandments', 'laws', 'love'
	]
	groundtruth = {}
	for word in words:
		groundtruth[word] = []

	# Insert every word from every verse into the index.
	for i, verse in enumerate(verses):
		tokens = get_tokens(verse)
		index.add_doc(i, value=len(verse))
		for token in tokens:
			index.add(token, docid=i, value=len(verse))
		for word in words:
			if word in tokens:
				groundtruth[word].append(i)
	index.save()

	end_time = time.time()

	# Load index from disk.
	index = None
	index = spot.Index(index_directory)

	for word in words:
		start_time = time.time()
		# Query for verses that contain the word "beasts" and the word "clean"
		fetcher = index.documents_with_token(word)
		results = fetcher.retrieve()
		end_time = time.time()
		assert len(results) == len(groundtruth[word])
		for i, (score, docid) in enumerate(results):
			assert docid in groundtruth[word]
