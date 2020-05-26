import math, os, re, shutil, sys, time

pjoin = os.path.join

from spot import Index

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
	index = Index(index_directory)

	# Insert every word from every verse into the index.
	for i, verse in enumerate(verses):
		for token in get_tokens(verse):
			index.add(token, docid=i, value=len(verse))
	index.save()

	end_time = time.time()

	print('Index constructed in %.2f seconds' % (end_time - start_time))

	# Load index from disk.
	index = None
	index = Index(index_directory)

	# Query for verses that contain the word "beasts"
	start_time = time.time()
	results = list(index.iter_for_token('beasts'))
	end_time = time.time()

	print('Index queried in %.4f seconds' % (end_time - start_time))
	print(f"{len(results)} results found!")

	for i, (score, docid) in enumerate(results):
		verse = verses[docid]
		if len(verse) > 100:
			print(i, verse[:97] + '...')
		else:
			print(i, verse)

