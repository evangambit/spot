# Spot

Spot is a Python module for token-based retrieval.

## Retrieval

Suppose you have a document with millions of lines and you want to construct an index that lets you search lines by word.

```Python
# build_index.py

from spot import Retrievable, Index

class MyLine(Retrievable):
	def __init__(self, index, text):
		self.id = index
		self.text = text

  # The tokens associated with the line is the words it contains.
	def tokens(self):
		return self.text.lower().split(' ')

  # Some unique integer associated with the line.
	def docid(self):
		return self.id

  # The value to sort results by during retrieval.  In this case
  # we want to return short lines before long lines.
	def value(self):
		return len(self.text)

with open('myFile.txt', 'r') as f:
  lines = f.readlines()

index = Index('myIndex')
for i, line in enumerate(lines):
  index.add(MyLine(i, verse))
index.save()
```

Now we can load the index from disk and query it.

```Python
# query_index.py
import spot

index = spot.Index('myIndex')

criteria = spot.And(
  index.listFor("hello"),
  index.listFor("world"),
)

# Get first 20 lines that contain the words "hello" and "world"
results, pagination = index.retrieve(criteria, max_results=20)

# Get the next 20:
results, pagination = index.retrieve(criteria, max_results=20, pagination=pagination)
```

Criteria are built by joining iteration-based filters together.  For instance:

```Python
criteria = spot.And(
  index.listFor("foo"),
  spot.Or(
    index.listFor("bar"),
    index.listFor("baz"),
  ),
)

results, pagination = index.retrieve(criteria, max_results=20)
```

With find documents that contain "foo" and "bar" or "baz".  The types of nodes are

- And
- Or
- AtLeast

The "And" and "Or" nodes also let you optionally negate inputs -- for instance:

```Python
spot.And(
  index.listFor("foo"),
  index.listFor("bar"),
  negate=[0, 1]
)
```

Will find documents that contain "foo" but do *not* contain "bar".

The "AtLeast" node lets you use float thresholds and assign different weights to tokens:

```Python
spot.AtLeast(
  index.listFor("foo"),
  index.listFor("bar"),
  index.listFor("baz"),
  threshold=1.0
  negate=[0.4, 0.7, 1.1]
)
```

(note: Nodes do *not* re-order results; they're simple boolean filters)

## Additional Notes

Spot hashes tokens

### Text Search

This kind of token-based retrieval is a well-studied problem and involves all sorts of tricks that vary based on usage. You need to decide on the case sensitivity of your tokens, whether you want to user k-mers, how to deal with non-ascii text, etc.

You will also likely want a list of stop words -- commonly used words that don't carry much semantic meaning ("the", "a", "in", etc.).  Beyond likely making your results more robust, removing very common words helps decrease the index size.

### Post-Retrieval

While expressive, the types of queries you actually wish to perform often cannot be expressed as ands/ors.  Generally queries will take two stages: an initial retrieval stage (performed by Spot) and a slower post-retrieval stage (performed by you), which performs some additional filtering and/or ranking of documents.

### Hashing

Tokens can be arbitrary strings, and every token needs its own file under the hood.  Spot gets around this by hashing tokens.  By default there are 4096 buckets, which means it is quite possible for two different tokens to collide.

*THE ONUS IS ON THE USER TO VERIFY RESULTS ARE CORRECT*.

Spot provides efficient retrieval but, to keep storage space lower, does not store each element's actual tokens, and so cannot verify the correctness of the results for you.

Of note is that rare tokens can end up hashing to the same value as more common tokens, which can mean *most* of the results are incorrect.

