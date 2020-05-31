# Spot

Spot is a Python module for token-based retrieval.

## Retrieval

Suppose you have a document with millions of lines and you want to construct an index that lets you search lines by word.

```Python
import spot

with open('my-file.txt', 'r') as f:
  lines = f.readlines()

# Create index.
index = spot.Index('my-index')

for i, line in enumerate(lines):
  for token in lines.lower().split(' '):
    index.add(token=token, docid=i, value=len(line))
index.save()

print('Index constructed in %.2f seconds' % (end_time - start_time))
```

Now we can load the index from disk and query it.

```Python
import spot

index = spot.Index('my-index')

# Query for verses that contain the word "dog"
for score, docid in index.documents_with_token('dog'):
  print(lines[docid])
```

Because these results are iterator-based, they can be passed as inputs to other iterators, which lets you buid up complex queries.

```Python
import spot

index = spot.Index('my-index')

fetcher = spot.And(
  index.documents_with_token("hello"),
  spot.Or(
    index.documents_with_token("world"),
    index.documents_with_token("world!"),
  )
)

# Get the first 20 lines that contain the word "hello" and either the word "world" or "world!"
results = fetcher.retrieve(max_results=20)
```

The types of nodes currently supported are

- And
- Or

## Additional Notes

1. Spot expects either 64-bit integer tokens or strings.  Strings are hashed to 64-bit integers, which means that it is possible for hashing collisions to make results inaccurate.

2. Doc IDs should be *unsigned* 56-bit integers.  Values are *signed* 56-bit integers, but with the smallest and largest values excluded (these are used to make the filtering code a bit nicer).

3. As an implementation detail, these hashes are forced into a range of [0, 4095], as this helps safe disk/memory.  The full 64-bit hashes are checked (indirectly) during retrieval, so from a user's perspective "this index hashes strings to 64 bit integers" is an accurate abstraction.  The only leakiness is that each bucket of tokens can only contain a maximum of 65k tokens.  In practice you should be perfectly safe even at millions of tokens (and if more than 65k collisions ever happen, the index will throw an error).


4. You are encouraged to tack on additional sorting/filtering with your own code. Spot is intended to be used to quickly get a list of initial candidates, but its implementation necessarily imposes restrictions on the types of queries you can make.  If 64-bit hash collisions concern you, you should verify the correctness of Spot's results yourself and remove any inaccurate documents.

5. Insertion is technically O(n), though in practice it is very fast.  In particular it is O(1) in disk reads/writes but will need to copy N/32500 pointers in RAM.

6. Queries are technically O(n) but, again, in practice are very fast, decreasing the number of elements you need to scan by a factor of 4096 and only forcing you to fetch documents (typically a slow operation) that are virtually guaranteed to be correct.

## Future Goals:

- Add token-negation (i.e. "all documents that do *not* contain this node").

- Add an "AtLeast" node (i.e. "at least 2 of 'foo bar baz qux'")

- Improve caching.  Caching pages in RAM isn't attrocious right now, but it is *far* sort of optimal.



