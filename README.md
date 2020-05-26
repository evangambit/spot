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

criteria = spot.And(
  index.documents_with_token("hello"),
  spot.Or(
    index.documents_with_token("world"),
    index.documents_with_token("world!"),
  )
)

# Get the first 20 lines that contain the word "hello" and either the word "world" or "world!"
results = index.retrieve(criteria, max_results=20)
```

The types of nodes currently supported are

- And
- Or
- AtLeast


## Additional Notes

1. Spot expects either 64-bit integer tokens or strings.  Strings are hashed to 64-bit integers, which means that it is possible (if unlikely) for hashing collisions to make results inaccurate.

2. As an implementation detail, these hashes are forced into a range of [0, 4095].  The 64-bit hashes are checked during retrieval (to ensure high accuracy) but there is a limit of 64 tokens in each of the 4096 buckets.

3. You are encouraged to tack on additional sorting/filtering with your own code. Spot is intended to be used to quickly get a list of initial candidates, but its implementation necessarily imposes restrictions on the types of queries you can make.  If complete accuracy is required, you're also recommended to verify the correctness of Spot's results (due to the 64-bit hashing limitation).

4. Insertion is O(lg(n)).  Retrieval generally runs in time proportional to the output, but in cases where a rare tokens and a common token hash to the same bucket, retrieval can be comparatively slow.

5. While insertion should always be quite fast, no thought was put into keeping ongoing queries accurate while insertions occur.  In general you should not be making insertions into a database that is currently serving requests.  This is great for personal projects (where you can regularly bring the server down, update it, and push it back up, or where it is cheap to keep duplicates of the index) but may be a non-starter otherwise.

