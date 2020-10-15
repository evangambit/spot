# Spot

Spot is a Python module for token-based retrieval.

## Retrieval

Suppose you have a million reddit comments and you want to construct an index that lets you search for comments by score, author, and text matching.

```Python
import spot

index = spot.Index.create(
	'my-index',
	# The rankings our index will support
	rankings=["score", "time_created"],
	# We can also restrict our search to ranges of times and/or scores
	ranges=["depth", "length"]
)

comments = load_comments_from_disk()

for comment in comments:
	tokens = set(comment.bodytext.lower().split(' '))
	tokens.add(f"author:{comment.author}")

	# For every comment we have
	# 1) a unique doc id
	# 2) the tokens we want to search by
	# 3a) arbitrary json data we may want
	# 3b) this json includes a key for every ranking and range our index needs
  index.add(docid=comment.id, token=tokens, jsondata={
  	# Probably useful stuff to have, but irrelevant to the index.
  	"permalink": comment.permalink,
  	"bodytext": comment.bodytext,

  	# Required (because of our "Index.create()" call above):
  	"score": comment.score,
  	"time_created": float(comment["created_utc"]),
  	"length": len(comment.bodytext),
  	"depth": len(comment.distance_to_root()),
  })

index.commit()
```

Now we can load the index from disk and query it.

```Python
import spot

index = spot.Index('my-index')

# Query for verses that contain the word "dog", sorted by score.
# NOTE: sorting is low-to-high by default.  Use "-" to sort from high-to-low.
for score, docid in index.token_iterator('dog', ranking="-score"):
	jsondata = index.json_from_docid(docid)
	print(jsondata["permalink"])
  print(jsondata["bodytext"])
  print('----' * 20)
```

Because these results are iterator-based, they can be efficiently combined to build up complex queries:

```Python
import spot

index = spot.Index('my-index')

iterator = spot.intersect(
  index.token_iterator("hello", ranking="-score"),
  spot.union(
    index.documents_with_token("world", ranking="-score"),
    index.documents_with_token("world!", ranking="-score"),
  )
)

for i in range(20):
	score, docid = next(iterator)
	jsondata = index.json_from_docid(docid)
	print(jsondata["permalink"])
```
