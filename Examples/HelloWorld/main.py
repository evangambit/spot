import json

from spot import Index, union

index = Index.create(':memory:', rankings=["score"], ranges=["date_created"])
index.insert(0, tokens=["foo", "bar"], jsondata={
  "score": 10,
  "date_created": 10,
  "document_text": "foo bar"
})
index.insert(1, tokens=["foo", "baz"], jsondata={
  "score": 3,
  "date_created": 11,
  "document_text": "foo baz"
})
index.insert(2, tokens=["bar", "baz"], jsondata={
  "score": 7,
  "date_created": 12,
  "document_text": "bar baz"
})

print('===' * 20)
print('All documents with the word "foo" (from low to high score)')
for score, docid in index.token_iterator("foo", ranking="-score"):
	print(f"document {docid} (score = {score})")
print('===' * 20)
print('docid 1:')
print(json.dumps(index.json_from_docid(1), indent=2))
print('===' * 20)

for _, docid in index.not_token_iterator("foo", ranking="score"):
  print(docid)