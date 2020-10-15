
kDefaultLimit = 1024

kLastResult = (float('inf'), float('inf'))

def intersect(*iters, limit=kDefaultLimit):
  return atleast(*iters, k=len(iters), limit=limit)

def union(*iters, limit=kDefaultLimit):
  return atleast(*iters, k=1, limit=limit)

def atleast(*iters, k=2, limit=kDefaultLimit):
  assert 1 <= k <= len(iters)

  num_returned = 0
  vals = []
  for it in iters:
    try:
      vals.append(next(it))
    except StopIteration:
      vals.append(kLastResult)

  while True:
    minval = min(vals)
    if minval == kLastResult:
      return
    if sum([v == minval for v in vals]) >= k:
      yield minval
      num_returned += 1
      if num_returned >= limit:
        break

    for i in range(len(vals)):
      if vals[i] == minval:
        try:
          vals[i] = next(iters[i])
        except StopIteration:
          vals[i] = kLastResult

