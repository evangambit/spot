import json

__all__ = [
	'Expression',
	'retrieve',
	'ListINode',
	'Or',
	'And',
]

def retrieve(expression, max_results=float('inf')):
	if type(expression) is str:
		expression = Expression.decode(expression)

	r = []
	expression.step()
	while expression.currentValue != Expression.kLastVal:
		r.append(expression.currentValue)
		if len(r) >= max_results:
			break
		expression.step()
	if expression.currentValue == Expression.kLastVal:
		return r, None
	return r, expression.encode()


class Expression:
	def __init__(self):
		# Before 'next' is called, this should be kFirstVal
		self.currentValue = Expression.kFirstVal

	# Fetch the next value that satisfies the expression.
	def step(self):
		raise NotImplementedError()

	def encode(self):
		raise NotImplementedError()

	@staticmethod
	def decode(J):
		if type(J) is str:
			J = json.loads(J)
		return Expression.registry[J['type']].decode(J)

	@staticmethod
	def register(string, cls):
		if string in Expression.registry:
			raise Exception('')
		Expression.registry[string] = cls
Expression.registry = {}

Expression.kFirstVal = ((256**7)//-2, None)
Expression.kLastVal = ((256**7)// 2 - 1, None)

class ListINode(Expression):
	def __init__(self, vals, index=-1):
		super().__init__()
		self.vals = vals
		self.idx = -1

	def step(self):
		self.idx += 1
		if self.idx < len(self.vals):
			self.currentValue = self.vals[self.idx]
		else:
			self.currentValue = Expression.kLastVal
		return self.currentValue

	def encode(self):
		return json.dumps({
			'type': 'ListINode',
			'vals': self.vals,
			'idx': self.idx
		})

	@staticmethod
	def decode(J):
		return ListINode(J['vals'], index=J['idx'])
Expression.register('ListINode', ListINode)

class Or(Expression):
	def __init__(self, *children):
		super().__init__()
		self.children = children

	def step(self):
		v = min([c.currentValue for c in self.children])
		for child in self.children:
			if child.currentValue == v:
				child.step()
		self.currentValue = min([c.currentValue for c in self.children])
		return self.currentValue

	def encode(self):
		return json.dumps({
			'children': [c.encode() for c in self.children],
			'type': 'Or'
		})

	@staticmethod
	def decode(J):
		return Or(*[Expression.decode(c) for c in J['children']])
Expression.register('Or', Or)

class And(Expression):
	def __init__(self, *children):
		super().__init__()
		self.children = children

	def step(self):
		for child in self.children:
			child.step()
		while True:
			changeMade = False
			v = max([c.currentValue for c in self.children])
			for child in self.children:
				if child.currentValue < v:
					child.step()
					changeMade = True
			if not changeMade:
				break
		self.currentValue = self.children[0].currentValue
		return self.currentValue

	def encode(self):
		return json.dumps({
			'children': [c.encode() for c in self.children],
			'type': 'And'
		})

	@staticmethod
	def decode(J):
		return And(*[Expression.decode(c) for c in J['children']])
Expression.register('And', And)