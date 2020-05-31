import json

__all__ = [
	'Expression',
	'ListINode',
	'Or',
	'And',
]

class Expression:
	def __init__(self):
		# Before 'next' is called, this should be kFirstVal
		self.currentValue = Expression.kFirstVal

	# Fetch the next value that satisfies the expression.
	def step(self):
		raise NotImplementedError()

	def encode(self):
		raise NotImplementedError()

	def retrieve(self, max_results=0xffffffffffffffff):
		r = []
		self.step()
		while self.currentValue != Expression.kLastVal:
			r.append(self.currentValue)
			if len(r) >= max_results:
				break
			self.step()
		if self.currentValue == Expression.kLastVal:
			return r
		return r

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
		self.children = tuple(children)

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

class AndWithNegations(Expression):
	def __init__(self, *children, negatation):
		super().__init__()
		assert sum([bool(x) for x in negatation]) < len(children), 'There must be at least one non-negated child!'
		assert negatation is not None
		assert len(children) == len(negatation)
		self.children = tuple(zip(children, [bool(x) for x in negatation]))

	def _highest_child_value(self):
		return max([c[0].currentValue for c in self.children if not c[1]])

	def step(self):
		if self.currentValue == Expression.kLastVal:
			return self.currentValue

		# All (non-negated) children currently point to the value
		# we just emitted (self.currentValue), so we increment
		# all of them.
		low = min([c[0].currentValue for c in self.children])
		for child, negated in self.children:
			if child.currentValue == low:
				child.step()

		# Now we keep incrementing children until
		# 1. all non-negated children have equal values
		# 2. all negated children are above that value
		while True:
			high = self._highest_child_value()
			for child, negated in self.children:
				while child.currentValue < high:
					child.step()
			high = self._highest_child_value()
			if sum([(c[0].currentValue == high) != c[1] for c in self.children]) == len(self.children):
				self.currentValue = high
				return self.currentValue

			# It's possible for all children (including
			# negated ones!) to be equal.  In this case we
			# want to increment all children.
			if sum([c[0].currentValue == high for c in self.children]) == len(self.children):
				for child, _ in self.children:
					child.step()

			high = self._highest_child_value()
			if high == Expression.kLastVal:
				self.currentValue = Expression.kLastVal
				return self.currentValue

	def encode(self):
		return json.dumps({
			'children': [c[0].encode() for c in self.children],
			'negatation': [c[1] for c in self.children],
			'type': 'AndWithNegations'
		})

	@staticmethod
	def decode(J):
		return AndWithNegations(
			*[Expression.decode(c) for c in J['children']],
			negatation=J['negatation']
		)
Expression.register('AndWithNegations', AndWithNegations)

class And(Expression):
	def __init__(self, *children):
		super().__init__()
		self.children = tuple(children)

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