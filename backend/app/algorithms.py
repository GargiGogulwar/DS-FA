from dataclasses import dataclass, field


@dataclass
class Lamport:
    value: int = 0

    def tick(self, received=0):
        self.value = max(self.value, received) + 1
        return self.value


@dataclass
class Vector:
    values: dict = field(default_factory=dict)

    def tick(self, node, received=None):
        for key, value in (received or {}).items():
            self.values[key] = max(self.values.get(key, 0), value)
        self.values[node] = self.values.get(node, 0) + 1
        return dict(self.values)

    def relation(self, other):
        keys = self.values.keys() | other.keys()
        less = any(self.values.get(k, 0) < other.get(k, 0) for k in keys)
        more = any(self.values.get(k, 0) > other.get(k, 0) for k in keys)
        return 'concurrent' if less and more else 'before' if less else 'after' if more else 'equal'


def cycle(graph):
    visited, stack = set(), []

    def visit(node):
        if node in stack:
            return stack[stack.index(node):] + [node]
        if node in visited:
            return None
        stack.append(node)
        for neighbor in graph.get(node, []):
            found = visit(neighbor)
            if found:
                return found
        stack.pop()
        visited.add(node)
        return None

    for node in graph:
        found = visit(node)
        if found:
            return found
    return []


class Snapshot:
    """Educational simulator: reliable directed FIFO channels, no process failures."""
    def __init__(self, nodes):
        self.nodes = nodes
        self.states = {}
        self.channels = {}
        self.closed = set()

    def marker(self, source, target, state):
        first = target not in self.states
        if first:
            self.states[target] = state
        self.closed.add((source, target))
        return first

    def message(self, source, target, value):
        if target in self.states and (source, target) not in self.closed:
            self.channels.setdefault(f'{source}->{target}', []).append(value)

    @property
    def complete(self):
        return len(self.states) == len(self.nodes) and len(self.closed) == len(self.nodes) * (len(self.nodes) - 1)
