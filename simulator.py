from numpy.random import RandomState
from collections import deque
from functools import partial
import heapq
from enum import Enum


class EventQueue:

	def __init__(self):
		self.events = []
		self.i = 0
		self.t = 0.0

	def schedule(self, delta_t, callback):
		heapq.heappush(self.events, (self.t + delta_t, self.i, callback))
		self.i += 1

	def advance(self):
		t, i, callback = heapq.heappop(self.events)
		self.t = t
		return callback



q = EventQueue()
random_seed = 1

def now():
	return q.t

def t_str(t):
	seconds = int(t)
	millis = int(t*1000) % 1000
	micros = int(t * 1000000) % 1000
	return "%d.%03d,%03d" % (seconds, millis, micros)

# def print(s):
# 	__builtins__.print("%s - %s" % (t_str(now()).rjust(11), s))

def logEvent(event_type, message):
	print("%s %s" % (event_type.ljust(15), message))


class ClosedLoopWorkload:

	def __init__(self, workload_id, user_id, model_id, request_generator, concurrency):
		self.workload_id = workload_id
		self.user_id = user_id
		self.model_id = model_id
		self.request_generator = request_generator
		self.random = RandomState(random_seed + workload_id)
		for i in range(concurrency):
			self.start_next_request()

	def start_next_request(self):
		resource_requirements = self.request_generator.make_request(self.random)
		request = Request(self, resource_requirements)
		#print 'Your name is '
		#print("%s" % (request))
		request.begin()

	def request_completed(self, request):
		self.start_next_request()

	def __str__(self):
		return "Workload %s" % str(workload_id)



class Request:

	request_id_seed = 0

	def __init__(self, workload, resource_requirements):
		self.request_id = Request.request_id_seed
		Request.request_id_seed += 1

		self.workload = workload
		self.pending_stages = [Stage(self, r, q, i) for (i, (r, q)) in enumerate(resource_requirements)]
		self.completed_stages = []

		self.arrival = None
		self.completion = None

	def begin(self):
		self.arrival = now()
		logEvent("Arrive", "User %d, %s: %s" % (self.workload.user_id, self, self.verbose_description()))
		self.execute_next_stage()

	def execute_next_stage(self):
		self.pending_stages[0].execute()

	def stage_completed(self, stage):
		self.completed_stages.append(self.pending_stages[0])
		self.pending_stages = self.pending_stages[1:]
		if len(self.pending_stages) == 0:
			self.complete()
		else:
			self.execute_next_stage()

	def complete(self):
		self.completion = now()		
		lcy = self.completion - self.arrival
		logEvent("Finish", "User %d, %s.  E2ELatency = %s" % (self.workload.user_id, self, t_str(lcy)))
		self.workload.request_completed(self)

	def verbose_description(self):
		return "[%s]" % " > ".join([s.verbose_description() for s in self.pending_stages + self.completed_stages])

	def __str__(self):
		return "Request %d" % self.request_id


		


class Stage:

	def __init__(self, request, resource, quantity, stage_ix):
		self.request = request
		self.resource = resource
		self.quantity = quantity
		self.stage_ix = stage_ix

		self.enqueue = None
		self.dequeue = None
		self.complete = None

	def execute(self):
		self.resource.enqueue(self)

	def on_complete(self):
		self.request.stage_completed(self)

	def verbose_description(self):
		return "%s(%d)" % (self.resource.name, self.quantity)

	def __str__(self):
		return "%s, Task %d" % (self.request, self.stage_ix)




class RequestGenerator:

	def __init__(self):
		self.stages = []

	def _add_stage(self, resource, request_size_generator):
		self.stages.append((resource, request_size_generator))

	def exactly(self, resource, amount):
		self._add_stage(resource, lambda r: amount)
		return self

	def binomial(self, resource, mean, stdev):
		self._add_stage(resource, lambda r: r.normal(mean, stdev))
		return self

	def make_request(self, r):
		return [(resource, request_size_generator(r)) for resource, request_size_generator in self.stages]





class Resource:

	def __init__(self, name, capacity):
		self.name = name
		self.capacity = capacity
		self.pending_executions = deque()
		self.idle = True

	def enqueue(self, execution):
		execution.enqueue = now()
		logEvent(self.name.upper(), "Enqueue   %s: %s" % (execution, execution.verbose_description()))
		self.pending_executions.append(execution)
		if self.idle:
			self.idle = False
			q.schedule(0, self.execute_next)

	def execute_next(self):
		next_execution = self.pending_executions.popleft()
		next_execution.dequeue = now()
		qtime = next_execution.dequeue - next_execution.enqueue
		#print("%s" % (qtime))
		logEvent(self.name.upper(), "Dequeue %s: %s.  Queued for %s" % (next_execution, next_execution.verbose_description(), t_str(qtime)))
		execution_duration = next_execution.quantity / self.capacity
		q.schedule(execution_duration, partial(self.on_execution_completed, next_execution))

	def on_execution_completed(self, execution):
		execution.completion = now()
		etime = execution.completion - execution.dequeue
		logEvent(self.name.upper(), "Completed %s: %s.  Executed for %s" % (execution, execution.verbose_description(), t_str(etime)))
		execution.on_complete()
		if len(self.pending_executions) == 0:
			self.idle = True
		else:
			q.schedule(0, self.execute_next)

	def __str__(self):
		return self.name



cpu = Resource("cpu", capacity = 1024 * 1024 * 1024 * 3)
pcie = Resource("pcie", capacity = 12 * 1024 * 1024 * 1024)
gpu = Resource("gpu", capacity = 1024 * 1024 * 1024 * 1024)


workload1generator = RequestGenerator().binomial(cpu, 100000, 10000).binomial(pcie, 128 * 1024 * 1024, 32 * 1024 * 1024).binomial(gpu, 1024 * 1024 * 1024, 128 * 1024 * 1024)
#workload1 = ClosedLoopWorkload(workload_id=1, user_id=1, model_id=1, request_generator=workload1generator, concurrency=3)
workload2 = ClosedLoopWorkload(workload_id=5, user_id=3, model_id=7, request_generator=workload1generator, concurrency=3)

for i in range(20):
	q.advance()()