from numpy.random import RandomState
from collections import deque
from functools import partial
import heapq
from enum import Enum
import random
from random import randint
import array as arr
import numpy as geek 

# Implementation of event quque
class EventQueue:

	def __init__(self):
		self.events = []                                                                  # Events associated with each operation for different jobs
		self.i = 0
		self.t = 0.0

	def schedule(self, delta_t, priority, callback):
		delta_t += random.randint(1,8)*0.02                                                # Generate the time required to finish to any operation (i.e., CPU and GPU execution, network transfer and PCIE transfer)
		heapq.heappush(self.events, (self.t + delta_t, priority, self.i, callback))        # Heap Quque
		self.i += 1

	def advance(self):
		t, priority, i, callback = heapq.heappop(self.events)
		self.t = t
		return callback



q = EventQueue()
random_seed = 1

# Tracking the time stamp of each event
def now():
	return q.t

# Calculation of time stamp
def t_str(t):
	seconds = int(t)
	millis = int(t*1000) % 1000
	micros = int(t * 1000000) % 1000
	return "%d.%03d,%03d" % (seconds, millis, micros)

def print_m(s):
	__builtins__.print_m("%s - %s" % (t_str(now()).rjust(11), s))

# Print log of each event 
def logEvent(name, message):
	print("%s %s" % (name.ljust(15), message))


class Priority:
	RESOURCE_EXEC_COMPLETE = 0
	REQUEST_COMPLETE = 1
	ADMIT_NEXT = 10
	RESOURCE_EXEC_NEXT = 11

# Closed loop workload generator 
class ClosedLoopWorkload:

	def __init__(self, workload_id, worker_id, user_id, model_id, request_generator, concurrency, admission_control):
		self.workload_id = workload_id                                 # Workload ID 
		self.user_id = user_id                                         # User ID 
		self.model_id = model_id                                       # Model ID
		self.worker_id = worker_id                                     # Worker ID
		self.request_generator = request_generator
		self.random = RandomState(random_seed + workload_id)
		self.admission_control = admission_control
		for i in range(concurrency):                                   # Generate number of concurrent requets for each user
			self.start_next_request()

	def start_next_request(self):
		resource_requirements = self.request_generator.make_request(self.random)
		request = Request(self, self.admission_control, resource_requirements)
		request.begin()

	def request_completed(self, request):
		self.start_next_request()

	def __str__(self):
		return "Workload %s" % str(workload_id)


# Request generator 
class Request:

	request_id_seed = 0
	count = 0
	total_latency = 0

	def __init__(self, workload, admission_control, resource_requirements):
		self.request_id = Request.request_id_seed
		Request.request_id_seed += 1

		self.workload = workload

		self.admission_control = admission_control
		self.pending_stages = [ResourceStage(self, r, q, i) for (i, (r, q)) in enumerate(resource_requirements)]
		self.completed_stages = []

		self.arrival = None
		self.completion = None

	def begin(self):                                               # Tacking the arrival of a job
		self.arrival = now()
		logEvent("Arrive", "User %d, %s: Worker %s: Model %s: %s" % (self.workload.user_id, self, self.workload.worker_id, self.workload.model_id, self.verbose_description()))
		self.admission_control.enqueue(self)

	def admitted(self):                                            # Admitting a job
		logEvent("Admit", "User %d, %s: %s" % (self.workload.user_id, self, self.verbose_description()))
		self.execute_next_stage();

	def execute_next_stage(self):
		self.pending_stages[0].execute()

	def stage_completed(self, stage):                             # Tracking the completion a job
		self.completed_stages.append(self.pending_stages[0])
		self.pending_stages = self.pending_stages[1:]
		if len(self.pending_stages) == 0:
			self.admission_control.completed(self)
			self.complete()
		else:
			self.execute_next_stage()

	def complete(self):
		Request.count += 1
		self.completion = now()		
		lcy = self.completion - self.arrival
		logEvent("Finish", "User %d, %s.  E2ELatency = %s" % (self.workload.user_id, self, t_str(lcy)))
		logEvent("Job completed count", Request.count)
		Request.total_latency = Request.total_latency + lcy
		logEvent("Total latency", Request.total_latency)
		average_latency = Request.total_latency/Request.count
		logEvent("Average latency", average_latency)
		self.workload.request_completed(self)


	def verbose_description(self):
		return "[%s]" % " > ".join([s.verbose_description() for s in self.pending_stages + self.completed_stages])

	def __str__(self):
		return "Request %d" % self.request_id


# Admission control mechanism 
class AdmissionControl:

	def __init__(self):
		pass

	def enqueue(self, request):
		pass

	def completed(self, request):
		pass


class NoAdmissionControl(AdmissionControl):

	def __init__(self):
		pass

	def enqueue(self, request):
		request.admitted()

	def completed(self, request):
		pass

# Fixed concurrent admission control mechanism 
class FixedConcurrencyAdmissionControl(AdmissionControl):

	def __init__(self, concurrency, queue):
		self.concurrency = concurrency
		self.count = 0
		self.queue = queue

	def _schedule_admission(self):
		q.schedule(0, Priority.ADMIT_NEXT, self._admit_if_possible)

	def _admit_if_possible(self):
		while self.count < self.concurrency and not self.queue.is_empty():
			request_to_admit = self.queue.dequeue()
			self.count += 1
			request_to_admit.admitted()

	def enqueue(self, request):
		self.queue.enqueue(request)
		self._schedule_admission()

	def completed(self, request):
		self.queue.completed(request)
		self.count -= 1
		self._schedule_admission()


class Queue:

	def __init__(self):
		pass

	def is_empty(self):
		return True

	def enqueue(self, request):
		pass

	def dequeue(self):
		return None

	def completed(self, request):
		pass

# FIFO scheudling mechanism
class FIFOQueue(Queue):

	def __init__(self):
		self.pending = deque()

	def is_empty(self):
		return len(self.pending) == 0

	def enqueue(self, request):
		self.pending.append(request)

	def dequeue(self):
		return self.pending.popleft()

	def completed(self, request):
		pass


# Tacking the resource usage of each operation
class ResourceStage:

	stage_id_seed = 0

	def __init__(self, request, resource, quantity, stage_ix):
		self.stage_id = ResourceStage.stage_id_seed
		ResourceStage.stage_id_seed += 1

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


# Basic module of Dominant Fair Queuing scheme
class DominantFairQueuing:

	def __init__(self, ):
		print(" ")

	def __dove_tailing(self):
		print("Implementation of Dove Tailing")

	def __virtual_time(self):
		virtual_time = self.request.complete.now()
		virtaul_finish_time = now()
		request_id = 0
		print("self.virtual_time", self.virtual_time)
		virtual_start_time = max(self.virtual_time, self.virtaul_finish_time)
		if request_id != 0:
			virtual_time_function = max()
		else:
			virtual_time_function = 0
		start_time = max(1, 2)
		print("Implementation of Virtual Time", start_time)


# Request generator
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



class VirtualTimeConsumptionTracker:

	def __init__(self, capacity):
		self.capacity = capacity

		self.t = 0
		self.vt = 0

		self.ongoing = []

		self.iteration = 0

	def _advance_to_now(self):
		if len(self.ongoing) == 0:
			self.t = now()
			self.vt = 0
		else:
			cur_t = now()
			self.vt += (cur_t - self.t) * (self.capacity / len(self.ongoing))
			self.t = cur_t


	def _check_for_completions(self):
		while len(self.ongoing) > 0:
			finish_vt, callback = self.ongoing[0]
			if finish_vt <= self.vt:
				self.ongoing.pop(0)
				callback()
			else:
				return

	def add(self, quantity, on_complete):
		self._advance_to_now()
		finish_vt = self.vt + quantity
		self.ongoing.append((finish_vt, on_complete))
		self.ongoing.sort()
		self._schedule_next_completion()

	def _check_completions_callback(self, iteration, callback_vt):
		if self.iteration == iteration:
			self.t = now()
			self.vt = callback_vt
			self._check_for_completions()
			self._schedule_next_completion()

	def _schedule_next_completion(self):
		if len(self.ongoing) > 0:
			self.iteration += 1
			finish_vt, _callback = self.ongoing[0]
			delta_t = (finish_vt - self.vt) * (len(self.ongoing) / self.capacity)
			q.schedule(delta_t, Priority.RESOURCE_EXEC_COMPLETE, partial(self._check_completions_callback, self.iteration, finish_vt))


# Different operations for each event
class Resource:

	def __init__(self, name, capacity, queue, concurrency):
		self.name = name
		self.consumption_tracker = VirtualTimeConsumptionTracker(capacity)
		self.queue = queue
		self.concurrency = concurrency
		self.count = 0


	def _exec_next(self):                             # Dequeueing a job
		while self.count < self.concurrency and not self.queue.is_empty():
			self.count += 1
			next_execution = self.queue.dequeue()
			next_execution.dequeue = now()
			qtime = next_execution.dequeue - next_execution.enqueue
			#print("%s" % (next_execution.dequeue))
			logEvent(self.name.upper(), "Dequeue %s: %s.  Queued for %s" % (next_execution, next_execution.verbose_description(), t_str(qtime)))
			self.consumption_tracker.add(next_execution.quantity, partial(self._on_execution_completed, next_execution))


	def _schedule_exec_next(self):
		if self.count < self.concurrency:
			q.schedule(0, Priority.RESOURCE_EXEC_NEXT, self._exec_next)


	def enqueue(self, execution):                    # Enqueueing a job
		execution.enqueue = now()
		logEvent(self.name.upper(), "Enqueue   %s: %s" % (execution, execution.verbose_description()))
		self.queue.enqueue(execution)
		self._schedule_exec_next()

	def _on_execution_completed(self, execution):    # Execution of a job
		self.count -= 1
		execution.completion = now()
		etime = execution.completion - execution.dequeue
		logEvent(self.name.upper(), "Completed %s: %s.  Executed for %s" % (execution, execution.verbose_description(), t_str(etime)))
		self.queue.completed(execution)
		execution.on_complete()
		self._schedule_exec_next()

	def __str__(self):
		return self.name

# Module to generate the concurrent requests of a user
def concurrency_generator():
	#no_of_concurrent_requests = 1
	no_of_concurrent_requests = randint(0, 4)     # Total number of concurrent requests from each user
	return no_of_concurrent_requests


# Module to genrate multiple workers
class MultipleWorker:

	def __init__(self, no_of_workers, no_of_users):
		self.no_of_workers = no_of_workers
		self.no_of_users = no_of_users
		j = 0
		while j < no_of_users:
			mappingfunction = MappingFunction(no_of_workers=no_of_workers)
			modelchecking = mappingfunction._model_checking(mappingfunction, mappingfunction.worker_id, mappingfunction.requested_model_id)
			workload1 = ClosedLoopWorkload(workload_id=1, worker_id=modelchecking, user_id=j, model_id=mappingfunction.requested_model_id, request_generator=workload1generator, concurrency=concurrency_generator(), admission_control=admissioncontrol)	
			j = j+1

# Basic job placement algorithm based on the modeles available in each worker
class MappingFunction:

	def __init__(self, no_of_workers):
		self.requested_model_id = self._model_id_generator()
		#print("Requested model", self.requested_model_id)
		for i in range(no_of_workers):
			self.model_list = random.sample(range(1, 100), 30)
			self.worker_id = i
			#print("Model list and Worker ID", self.model_list, self.worker_id)
			return_value = self._model_checking(self.model_list, self.worker_id, self.requested_model_id)
			if return_value > 0:
				break

	def _model_checking(self, model_list, worker_id, requested_model_id):
		self.requested_model_id = requested_model_id
		for i in range(len(self.model_list)):
			if self.requested_model_id == self.model_list[i]:
				#print("worker id", worker_id)
				return worker_id
				break

	def _model_id_generator(self):
		self.model_id_generator = random.randrange(1, 100)
		return self.model_id_generator

# Module for cental controller
class CentralController:

	def __init__(self, no_of_workers, no_of_users):
		self.no_of_workers = no_of_workers
		self.no_of_users = no_of_users
		multipleworker = MultipleWorker(no_of_workers=no_of_workers, no_of_users =no_of_users)
		DominatFairQueuing()


cpu = Resource("cpu", capacity = 1000000, queue = FIFOQueue(), concurrency=100)           # CPU resources
pcie = Resource("pcie", capacity = 1000000, queue = FIFOQueue(), concurrency=100)         # PCIE resources
gpu = Resource("gpu", capacity = 1000000, queue = FIFOQueue(), concurrency=100)           # GPU resources
network = Resource("network", capacity = 10000, queue = FIFOQueue(), concurrency=100)     # Network resources

admissionqueue = FIFOQueue()
admissioncontrol = FixedConcurrencyAdmissionControl(1, admissionqueue)

workload1generator = RequestGenerator().binomial(cpu, 1000, 100).binomial(pcie, 4000, 1000).binomial(gpu, 1000, 200).binomial(network, 2000, 500) # Workload generator
centralcontroller = CentralController(no_of_workers=8, no_of_users=10) # Central controller with 8 workers and 10 users

total_time = 200 # Time-frame 

for i in range(total_time):
	q.advance()()