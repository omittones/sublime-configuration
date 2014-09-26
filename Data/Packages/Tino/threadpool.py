import threading
from Queue import Queue

class Worker(threading.Thread):
	def __init__(self, identification, pool):
		threading.Thread.__init__(self)
		self.identification = identification
		self.setDaemon(True)
		self.pool = pool

	def run(self):
		while True:
			command, success, error, callable, args, kwds = self.pool.inQueue.get()
			if command == 'stop':
				print('pool Worker ' + self.identification + ' stopped...')
				break
			try:
				if command != 'process':
					raise ValueError, 'Unknown command %r' % command
				ret = callable(*args, **kwds)
				if success!=None:
					success(ret)
			except Exception as e:
				if error!=None:
					error(e, str(callable), args, kwds)
			finally:
				pass

	def dismiss(self):
		command = 'stop'
		self.pool.inQueue.put((command, None, None, None, None, None))
		print('stopping pool Worker ' + self.identification + '...')

class ThreadPool():
	maxThreads = 32
	def __init__(self, numThreads, poolSize=0):

		#poolSize = 0 indicates buffer is unlimited.
		if numThreads > ThreadPool.maxThreads:
			numThreads = ThreadPool.maxThreads
		self.poolSize = poolSize
		self.inQueue  = Queue(self.poolSize)

		self.pool = []
		for i in range(numThreads):
			newThread = Worker(str(i), self)
			newThread.start()
			self.pool.append(newThread)

	def addTaskAndCallback(self, success, error, callable, *args, **kwds):
		command = 'process'
		self.inQueue.put((command, success, error, callable, args, kwds))

	def addTask(self, callable, *args, **kwds):
		command = 'process'
		self.inQueue.put((command, None, None, callable, args, kwds))

	def dismissWorkers(self):

		print('stopping ALL pool Workers...')
		self.inQueue = Queue(self.poolSize)

		# order is important: first, request all threads to stop...:
		for worker in self.pool:
			print('sending stop to ' + worker.identification)
			worker.dismiss()

		# ...then, wait for each of them to terminate:
		#for i in self.pool:
		#	self.pool[i].join()

		#clean up the pool from now-unused thread objects
		del self.pool
		self.pool = []