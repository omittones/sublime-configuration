import cPickle
import string
import threading
from _Loggers import FakeLogView

class Database():
	version = '0.0.3'

	def __init__(self, pathRoot, logger=None):

		self._pathRoot = ''
		self._lock = None
		self._files = []
		self._words = dict()
		self._dirty = False

		if logger == None:
			self._logger = FakeLogView('Database Log')
		else:
			self._logger = logger

		self._pathRoot = pathRoot
		self._lock = threading.RLock()
		self._lock.acquire()

		#init databases
		try:
			with open(self._pathRoot + '/database_words' + self.version + '.cPickle', 'rb') as cache:
				self._words = cPickle.load(cache)
				self._logger.write('loaded word cache...' + str(len(self._words)) + ' words.')
		except Exception as e:
			self._words = None
			self._logger.write(str(e))
		try:
			with open(self._pathRoot + '/database_files' + self.version + '.cPickle', 'rb') as cache:
				self._files = cPickle.load(cache)
				self._logger.write('loaded file cache...' + str(len(self._files)) + ' files.')
		except Exception as e:
			self._files = None
			self._logger.write(str(e))

		if self._words == None or self._files == None:
			self._words = dict()
			self._logger.write('created word cache!')
			self._files = dict()
			self._logger.write('created file cache!')
			self._dirty = True

		self._lock.release()

	def file_position_get_for_word(self, word):
		self._lock.acquire()
		try:
			return self._words[word].items()
		finally:
			self._lock.release()

	def words_get_all(self):
		self._lock.acquire()
		try:
			return self._words.keys()
		finally:
			self._lock.release()

	def file_get(self, index):
		self._lock.acquire()
		try:
			return self._files[str(index)]
		finally:
			self._lock.release()

	def count_of_words(self):
		self._lock.acquire()
		try:
			return len(self._words)
		finally:
			self._lock.release()

	def count_of_files(self):
		self._lock.acquire()
		try:
			return len(self._files)
		finally:
			self._lock.release()

	def add_new_file(self, path):
		self._lock.acquire()
		try:

			skip = False
			for key, value in self._files.items():
				if value[0] == path:
					skip = True
					break
			if not skip:
				x = str(len(self._files))
				self._files[x] = (path, 0)
				self._dirty = True

			return not skip

		finally:
			self._lock.release()


	def update_words_in_file(self, words, indexOfFile, newModifiedTimestamp):

		self._lock.acquire()
		try:

			#delete old references
			for key in self._words:
				self._words[key].pop(indexOfFile, None)

			#update with new words in file
			for pos, word in words:
				if not word in self._words:
					self._words[word] = { indexOfFile : set([pos]) }
				elif not indexOfFile in self._words[word]:
					self._words[word][indexOfFile] = set([pos])
				else:
					self._words[word][indexOfFile].add(pos)

			x = str(indexOfFile)
			path, timestamp = self._files[x]
			self._files[x] = (path, newModifiedTimestamp)

		finally:
			self._lock.release()

		self._dirty = True

	def clear(self):
		self._lock.acquire()
		try:

			self._words = dict()
			self._files = dict()

		finally:
			self._lock.release()

	def flush(self):
		if not self._dirty:
			self._logger.write('Db not dirty...')
			return
		else:
			self._logger.write('Saving db...')

		self._lock.acquire()
		try:
			with open(self._pathRoot + '/database_files' + self.version + '.cPickle', 'wb') as cache:
				cPickle.dump(self._files, cache)
				self._logger.write(str(len(self._files)) + ' files pickled...')
			with open(self._pathRoot + '/database_words' + self.version + '.cPickle', 'wb') as cache:
				cPickle.dump(self._words, cache)
				self._logger.write(str(len(self._words)) + ' words pickled...')
			self._dirty = False
		except Exception as e:
			self._logger.write(str(e))
		finally:
		    self._lock.release()

	def close(self):
	   	self.flush()