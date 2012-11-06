import string
import threading
import shelve
from _Loggers import FakeLogView

class DatabaseByShelfing():
	version = '0.0.3'

	def __init__(self, pathRoot, logger=None):

		self._lock = None
		self._words = None
		self._files = None
		self._pathRoot = pathRoot

		if logger == None:
			self._logger = FakeLogView('Database Log')
		else:
			self._logger = logger

		self._lock = threading.RLock()
		self._lock.acquire()
		try:
			self._files = shelve.open(self._pathRoot + '/shelve_files.' + self.version, flag='c')
			self._words = shelve.open(self._pathRoot + '/shelve_words.' + self.version, flag='c')
			self._logger.write('Shelve DB loaded file cache...' + str(len(self._files)) + ' files.')
			self._logger.write('Shelve DB loaded word cache...' + str(len(self._words)) + ' words.')
		finally:
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
			for p, m in self._files.values():
				if p == path:
					skip = True
					break
			if not skip:
				x = str(len(self._files))
				self._files[x] = (path, 0)
			return not skip

		finally:
			self._lock.release()

	def update_words_in_file(self, words, indexOfFile, newModifiedTimestamp):

		self._lock.acquire()
		try:

			#delete old references
			for key in self._words.keys():
				temp = self._words[key]
				if indexOfFile in temp:
					del temp[indexOfFile]
					self._words[key] = temp

			#update with new words in file
			for pos, word in words:
				try:
					if not word in self._words:
						temp = dict()
						temp[indexOfFile] = set([pos])
						self._words[word] = temp
					elif not indexOfFile in self._words[word]:
						temp = self._words[word]
						temp[indexOfFile] = set([pos])
						self._words[word] = temp
					else:
						temp = self._words[word]
						temp[indexOfFile].add(pos)
						self._words[word] = temp
				except Exception as e:
					print('ERROR: word=' + word)
					raise

			x = str(indexOfFile)
			path, timestamp = self._files[x]
			self._files[x] = (path, newModifiedTimestamp)

		finally:
			self._lock.release()



	def clear(self):
		self._lock.acquire()
		try:

			self._files.close()
			self._words.close()
			self._files = shelve.open(self._pathRoot + '/shelve_files.' + self.version, flag='n')
			self._words = shelve.open(self._pathRoot + '/shelve_words.' + self.version, flag='n')

		finally:
			self._lock.release()

	def flush(self):

		self._logger.write('Saving db...\n')

		self._lock.acquire()
		try:
			self._files.sync()
			self._words.sync()
		except Exception as e:
			self._logger.write(str(e))
		finally:
		    self._lock.release()

	def close(self):
		self._files.close()
		self._words.close()