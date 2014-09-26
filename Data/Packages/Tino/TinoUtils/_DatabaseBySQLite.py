import sqlite3
import string
import threading
from _Loggers import FakeLogView


class DatabaseConnection():
	def __init__(self, path, version):
		self._path = path
		self._version = version
		self._lock = threading.RLock()
		self._connections = dict()
		self._cursors = dict()
		self._refCount = dict()

	def get_thread_id(self):
		idT = threading.current_thread().ident
		assert idT != None, 'Thread id must not be None!!!'
		return idT

	def acquire(self):
		self._lock.acquire()
		idT = self.get_thread_id()
		if not idT in self._connections:
			self._connections[idT] = sqlite3.connect(self._path + '/database.' + self._version + '.sqlite')
			self._cursors[idT] = self._connections[idT].cursor()
			self._refCount[idT] = 1
			#print(idT, 'ACK:', self._connections.keys())
		else:
			self._refCount[idT] += 1

	def curs(self):
		idT = self.get_thread_id()
		return self._cursors[idT]

	def commit(self):
		idT = self.get_thread_id()
		self._connections[idT].commit()

	def release(self):
		idT = self.get_thread_id()
		if self._refCount[idT] == 1:
			self._connections[idT].close()
			del self._connections[idT]
			del self._cursors[idT]
			del self._refCount[idT]
		elif self._refCount[idT] > 1:
			self._refCount[idT] -= 1
		else:
			raise Exception('Connection reference count not working!')

		#print(idT, 'REL:', self._connections.keys())
		self._lock.release()

	def close(self):
		pass





class DatabaseBySQLite():
	version = '0.0.4'

	def __init__(self, pathRoot, logger=None):

		self._connection = DatabaseConnection(pathRoot, self.version)

		if logger == None:
			self._logger = FakeLogView('Database Log')
		else:
			self._logger = logger

		self._connection.acquire()
		try:

			self._connection.curs().execute('''CREATE TABLE IF NOT EXISTS "files"
				("id" INTEGER PRIMARY KEY NOT NULL , "path" TEXT NOT NULL, "timestamp" INTEGER NOT NULL)''')
			self._connection.curs().execute('''CREATE  TABLE  IF NOT EXISTS "words"
				("id" INTEGER PRIMARY KEY NOT NULL , "word" TEXT)''')
			self._connection.curs().execute('''CREATE  TABLE  IF NOT EXISTS "positions"
				("id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
				 	"id_file" INTEGER NOT NULL,
					"id_word" INTEGER NOT NULL,
					"position" INTEGER NOT NULL,
					FOREIGN KEY(id_file) REFERENCES files(id),
					FOREIGN KEY(id_word) REFERENCES words(id))''')
			self._connection.commit()

			self._logger.write('loaded sqlite cache...')
			self._logger.write(str(self.count_of_files(False)) + ' files...')
			self._logger.write(str(self.count_of_words(False)) + ' words...')

		finally:
			self._connection.release()

	def file_position_get_for_word(self, word):
		self._connection.acquire()
		try:

			results = dict()
			self._connection.curs().execute('''SELECT path, position
				FROM positions
				INNER JOIN words ON positions.id_word = words.id
				INNER JOIN files ON positions.id_file = files.id
				WHERE words.word = ?
				ORDER BY path ASC''', (word,))
			for path, position in self._connection.curs().fetchall():
				if path in results:
					results[path].update([position])
				else:
					results[path] = set([position])

			return results.items()

		finally:
			self._connection.release()

	def words_get_all(self):
		self._connection.acquire()
		try:
			self._connection.curs().execute('''SELECT word FROM words ORDER BY id ASC''')
			return map(lambda result: result[0], self._connection.curs().fetchall())
		finally:
			self._connection.release()

	def files_get_all(self):
		self._connection.acquire()
		try:
			self._connection.curs().execute('''SELECT path, timestamp FROM files ORDER BY id ASC''')
			return self._connection.curs().fetchall()
		finally:
			self._connection.release()

	def count_of_words(self, cached=True):
		if cached:
			return self._cached_count_of_words
		else:
			self._connection.acquire()
			try:
				self._connection.curs().execute("SELECT COUNT(id) FROM words")
				self._cached_count_of_words = self._connection.curs().fetchone()[0]
				return self._cached_count_of_words
			finally:
				self._connection.release()

	def count_of_files(self, cached=True):
		if cached:
			return self._cached_count_of_files
		else:
			self._connection.acquire()
			try:
				self._connection.curs().execute("SELECT COUNT(id) FROM files")
				self._cached_count_of_files = self._connection.curs().fetchone()[0]
				return self._cached_count_of_files
			finally:
				self._connection.release()

	def add_new_file(self, path):
		self._connection.acquire()
		try:

			self._connection.curs().execute('''SELECT COUNT(id) FROM files
				WHERE path = ?''', (path,))
			count = self._connection.curs().fetchone()[0]

			if count == 1:
				return False
			elif count == 0:
				count = self.count_of_files()
				self._connection.curs().execute('''INSERT INTO files
					VALUES(?, ?, 0)''', (count, path))
				self._connection.commit()
				self._cached_count_of_files += 1
				return True
			else:
				raise Exception('Found multiple same files!!!')
		finally:
			self._connection.release()


	def update_words_in_file(self, words, path, newModifiedTimestamp):
		self._connection.acquire()
		try:

			#find file id, or add it, if it doesn't exit
			self._connection.curs().execute('''SELECT id FROM files WHERE path = ?''', (path, ))
			id_file = self._connection.curs().fetchone()
			if id_file == None:
				self.add_new_file(path)
				self._connection.curs().execute('''SELECT MAX(id) FROM files''')
				id_file = self._connection.curs().fetchone()[0]
			else:
				id_file = id_file[0]


			#delete old references
			self._connection.curs().execute('''DELETE FROM positions
				WHERE id_file = ?''', (id_file,))

			params = []

			#update with new words in file
			for pos, word in words:
				self._connection.curs().execute('''SELECT id FROM words WHERE word = ?''', (word, ))
				id_word = self._connection.curs().fetchone()
				if id_word == None:
					id_word = self.count_of_words()
					self._connection.curs().execute('''INSERT INTO words VALUES(?, ?)''', (id_word, word))
					self._cached_count_of_words += 1
				else:
					id_word = id_word[0]

				params.append((id_file, id_word, pos))
			self._connection.curs().executemany('''INSERT INTO positions(id_file, id_word, position) VALUES(?, ?, ?)''', params)
			self._connection.curs().execute('''UPDATE files SET timestamp = ? WHERE id = ?''', (newModifiedTimestamp, id_file))

			self._connection.commit()

		finally:
			self._connection.release()

	def clear(self):
		self._connection.acquire()
		try:

			self._connection.curs().executescript(
				'''DELETE FROM positions;
				DELETE FROM files;
				DELETE FROM words;''')
			self._connection.commit()
			assert self.count_of_files(False) == 0, 'Files could not be deleted!'
			assert self.count_of_words(False) == 0, 'Words could not be deleted!'

		finally:
			self._connection.release()

	def flush(self):
		pass

	def close(self):
		self._connection.close()