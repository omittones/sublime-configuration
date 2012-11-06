import sys

DISABLED = False
DEBUG = True

if DEBUG:
	pass

import sublime
import sublime_plugin
import string
import os
import codecs
import time
import re
import threading
import threadpool
import TinoUtils


def setTimeout(callback, timeout):
	global DISABLED
	if not DISABLED:
		sublime.set_timeout(callback, timeout)

class FileLoader():

	def __init__(self, db, pool, folders, logger=None):
		self.binary_ext = set(['.zip', '.png', '.jpeg', '.rar', '.bmp', '.jpg', '.7z', '.doc', '.xls', '.pdf', '.ppt', '.dat', '.cab', '.swf', '.exe', '.chm', '.gif', '.iso', '.mp3', '.msi', '.wav', '.rmvb', '.wmv', '.rm'])
		self.binary_ext.update(['.pyd', '.pyc', '.pyo', '.exe', '.dll', '.obj','.o', '.a', '.lib', '.so', '.dylib', '.ncb', '.sdf', '.suo', '.pdb', '.idb', '.class', '.psd', '.db', '.jpg', '.jpeg', '.png', '.gif', '.ttf', '.tga', '.dds', '.ico', '.eot', '.pdf', '.swf', '.jar', '.zip'])

		self.db = db
		self.pool = pool
		self.folders = folders
		self.countNewFiles = 0
		if logger == None:
			self._logger = TinoUtils.FakeLogView('File Loader')
		else:
			self._logger = logger

	def excludeFilter(self, path):
		name, ext = os.path.splitext(path)
		if ext in self.binary_ext:
			return True
		else:
			exclude = ext == '.cPickle' or ext == '.cix'
			exclude |= (path.find('.svn') >= 0)
			exclude |= (path.find('.git') >= 0)
			exclude |= (path.find('.hg') >= 0)
			exclude |= (path.find('_svn') >= 0)
			exclude |= (path.find('_git') >= 0)
			exclude |= (path.find('_hg') >= 0)
			exclude |= (path.find('.DS_Store') >= 0)
			exclude |= not isText(path)
		return exclude

	def add(self, path):
		self.db.add_new_file(path)

	def execute(self):
		files = []
		for folder in self.folders:
			for dirname, dirnames, filenames in os.walk(folder):
				for filename in filenames:
					path = os.path.join(dirname, filename)
					if not self.excludeFilter(path):
						self.pool.addTaskAndCallback(None, reportError, self.add, path)



class FileProcessor():

	def __init__(self, db, pool, logger=None):
		self.db = db
		self.pool = pool
		self.processed_words = 0
		if logger == None:
			self._logger = TinoUtils.FakeLogView('File Processor')
		else:
			self._logger = logger
		self._regexWord = re.compile(r'\w+', re.UNICODE)

	def execute(self):
		self.pool.addTaskAndCallback(None, reportError, self.process_file, 0)

	def process_file(self, indexOfFile):
		#self._logger.write('process_file:' + str(indexOfFile))

		allfiles = self.db.files_get_all()

		if indexOfFile <= len(allfiles):
			path, timeModifiedOld = allfiles[indexOfFile]
			timeModifiedNew, words = self.extract_words(path, timeModifiedOld)
			if timeModifiedOld != timeModifiedNew:
				oldc = self.db.count_of_words()
				self.db.update_words_in_file(words, path, timeModifiedNew)
				self.processed_words += self.db.count_of_words() - oldc

			if self.processed_words > 1000:
				self.pool.addTaskAndCallback(None, reportError, self.db.flush)
				self.processed_words = 0

			#next file
			indexOfFile += 1
			if indexOfFile > len(allfiles):
				indexOfFile = 0

		#schedule next file task
		self.pool.addTaskAndCallback(None, reportError, self.process_file, indexOfFile)

	def extract_words(self, path, lastModifiedTime):
		try:
			words = []
			timeModifiedNew = int(os.path.getmtime(path))
			if lastModifiedTime == timeModifiedNew:
				return (timeModifiedNew, words)
			with codecs.open(path, encoding='utf-8', errors='replace', mode='r') as textFile:
				pos = 0
				while True:
					lines = textFile.readlines(100000)
					if not lines:
						break
					for line in lines:
						for i in self._regexWord.finditer(line):
							if i.start() < i.end():
								temp = line[i.start():i.end()]
								if len(temp) < 64:
									words.append((i.start() + pos, temp))
						pos += len(line)
			return (timeModifiedNew, words)
		except Exception as e:
			print('ERROR : ', path)
			raise


class SuperWordCommand(sublime_plugin.WindowCommand):
	words = []
	def run(self):
		global _db
		self.words = map(lambda w: w.encode('ascii', 'replace'), _db.words_get_all())
		self.window.show_quick_panel(self.words, self.on_done)

	def on_done(self, index):
		global _db
		filepositions = _db.file_position_get_for_word(self.words[index])
		results = []
		for x, positions in filepositions:
			path = _db.file_get(x)[0]
			results.append((path, -len(positions)))
		results = sorted(results, key=lambda result: result[1])
		view = sublime.active_window().open_file(results[0][0])



class SuperJumpCommand(sublime_plugin.WindowCommand):

	def run(self):
		self.window.show_input_panel("Find word across all files:", "", self.on_done, None, None)
		pass

	def on_done(self, text):
		global _db

		output = TinoUtils.SublimeLogView('Tino Find Results')
		output._output.settings().set("result_file_regex", "^FILE:::(.+)")

		sets = dict()
		reg = re.compile(r'\w+', re.UNICODE)
		for i in reg.finditer(text):
			if i.start() < i.end():
				match = text[i.start():i.end()]
				for word in _db.words_get_all():
					if word.lower().startswith(match.lower()) and len(word) < (len(match) * 1.25):
						output.write(' - ' + word)
						files = _db.file_position_get_for_word(word)
						for path, positions in files:
							if not path in sets:
								sets[path] = []
							for pos in positions:
								sets[path].append((word, pos))
		output.write('')

		sets = sorted(sets.iteritems(), key=lambda item: len(item[1]), reverse=True)
		for path, positions in sets:
			output.write('FILE:::' + path)
			output.write(' ' + str(len(positions)) + '  ' + str(positions))
			output.write('\n')



def isText(filename):
	"""Test if file is text by fuzzy looking at first 512 bytes."""

	# init variables
	s = open(filename).read(512)

	# empty files are considered text
	if not s:
		return True

	# Get the non-text characters (maps a character to itself then
	# use the 'remove' option to get rid of the text characters.)
	t = string.translate(s, None, string.printable)

	# if more than 10% non-text characters, then
	# this is considered a binary file
	return (float(len(t))/float(len(s)) < 0.10)



def show_progress(db, pool, logger):
	screen = ['', str(time.clock()), 'todo:  ' + str(pool.inQueue.qsize()), 'files: ' + str(db.count_of_files()), 'words: ' + str(db.count_of_words()), '', '']
	logger.write(screen)

def queueShowProgress():
	g = globals()
	if '_db' in g and '_pool' in g and '_logScreen' in g:
		global _db, _pool, _logScreen
		show_progress(_db, _pool, _logScreen)
	setTimeout(queueShowProgress, 500)

def reportError(e, name, args, kwds):
	global _logScreen
	_logScreen.write(['', '**** ERROR ****', name + str(args) + str(kwds), str(e), ''])

def init_db(path, logger, folders):
	global DISABLED, _pool, _loader, _processor, _db

	_db = TinoUtils.DatabaseBySQLite(path, logger)
	_loader = FileLoader(_db, _pool, folders, logger)
	_processor = FileProcessor(_db, _pool, logger)

	if not DISABLED:
		setTimeout(queueShowProgress, 0)
		_pool.addTaskAndCallback(None, reportError, _loader.execute)
		_pool.addTaskAndCallback(None, reportError, _processor.execute)


if '_pool' in globals():
	_pool.dismissWorkers()
	del _pool

if '_db' in globals():
	_db.close()
	del _db

#init module
name = 'Tino Log @ ' + str(int(time.clock())) + 's'
if DEBUG:
	_logScreen = TinoUtils.QueuedLogView(name, TinoUtils.SublimeLogView)
else:
	_logScreen = TinoUtils.FakeLogView(name)

_logScreen.write('super_jump.py loaded!')

_pathRoot = os.path.dirname(os.path.abspath(__file__))
_folders = sublime.active_window().folders()

_pool = threadpool.ThreadPool(2)
_pool.addTaskAndCallback(None, reportError, init_db, _pathRoot, _logScreen, _folders)