import sublime
import sublime_plugin
import super_jump
import unittest
import os

import TinoUtils._Database
import TinoUtils._DatabaseByShelfing
import TinoUtils._DatabaseBySQLite

reload(TinoUtils._Database)
reload(TinoUtils._DatabaseByShelfing)
reload(TinoUtils._DatabaseBySQLite)


class Tests(unittest.TestCase):

	pathRoot = os.path.dirname(os.path.abspath(__file__))

	def log(self, message):
		print('\n\n************ DEBUG ************')
		print(message)
		print('\n')

	def _test_database(self, dbtype):
		db = dbtype(os.path.join(self.pathRoot,'test'))
		db.clear()
		db.add_new_file('file1.txt')
		db.update_words_in_file([(1,'mustnotbehere')], 'file1.txt', 5)
		db.update_words_in_file([(1,'one'), (5,'two'), (10,'one')], 'file1.txt', 10)
		db.update_words_in_file([(1,'one'), (20,'one')], 'file2.txt', 15)
		db.close()

		db2 = dbtype(os.path.join(self.pathRoot, 'test'))
		self.assertEqual(db2.count_of_files(), 2)
		self.assertEqual(db2.file_get_all(), [('file1.txt', 10), ('file2.txt', 15)])
		self.assertEqual(db2.count_of_words(), 3)
		wone = db2.file_position_get_for_word('one')
		wtwo = db2.file_position_get_for_word('two')
		self.assertEqual(wone, { 'file1.txt':set([1, 10]), 'file2.txt':set([1, 20]) }.items())
		self.assertEqual(wtwo, { 'file1.txt':set([5])}.items())
		db2.close()

	def test_database_sqlite(self):
		self._test_database(TinoUtils._DatabaseBySQLite.DatabaseBySQLite)

	def _test_database_norm(self):
		self._test_database(TinoUtils._Database.Database)

	def _test_database_shelf(self):
		self._test_database(TinoUtils._DatabaseByShelfing.DatabaseByShelfing)

	def test_isText(self):
		self.assertTrue(super_jump.isText(os.path.join(self.pathRoot,'super_jump.py')))
		self.assertFalse(super_jump.isText(os.path.join(self.pathRoot,'super_jump.pyc')))
		self.assertTrue(super_jump.isText(os.path.join(self.pathRoot,'..','packages.sublime-project')))
		self.assertFalse(super_jump.isText(os.path.join(self.pathRoot, 'test.binary.png')))

	def test_processFile(self):

		obj = super_jump.FileProcessor(None, None)

		#check for proper generation
		time, words = obj.extract_words(os.path.join(self.pathRoot, 'test.list.txt'), 0)
		self.assertTrue(len(words) == 6)
		self.assertTrue(words[0] == (19, u'caption'))
		self.assertTrue(words[1] == (30, u'Tino'))
		self.assertTrue(words[2] == (36, u'Super'))
		self.assertTrue(words[3] == (42, u'Jump'))
		self.assertTrue(words[4] == (59, u'command'))
		self.assertTrue(words[5] == (70, u'super_jump'))
		#check for proper skip if hash not changed
		time, words = obj.extract_words(os.path.join(self.pathRoot,'test.list.txt'), time)
		self.assertTrue(len(words)==0)

if __name__ == '__main__':
	unittest.main()
else:
	suite = unittest.TestLoader().loadTestsFromTestCase(Tests)
	unittest.TextTestRunner(verbosity = 2).run(suite)