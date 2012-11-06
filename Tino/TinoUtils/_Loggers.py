import sublime
import string

class FakeLogView():
	def __init__(self, logName):
		pass
	def setSticky(self, boolean):
		pass
	def write(self, textOrLines):
		pass

class QueuedLogView():
	_inner = None
	_queue = []

	def setTimeout(self, callback, timeout):
	 	sublime.set_timeout(callback, timeout)

	def __init__(self, logName, innerType):
		self._inner = innerType(logName)

	def setSticky(self, boolean):
		self._queue.append(('setSticky', boolean))
		self.setTimeout(self.processQueue, 0)

	def write(self, textOrLines):
		self._queue.append(('write', textOrLines))
		self.setTimeout(self.processQueue, 0)

	def processQueue(self):
		if len(self._queue) != 0:
			msg, arg = self._queue.pop(0)
			if msg=='setSticky':
				self._inner.setSticky(arg)
			elif msg=='write':
				self._inner.write(arg)
			self.setTimeout(self.processQueue, 0)

#HOWTO - link to files in view or output panel
#self.view.settings().set("result_file_regex", "^(.+):([0-9]+),([0-9]+)")

#HOWTO - show output panel
#window = sublime.active_window()
#self._output = window.get_output_panel(logName)
#window.run_command("show_panel", {"panel": "output." + logName})

class SublimeLogView():
	_sticky = None

	def __init__(self, logName):
		self._output = sublime.active_window().new_file()
		self._output.set_name(logName)
		self._output.set_scratch(True)
		self._output.set_read_only(True)

	def setSticky(self, boolean):
		if self._output == None:
			return
		if boolean and self._sticky == None:
			self._sticky = self._output.size()
		elif not boolean:
			self._sticky = None

	def write(self, textOrLines):
		if not isinstance(textOrLines, basestring):
			textOrLines = string.join(textOrLines, '\n')
		else:
			textOrLines = textOrLines + '\n'

		self._output.set_read_only(False)
		edit = self._output.begin_edit()

		lastIndex = self._output.size()
		if self._sticky == None:
			self._output.insert(edit, lastIndex, textOrLines)
		else:
			reg = sublime.Region(self._sticky, lastIndex)
			self._output.replace(edit, reg, '\n' + textOrLines + '\n')
		self._output.end_edit(edit)
		self._output.set_read_only(True)