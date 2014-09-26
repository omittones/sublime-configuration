import sublime
import sublime_plugin
import os

class SuperTestCommand(sublime_plugin.WindowCommand):
	def run(self, *arg, **kwarg):
		print(arg)
		print(kwarg)


# def get_closure(arg):
# 	a = arg + 1
# 	def closure():
# 		#a = a + 1
# 		return a
# 	return closure


# def method(a):
# 	print a
# 	return -1

# old_call = method.__call__
# def new_call(*args, **kwargs):
# 	print 'aspect'
# 	return old_call(*args, **kwargs)
# method.__call__ = new_call

# print(method(5))