import _thread

_allocate_lock = _thread.allocate_lock
Lock = _allocate_lock

from _thread import stack_size

class Thread:

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None):
        self.target = target
        self.args = args
        self.kwargs = {} if kwargs is None else kwargs

    # def change_stack_size(self, size):
    #     if size > 0:
    #         # stack size (in bytes) to be used for subsequently created threads (using start_new_thread)
    #         first = _thread.stack_size(size)
    #         second = _thread.stack_size()
    #         print("Stack size changed from {} to {}".format(first, second))

    def start(self):
        _thread.start_new_thread(self.run, ())

    def run(self):
        self.target(*self.args, **self.kwargs)
