
#          Copyright Lennart Braun 2020,
#                    Niek Bouman 2021.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE or copy at
#          https://www.boost.org/LICENSE_1_0.txt)

import gdb


def write_register(name, value):
    """Write value to register"""
    hex_value = value.format_string(format='x')
    cmd = f"set ${name} = {hex_value}"
    gdb.execute(cmd)


class FContext:
    """Representation of boost::context::detail::fcontext"""

    def __init__(self, ptr):
        self.ptr = ptr

    def read_registers(self):
        values = {}
        #  gdb.execute('x /8gx {}'.format(ctx.format_string(format='x')))
        t = gdb.lookup_type('uint64_t').array(8)
        registers = self.ptr.cast(t)
        # ignore x87 control word and MXCSR
        values['r12'] = registers[1]
        values['r13'] = registers[2]
        values['r14'] = registers[3]
        values['r15'] = registers[4]
        values['rbx'] = registers[5]
        values['rbp'] = registers[6]
        values['rip'] = registers[7]
        values['rsp'] = self.ptr
        return values


class Context:
    """Representation of boost::fibers::context"""

    # offsets:
    # remote_ready_hook_: 16
    # wait_hook_: 48
    # sleep_hook_: 128
    # ready_hook_: 160
    # terminated_hook_: 176
    # worker_hook_: 184

    def __init__(self, ptr, raw=False):
        if raw:
            ctx_p = gdb.lookup_type("boost::fibers::context").pointer()
            ptr = gdb.Value(ptr).cast(ctx_p)
        self.ptr = ptr

    @staticmethod
    def offset_of(member):
        return int(gdb.parse_and_eval(f"&'boost::fibers::context'::{member}").format_string(format='x'), 0)

    @staticmethod
    def offset_of_remote_ready_hook():
        return Context.offset_of('remote_ready_hook_')

    @staticmethod
    def offset_of_wait_hook():
        return Context.offset_of('wait_hook_')

    @staticmethod
    def offset_of_sleep_hook():
        return Context.offset_of('sleep_hook_')

    @staticmethod
    def offset_of_ready_hook():
        return Context.offset_of('ready_hook_')

    @staticmethod
    def offset_of_terminated_hook():
        return Context.offset_of('terminated_hook_')

    @staticmethod
    def offset_of_worker_hook():
        return Context.offset_of('worker_hook_')

    @staticmethod
    def find_active_context():
        ptr = gdb.parse_and_eval("'boost::fibers::context::active'()")
        return Context(ptr)

    def get(self):
        return self.ptr.dereference()

    def get_ptr(self):
        return self.ptr

    def get_fctx(self):
        return FContext(self.ptr.dereference()['c_']['fctx_'])

    def get_type(self):
        return self.ptr.dereference()['type_']

    def get_policy(self):
        return self.ptr.dereference()['policy_']

    def is_worker(self):
        return self.ptr.dereference()['worker_hook_']['next_'] == 0

    def is_terminated(self):
        #  return self.ptr.dereference()['terminated_hook_']['next_'] == 0
        return self.ptr.dereference()['terminated_']

    def is_ready(self):
        return self.ptr.dereference()['ready_hook_']['next_'] == 0

    def is_resumable(self):
        fun = gdb.parse_and_eval("boost::fibers::context::is_resumable")
        return fun(self.ptr)    
    
    def is_sleeping(self):
        raise NotImplementedError

    def is_waiting(self):
        return self.ptr.dereference()['wait_hook_']['next_'] == 0

    def get_scheduler(self):
        return Scheduler(self.ptr.dereference()['scheduler_'])

    def backtrace(self):
        original_frame = gdb.selected_frame()
        regs = self.get_fctx().read_registers()
        gdb.execute('regstash push')
        gdb.newest_frame().select()
        for reg, val in regs.items():
            write_register(reg, val)

        print("backtrace on fiber:")
        gdb.execute('bt')

        gdb.execute('regstash pop')
        original_frame.select()

    def switch(self):
        regs = self.get_fctx().read_registers()
        gdb.newest_frame().select()
        for reg, val in regs.items():
            write_register(reg, val)
        print(f"switched to context {self}")

    def __str__(self):
        return f'({self.ptr.type}) {self.ptr}'

    def print(self):
        print(self)
        print('type: {}'.format(str(self.get_type())))
        state = 'state: ['
        if self.is_terminated():
            state += 'T'
        #  if self.is_sleeping():
        #      state += 'S'
        if self.is_waiting():
            state += 'W'
        state += ']'
        print(state)


class Scheduler:
    """Representation of boost::fibers::scheduler"""

    def __init__(self, ptr):
        self.ptr = ptr

    def get(self):
        return self.ptr.dereference()

    def get_ptr(self):
        return self.ptr

    def get_main_ctx(self):
        ctx = self.ptr.dereference()['main_ctx_']
        return Context(ctx)

    def get_dispatcher_ctx(self):
        ctx = self.ptr.dereference()['dispatcher_ctx_']['px']
        return Context(ctx)

    def get_shutdown(self):
        return self.ptr.dereference()['shutdown_']

    def get_algorithm(self):
        algo = self.ptr.dereference()['algo_']['px']
        return Algorithm(algo)

    def get_worker_queue(self):
        list_root = self.ptr.dereference()['worker_queue_']['data_']['root_plus_size_']['m_header']
        node = list_root['next_']
        last_node = list_root['prev_']
        if node == last_node:
            return []
        list_nodes = []
        while node != last_node:
            list_nodes.append(node)
            node = node['next_']
        list_nodes.append(last_node)
        char_p = gdb.lookup_type('char').pointer()
        context_p = gdb.lookup_type("boost::fibers::context").pointer()
        # worker_hook_ is at offset 184 in context
        # => pointer in worker_queue_ -184 to get context pointers
        fibers = [
                #  Context((n.cast(char_p) - 184).cast(context_p))
                Context((n.cast(char_p) - Context.offset_of_worker_hook()).cast(context_p))
                for n in list_nodes
                ]
        return fibers

    def get_sleep_queue(self):
        raise NotImplementedError

    def get_terminated_queue(self):
        raise NotImplementedError

    def get_remote_ready_queue(self):
        raise NotImplementedError

    def has_ready_fibers(self):
        fun = gdb.parse_and_eval("boost::fibers::scheduler::has_ready_fibers")
        return fun(self.ptr)
        
    @staticmethod
    def find():
        ctx = Context.find_active_context()
        return ctx.get_scheduler()

    def __str__(self):
        return f'({self.ptr.type}) {self.ptr}'

    def print(self):
        print(f'Scheduler: {self}')
        print(f'algorithm: {self.get_algorithm()}')
        print(f'main context: {self.get_main_ctx()}')
        print(f'dispatcher context: {self.get_dispatcher_ctx()}')


class Algorithm:
    """Representation of boost::fibers::algo::algorithm"""

    def __init__(self, ptr):
        self.ptr = ptr

    @staticmethod
    def find():
        scheduler = Scheduler.find()
        ptr = scheduler.get()['algo_']['px']
        vptr = ptr['_vptr.algorithm']
        vptr_s = str(vptr)
        if 'boost::fibers::algo::round_robin' in vptr_s:
            return RoundRobin(ptr)
        elif 'boost::fibers::asio::round_robin' in vptr_s:
            return AsioRoundRobin(ptr)
        else:
            print("unknown scheduling algorithm: {}".format(str(vptr_s)))
            return Algorithm(ptr)

    def __str__(self):
        return f'({self.ptr.type}) {self.ptr}'

class RoundRobin(Algorithm):
    """Representation of boost::fibers::algo::round_robin"""

    def __init__(self, ptr):
        t = gdb.lookup_type("'boost::fibers::algo::round_robin'").pointer()
        super().__init__(ptr.cast(t))

class AsioRoundRobin(Algorithm):
    """Representation of boost::fibers::asio::round_robin"""

    def __init__(self, ptr):
        t = gdb.lookup_type('boost::fibers::asio::round_robin').pointer()
        super().__init__(ptr.cast(t))

class Fiber(gdb.Command):
    """Collection of commands for Boost.Fiber"""

    def __init__(self):
        super(Fiber, self).__init__ ("fiber", gdb.COMMAND_USER, prefix=True)


class FiberMain(gdb.Command):
    """Give information about this threads main fiber"""

    def __init__(self):
        super(FiberMain, self).__init__ ("fiber main", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if len(argv) not in [0, 1]:
            print("usage: fiber main [bt | switch]")
            return
        scheduler = Scheduler.find()
        if scheduler is None:
            return
        ctx = scheduler.get_main_ctx()
        ctx.print()
        regs = ctx.get_fctx().read_registers()
        for reg, val in regs.items():
            print("{:3s}    {:s}".format(reg, val.format_string(format='x')))

        if len(argv) == 0:
            return

        if argv[0] == 'bt':
            ctx.backtrace()
        elif argv[0] == 'switch':
            ctx.switch()


class FiberScheduler(gdb.Command):
    """Show information about the current threads' scheduler object"""

    def __init__(self):
        super(FiberScheduler, self).__init__ ("fiber scheduler", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        scheduler = Scheduler.find()
        if scheduler is None:
            print("Could not find scheduler")
            return
        scheduler.print()
        # TODO: print more information


class FiberAlgorithm(gdb.Command):
    """Show information about the current threads' scheduling algorithm"""

    def __init__(self):
        super(FiberAlgorithm, self).__init__ ("fiber algorithm", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        algorithm = Algorithm.find()
        print(algorithm)
        # TODO: print more information


class FiberWorkers(gdb.Command):
    """List the fibers in this thread's scheduler's worker queue"""

    def __init__(self):
        super(FiberWorkers, self).__init__ ("fiber workers", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        scheduler = Scheduler.find()
        if scheduler is None:
            print("Could not find scheduler")
            return
        workers = scheduler.get_worker_queue()
        for w in workers:
            print(w)


class FiberContext(gdb.Command):
    """Gives information about a given fiber context"""

    def __init__(self):
        super(FiberContext, self).__init__ ("fiber context", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if len(argv) not in [1, 2]:
            print("usage: fiber context <pointer to context> [bt | switch]")
            return
        ptr = int(argv[0], 0)
        ctx = Context(ptr, raw=True)
        ctx.print()
        regs = ctx.get_fctx().read_registers()
        for reg, val in regs.items():
            print("{:3s}    {:s}".format(reg, val.format_string(format='x')))

        if len(argv) == 1:
            return

        if argv[1] == 'bt':
            ctx.backtrace()
        elif argv[1] == 'switch':
            ctx.switch()


Fiber()
FiberMain()
FiberScheduler()
FiberAlgorithm()
FiberWorkers()
FiberContext()
