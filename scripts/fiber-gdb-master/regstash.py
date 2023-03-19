
#          Copyright Lennart Braun 2020.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE or copy at
#          https://www.boost.org/LICENSE_1_0.txt)

import gdb
from collections import defaultdict


reg_stash = defaultdict(list)

BASE_REGISTERS = [ "rax", "rbx", "rcx", "rdx", "rsi", "rdi", "rbp", "rsp",
        "r8", "r9", "r10", "r11", "r12", "r13", "r14", "r15", "rip", "eflags",
        "cs", "ss", "ds", "es", "fs", "gs"]

EXTRA_REGISTERS = [ "st0", "st1", "st2", "st3", "st4", "st5", "st6", "st7",
        "fctrl", "fstat", "ftag", "fiseg", "fioff", "foseg", "fooff", "fop",
        "mxcsr"]
VECTOR_REGISTERS = [f"ymm{x}.v4_int64[{y}]" for x in range(16) for y in range(4)]

ALL_REGISTERS = BASE_REGISTERS + EXTRA_REGISTERS + VECTOR_REGISTERS


def get_thread_id():
    t = gdb.selected_thread()
    return t.global_num

def read_register(name):
    value = gdb.parse_and_eval(f'${name}')
    return value

def write_register(name, value):
    hex_value = value.format_string(format='x')
    cmd = f"set ${name} = {hex_value}"
    gdb.execute(cmd)

def store_registers(tid):
    values = {reg: read_register(reg) for reg in ALL_REGISTERS}
    reg_stash[tid].append(values)

def restore_registers(tid):
    values = reg_stash[tid].pop()
    for reg, value in values.items():
        write_register(reg, value)


class RegStash(gdb.Command):

    def __init__(self):
        super(RegStash, self).__init__ ("regstash", gdb.COMMAND_USER, prefix=True)


class RegStashPush(gdb.Command):

    def __init__(self):
        super(RegStashPush, self).__init__ ("regstash push", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        # get a unique identifier of the current thread
        tid = get_thread_id()
        # save the currently selected frame
        original_frame = gdb.selected_frame()
        # select the newest frame
        frame0 = gdb.newest_frame()
        frame0.select()
        # store register values in the stash
        store_registers(tid)
        # go back to the original frame
        original_frame.select()
        print(f"saved current register values of thread {tid} to stash")

class RegStashPop(gdb.Command):

    def __init__(self):
        super(RegStashPop, self).__init__ ("regstash pop", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        # get a unique identifier of the current thread
        tid = get_thread_id()
        # save the currently selected frame
        original_frame = gdb.selected_frame()
        # select the newest frame
        frame0 = gdb.newest_frame()
        frame0.select()
        # restore register values from the stash
        try:
            restore_registers(tid)
        except IndexError:
            original_frame.select()
            print(f"register stash for thread {tid} is empty")
        # go back to the original frame
        #  original_frame.select()

class RegStashDrop(gdb.Command):
    """Drop the most recent entry from the stash"""

    def __init__(self):
        super(RegStashDrop, self).__init__ ("regstash drop", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        tid = get_thread_id()
        try:
            reg_stash[tid].pop()
        except IndexError:
            print(f"register stash for thread {tid} is empty")

class RegStashShow(gdb.Command):
    """Give an overview of the stash"""

    def __init__(self):
        super(RegStashShow, self).__init__ ("regstash show", gdb.COMMAND_USER)

    def invoke (self, arg, from_tty):
        tid = get_thread_id()
        entries = reg_stash[tid]
        print(f"{len(entries)} entries for thread {tid} in the stash")
        if not entries:
            return
        values = entries[-1]
        print("most recent register values:")
        for register in BASE_REGISTERS:
            print("{:18s}    {:s}".format(register, values[register].format_string(format='x')))


RegStash()
RegStashPush()
RegStashPop()
RegStashDrop()
RegStashShow()
