#!/usr/bin/env python2

import os
import sys
import time
import optparse
import threading
import signal
import glob

# the size of block used to find the position of end of DST in SRC.
# note: false-positive (too small) or false-negative (too large) will
# cause data duplication.
PATTERN_SIZE = 4096
BUFSIZE = 1<<20
BATCH_SIZE = 1024
WAIT_DURATION = 0.1

source_paths = {}
open_files = {}
running = True

def search_pattch(f, pattern):
    if not pattern: return 0
    buff = ''
    pos = 0
    f.seek(pos)
    while True:
        buff += f.read(BUFSIZE)
        if len(buff) < len(pattern):
            return 0
        n = buff.find(pattern)
        if n >= 0:
            return pos + n + len(pattern)
        pos += len(buff) - len(pattern) + 1
        buff = buff[-len(pattern)+1:]
    return 0

def get_fsize(f):
    return os.fstat(f.fileno()).st_size

def find_last_pos(fin, fout):
    isize = get_fsize(fin)
    osize = get_fsize(fout)
    vl = min(min(osize, PATTERN_SIZE), isize)
    if osize <= isize and vl < isize and vl < osize:
        block = fin.read(vl)
        fout.seek(0)
        if fout.read(vl) == block:
            return osize
    fout.seek(osize-vl)
    pattern = fout.read(vl)
    return search_pattch(fin, pattern)

def stream(src, dst, option):
    fout = open(dst, 'ab+', BUFSIZE+4096)
    open_files[dst] = fout
    fin = open(src, 'rb')
    pos = get_fsize(fout)
    if option.resume:
        pos = find_last_pos(fin, fout)
        fout.seek(0, 2)
    print('start copying', src, 'at', pos)
    fin.seek(pos)
    last_copied = time.time()
    while running:
        line = fin.read(BUFSIZE)
        if not line:
            while running and get_fsize(fin) == pos:
                if os.path.exists(src) and os.path.getsize(src) != pos:
                    break  # rotated
                time.sleep(WAIT_DURATION)
                if option.deleteAfter and time.time() > last_copied+option.deleteAfter:
                    del source_paths[src]
                    del open_files[dst]
                    fin.close()
                    fout.close()
                    print("remove", src)
                    os.remove(src)
                    return
            if not running:
                return
            csize = get_fsize(fin)
            if csize > pos:
                # tell File to read more data
                fin.seek(pos)
            else:
                fin.close()
                fin = open(src, 'rb')
                pos = 0
            continue
        pos += len(line)
        last_copied = time.time()
        while running:
            try:
                fout.write(line)
                fout.flush()
                break
            except ValueError:
                # closed by rotate
                fout = open(dst, 'ab+', BUFSIZE+4096)
                open_files[dst] = fout


def start_thread(func, args):
    def target():
        try:
            func(*args)
        except Exception as e:
            sys.stderr.write(str(e)+'\n')
    t = threading.Thread(target=target)
    t.daemon = True
    t.start()
    return t

def start_stream(src, dst, option):
    return start_thread(stream, (src, dst, option))

def discover_new_file(src, dst, option):
    while running:
        for root, dirs, names in os.walk(src):
            if len(root) > len(src)+1:
                t = os.path.join(dst, root[len(src)+1:])
            else:
                t = root
            if not os.path.exists(t):
                os.makedirs(t)    
            for n in names:
                p = os.path.join(root, n)
                if p not in source_paths and os.path.isfile(p):
                    t = os.path.join(dst, p[len(src)+1:])
                    source_paths[p] = start_stream(p, t, option)
        time.sleep(1)

def rotate(signum, frame):
    for f in open_files:
        open_files[f].close()

def interrupted(signum, frame):
    print("interrupted")
    global running
    running = False
    os._exit(1)

def main():
    parser = optparse.OptionParser("streamcopy.py SRC DST [OPTIONS]")
    parser.add_option("--pid", help="path for pid (SIGHUP to rotate)")
    parser.add_option("--delete-after", dest="deleteAfter", type=int,
                      help="delete files after no new data for N seconds")
    parser.add_option("--resume", action="store_true",
                      help="resume copying based on guessed position "
                           + "(try to find first occurrence of last block of output file "
                           + " in input stream).")
    option, args = parser.parse_args()
    if len(args) < 2:
        parser.print_usage()
        return
    src, dst = args
    if not os.path.exists(dst):
        os.makedirs(dst)
    if option.pid:
        with open(option.pid, 'w') as f:
            f.write(str(os.getpid()))
        signal.signal(signal.SIGHUP, rotate)
    signal.signal(signal.SIGINT, interrupted)
    start_thread(discover_new_file, (src, dst, option))
    while running:
        time.sleep(1)

if __name__ == '__main__':
    main()
