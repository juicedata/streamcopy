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
    pos = get_fsize(fin)
    if option.resume:
        pos = find_last_pos(fin, fout)
        fout.seek(0, 2)
    print 'start copying', src, 'at', pos
    fin.seek(pos)
    while True:
        line = fin.read(BUFSIZE)
        if line == BUFSIZE:
            line += fin.readline()
        pos += len(line)
        if not line:
            while get_fsize(fin) == pos:
                if os.path.exists(src) and os.path.getsize(src) != pos:
                    break  # rotated
                time.sleep(WAIT_DURATION)
            csize = get_fsize(fin)
            if csize > pos:
                # tell File to read more data
                fin.seek(pos)
            else:
                fin.close()
                fin = open(src)
                pos = 0
            continue
        while True:
            try:
                fout.write(line)
                fout.flush()
                break
            except ValueError:
                # closed by rotate
                fout = open(dst, 'ab', BUFSIZE+4096)
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

def start_stream(path, option):
    dst = os.path.join(option.dst, os.path.basename(path))
    return start_thread(stream, (path, dst, option))

def discover_new_file(gs, option):
    while True:
        for g in gs:
            for path in glob.glob(g):
                if path not in source_paths and os.path.isfile(path):
                    source_paths[path] = start_stream(path, option)
        time.sleep(5)

def rotate():
    for f in open_files:
        open_files[f].close()

def main():
    parser = optparse.OptionParser("streamcopy.py GLOBS ...")
    parser.add_option("--dst", help="output directory")
    parser.add_option("--pid", help="path for pid (SIGHUP to rotate)")
    parser.add_option("--resume", action="store_true",
                      help="resume copying based on guessed position "
                           + "(try to find first occurrence of last block of output file "
                           + " in input stream).")
    option, globs = parser.parse_args()
    if not option.dst:
        print 'dst is required'
        return
    if not os.path.exists(option.dst):
        os.makedirs(option.dst)
    if option.pid:
        with open(option.pid, 'w') as f:
            f.write(str(os.getpid()))
        signal.signal(signal.SIGHUP, lambda signum, frame: rotate())
    try:
        discover_new_file(globs, option)
    except KeyboardInterrupt:
        return

if __name__ == '__main__':
    main()
