# streamcopy
Continuous copy growing text files to another place

# Usage

```
Usage: streamcopy.py GLOBS ...

Options:
  -h, --help       show this help message and exit
  --dst=DST        output directory
  --pid=PID        path for pid (SIGHUP to rotate)
  --resume=RESUME  resume copying based on guessed position (try to find first
                   occurrence of last block of output file  in input stream).
```
# Example

```
$ python streamcopy.py --dst /jfs/logs/`hostname`/ --pid /tmp/streamcopy.pid --resume /var/log/nginx/\*.log
```
