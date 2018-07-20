# streamcopy
Continuous copy growing text files to another place

# Example

```
$ python streamcopy.py --dst /jfs/logs/`hostname`/ --pid /tmp/streamcopy.pid --resume /var/log/nginx/\*.log
```

It will mirror all the Nginx logs on each machines into JuiceFS in realtime, so you can grep or tail the most recent logs.

You can setup logrotate for Nginx logs in local disk and JuiceFS separately. To work with logrotate, you need to rename the latest logfile and send SIGHUP to streamcopy process (`/tmp/streamcopy.pid`).

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
