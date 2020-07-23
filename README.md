# streamcopy
Continuous copy growing text files to another place

# Example

```
$ python streamcopy.py /var/log/nginx/ /jfs/logs/`hostname`/ --pid /tmp/streamcopy.pid
```

It will mirror all the Nginx logs on each machines into JuiceFS in realtime, so you can grep or tail the most recent logs.

You can setup logrotate for Nginx logs in local disk and JuiceFS separately. To work with logrotate, you need to rename the latest logfile and send SIGHUP to streamcopy process (`/tmp/streamcopy.pid`).

# Usage

```
Usage: streamcopy.py SRC DST [options]

Options:
  -h, --help       show this help message and exit
  --pid=PID        path for pid (SIGHUP to rotate)
  --delete-after=DELETEAFTER
                   delete files after no new data for N seconds  
  --resume    resume copying based on guessed position (try to find first
              occurrence of last block of output file  in input stream).
```
