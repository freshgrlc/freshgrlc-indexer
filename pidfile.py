from os import getpid, path

from logger import log_event


RUNDIR = '/run'
FILEEXT = '.pid'


def make_pidfile(module):
    basename = path.splitext(path.basename(module.__file__))[0]
    pidfile = path.join(RUNDIR, basename + FILEEXT)
    with open(pidfile, 'w') as f:
        f.write(str(getpid()))
    log_event('Wrote', 'PID', pidfile)
