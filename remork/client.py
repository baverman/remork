import os.path
import zlib
import shlex
import subprocess
import time
import logging

from . import process, files, router
from .router import copy, copydata, msg_proto_decode, drain, bg, bstr, LocalRouter, msg_call

log = logging.getLogger('remork.client')

BOOT_TAIL = '''\
DEBUG = %d
router()
'''

MODULES = [process, files]


class ConnectException(OSError): pass


def extract_remote_part(source):
    return source.partition('#==LO' + 'CAL==')[0]


def make_source_bundle(modules):
    result = []
    for m in modules:
        fname = m.__file__
        if fname.endswith('.pyc'):  # pragma: no cover
            fname = fname[:-4] + '.py'
        with open(fname, 'r') as fobj:
            source = extract_remote_part(fobj.read())
        result.append({'module': m.__name__, 'file': m.__file__, 'source': source})
    return result


STAGE0 = '''\
import os, zlib
data = os.read(0, {0})
source = zlib.decompress(data).decode('utf-8')
exec(source)
'''


def bootstrap(debug=0):
    script = []
    fname = os.path.join(os.path.dirname(__file__), 'router.py')
    script.append(extract_remote_part(open(fname).read()))
    script.append(BOOT_TAIL % debug)

    stage1 = zlib.compress(bstr('\n'.join(script)))
    # BAD. here we hope that os.read returns all requested data in a single call
    stage0 = 'import os, zlib; exec(zlib.decompress(os.read(0, {0})).decode("utf-8"))'.format(len(stage1))
    # stage0 = STAGE0.format(len(stage1))
    return stage0, stage1


class ProcessRouter(LocalRouter):
    def __init__(self, cmd, stage1, sources, shell=False, bufsize=None):
        super().__init__(bufsize=bufsize)
        self.stage1 = stage1
        self.sources = sources
        self.cmd = cmd
        self.shell = shell

    def connect(self):
        self.reset()
        self.proc = subprocess.Popen(self.cmd, shell=self.shell, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, bufsize=0)

    def handshake(self):
        ack = os.urandom(10)
        call = msg_call(0, 'remork.router', 'inject_modules', (self.sources,), compress=True)
        payload = b''.join([self.stage1, ack, call])
        copydata(self.proc.stdin, payload)
        # TODO: add timeout
        remote_ack = self.proc.stdout.read(10)
        if ack != remote_ack:
            raise ConnectException(
                'Invalid ack: \n' + repr(self.proc.stdout.read()))
        router.debug('handshake done', level=1)

    def check(self):
        if self.proc is None:
            self.connect()
            self.process()

    def monitor(self):
        while True:
            if self.proc.poll() is not None:
                self.close()
                break
            time.sleep(0.9)

    def process(self):
        self.handshake()
        proto = msg_proto_decode(self.handle_msg)
        t1 = bg(drain, self.proc.stdout, proto)
        t2 = bg(copy, self.proc.stdin, self.buffer)
        bg(self.monitor)

    def close(self):
        router.debug('Closing local router', level=1)
        self.buffer.done()
        proc = self.proc
        self.proc = None
        for r in self.results.values():
            r.set_error('Closed: ' + repr(proc.args) + '\n' + repr(proc.stdout.read()))
            r.done = True
        with self.result_cond:
            self.result_cond.notify_all()


def make_command(python_cmd, stage0, double_quote=False):
    quoted = shlex.quote(stage0)
    if double_quote:
        quoted = shlex.quote(quoted)
    return python_cmd + ' -c ' + quoted


def connect(python_cmd=['python'], shell_cmd=None, double_quote=False, debug=None, additional=None):
    if debug is None:
        debug = router.DEBUG
    else:
        router.DEBUG = debug
    stage0, stage1 = bootstrap(debug=debug)
    shell = False
    if isinstance(python_cmd, (list, tuple)):
        python_cmd += ['-c', stage0]
    else:  # pragma: no cover
        shell = True
        python_cmd = make_command(python_cmd, stage0, double_quote)
        if shell_cmd:
            python_cmd = shell_cmd.replace('{cmd}', shlex.quote(python_cmd))

    sources = make_source_bundle(MODULES + (additional or []))
    log.info('Creating router: %s', python_cmd)

    c = ProcessRouter(python_cmd, stage1, sources, shell=shell)
    c.connect()
    c.process()
    return c
