from __future__ import print_function

import os
import sys
import time
import struct
import json
import zlib
import threading
import itertools
import traceback

# PY2 = sys.version_info.major <= 2
# if PY2:
#     from StringIO import StringIO as BytesIO
# else:
#     from io import BytesIO

DEBUG = int(os.environ.get('REMORK_DEBUG', '0'))
DEBUG_LOG = None # open('/tmp/boo.log', 'w')
START = None

msg_hdr_st = struct.Struct('!Q B I')  # descriptor message-type message-subtype payload-size
IOSIZE = 65536
MSG_BUFFER_SIZE = 1 << 20
SIDE = 'REMOTE'

btype = type(b'')
ntype = type('')


def bstr(s, encoding='utf-8'):
    if type(s) != btype:
        s = s.encode(encoding)
    return s


def nstr(s, encoding='utf-8'):
    stype = type(s)
    if stype != ntype:
        if stype == btype:
            s = s.decode(encoding)
        else:  # pragma: no cover
            s = s.encode(encoding)
    return s


CALL = 1
RESULT = 2
DATA = 3
DATA_FLAG = 0x40
COMPRESS_FLAG = 0x80


class StopDrain(Exception):
    def __init__(self, value):  # pragma: no cover
        self.value = value


def bg(fn, *args, **kwargs):
    log = kwargs.pop('log_', None)
    def wrapper():
        try:
            fn(*args, **kwargs)
        except:  # pragma: no cover
            if log:
                log.exception('{} router error'.format(SIDE))
            elif traceback is not None:
                traceback.print_exc()

    t = threading.Thread(target=wrapper)
    t.daemon = True
    t.start()
    return t


def copydata(dest, data):
    while data:
        size = dest.write(data)
        debug('writedata', dest, size, data[:size])
        data = data[size:]


class Buffer(object):
    def __init__(self, size):
        lock = threading.RLock()
        self.need_space = threading.Condition(lock)
        self.need_data = threading.Condition(lock)
        self.buffer = b''
        self.maxsize = size
        self.is_done = False

    def done(self):  # pragma: no cover
        with self.need_data:
            self.is_done = True
            self.need_data.notify()

    def fresh(self):
        return Buffer(self.maxsize)

    def write(self, data):
        debug('buf-write', len(self.buffer), data, level=3)
        dsize = len(data)
        assert dsize <= self.maxsize
        with self.need_space:
            while dsize > self.maxsize - len(self.buffer):  # pragma: no cover
                debug('buf-write-wait', len(data), len(self.buffer), level=3)
                self.need_space.wait(0.1)

            self.buffer += data
            self.need_data.notify()
        return dsize

    def read(self, size):
        debug('buf-read-start', level=3)
        with self.need_data:
            debug('buf-read-lock', level=3)
            while not self.buffer and not self.is_done:
                debug('buf-read-wait', len(self.buffer), level=3)
                self.need_data.wait(0.01)

            result = self.buffer[:size]
            debug('buf-read-get', result, level=3)
            self.buffer = self.buffer[size:]
            self.need_space.notify_all()

        return result


class Reader(object):
    def start(self, receiver):
        self.receiver = receiver
        receiver.send(None)
        gen = self.feeder()
        gen.send(None)
        return gen.send

    def read(self, size):
        self.size = size

    def feeder(self):
        buf = b''
        while True:
            data = yield
            buf += data
            while len(buf) >= self.size:
                sz = self.size
                self.receiver.send(buf[:sz])
                buf = buf[sz:]


def protocol(fn):
    def inner(*args, **kwargs):
        reader = Reader()
        receiver = fn(reader, *args, **kwargs)
        return reader.start(receiver)
    return inner


@protocol
def msg_proto_decode(reader, handler):
    while True:
        data = yield reader.read(msg_hdr_st.size)
        msg_id, msg_type, size = msg_hdr_st.unpack_from(data)

        data = yield reader.read(size)
        debug('proto', handler, msg_id, msg_type, data, level=2)

        if msg_type & COMPRESS_FLAG:
            data = zlib.decompress(data)
            msg_type = msg_type & ~COMPRESS_FLAG

        handler(msg_id, msg_type, data)


def msg_encode(msg_id, msg_type, data, compress=False):
    if compress:
        msg_type |= COMPRESS_FLAG
        data = zlib.compress(data)
    return msg_hdr_st.pack(msg_id, msg_type, len(data)) + data


def iter_read(src, size=IOSIZE):
    while True:
        data = src.read(size)
        if not data:
            break
        yield data


class FD(object):  # pragma: no cover
    def __init__(self, fd):
        self.fd = fd

    def read(self, size):
        return os.read(self.fd, size)

    def write(self, data):
        return os.write(self.fd, data)

    def flush(self):
        pass

    def __repr__(self):
        return 'FD({0})'.format(self.fd)


def iter_write(dest, iterator, size=IOSIZE, close=False, flush=False):
    buf = b''
    for data in iterator:
        debug('write, getdata', dest, data, level=3)
        buf += data
        is_first = True
        while is_first or len(buf) > size:
            written = dest.write(buf[:size])
            debug('write', dest, written, buf[:size], level=4)
            buf = buf[written:]
            is_first = False

    if flush:  # pragma: no cover
        dest.flush()

    if close:  # pragma: no cover
        dest.close()


def copy(dest, src, size=IOSIZE, close=False):
    it = iter_read(src, size)
    iter_write(dest, it, size, close=close)


def drain(src, handler, size=IOSIZE):
    for data in iter_read(src, size):
        debug('read', src, data, level=3)
        try:
            handler(data)
        except StopDrain as e:  # pragma: no cover
            return e.value

    handler(b'')


def debug(*args, **kwargs):
    force = kwargs.get('force', False)
    level = kwargs.get('level', 2)
    if DEBUG >= level or force or DEBUG_LOG:  # pragma: no cover
        args = list(args)
        now = time.time()

        if START:  # pragma: no cover
            targs = [now, round((now - START)*1000)]
        else:
            targs = [now]

        if not force and type(args[-1]) == type(b'') and len(args[-1]) > 100:
            args[-1] = args[-1][:100] + b'...(%d bytes total)' % len(args[-1])
        if DEBUG_LOG:  # pragma: no cover
            print(SIDE + ':', *(targs + args), file=DEBUG_LOG)
            DEBUG_LOG.flush()
        else:
            print(SIDE + ':', *(targs + args), file=sys.stderr)
            sys.stderr.flush()


def simplecall(fn):
    fn._remork_simple = True
    return fn


class Router(object):
    def __init__(self, bufsize=None):
        self.buffer = Buffer(bufsize or MSG_BUFFER_SIZE)
        self.write = self.buffer.write
        self.results = {}
        self.counter = itertools.count(1)
        self.result_cond = threading.Condition()

    def reset(self):
        self.buffer = self.buffer.fresh()
        self.write = self.buffer.write

    def make_result(self, on_result):
        msg_id = next(self.counter)
        result = self.results[msg_id] = Result(self, msg_id, on_result)
        return result

    def write_msg(self, msg_id, msg_type, data, compress=None):
        self.buffer.write(msg_encode(msg_id, msg_type, data, compress=compress))

    def done(self, msg_id, result=None):
        self.results.pop(msg_id, None)
        self.write_msg(msg_id, RESULT, bstr(json.dumps({'result': result})))

    def error(self, msg_id, error):
        self.results.pop(msg_id, None)
        self.write_msg(msg_id, RESULT, bstr(json.dumps({'error': error})))

    def data_subscribe(self, msg_id, handler):
        self.results[msg_id] = handler

    def write_data(self, msg_id, data_type, data, compress=None):
        self.write_msg(msg_id, DATA_FLAG | data_type, data, compress=compress)

    def end_data(self, msg_id):
        self.write_msg(msg_id, DATA_FLAG, b'')

    def forget(self, msg_id):  # pragma: no cover
        self.results.pop(msg_id, None)

    def handle_msg(self, msg_id, msg_type, data):
        if msg_type & DATA_FLAG:
            data_type = msg_type & ~DATA_FLAG
            msg_type = DATA
        try:
            if msg_type == CALL:
                info = json.loads(nstr(data))
                m = sys.modules[info['module']]
                fn = getattr(m, info['fn'])
                if getattr(fn, '_remork_simple', None):
                    result = fn(*info['args'], **info.get('kwargs', {}))
                    self.done(msg_id, result)
                else:
                    fn(self, msg_id, *info['args'], **info.get('kwargs', {}))
            elif msg_type == RESULT:
                debug('results', self.results)
                info = json.loads(nstr(data))
                r = self.results.pop(msg_id, None)
                if r:
                    if 'error' in info:
                        r.set_error(info['error'])
                    else:
                        r.set_result(info['result'])
                with self.result_cond:
                    self.result_cond.notify_all()
            elif msg_type == DATA:
                r = self.results.get(msg_id)
                if r:
                    if hasattr(r, 'feed'):
                        r.feed(data_type, data)
                    else:
                        r(data_type, data)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.error(msg_id, str(e))


def router():  # pragma: no cover
    debug('starting router', level=2)
    r = Router()
    r.buffer.write(FD(0).read(10))
    bg(copy, FD(1), r.buffer)
    proto = msg_proto_decode(r.handle_msg)
    debug('router started', level=1)
    drain(FD(0), proto)


def inject_modules(router, msg_id, modules):  # pragma: no cover
    for minfo in modules:
        debug('injecting', minfo['module'], id(router), level=1)
        mname = nstr(minfo['module'])
        pparts = mname.split('.')[:-1]
        mod = type(sys)(mname)
        mod.__file__ = nstr(minfo['file'])
        if pparts:
            mod.__package__ = '.'.join(pparts)
            p = []
            for part in pparts[:-1]:
                p.append(part)
                tp = '.'.join(p)
                if tp not in sys.modules:
                    sys.modules[tp] = type(sys)(tp)

        code = compile(nstr(minfo['source']), mod.__file__, 'exec')
        exec(code, vars(mod))
        sys.modules[mname] = mod


if __name__ == '__main__':  # pragma: no cover
    import sys
    sys.modules['remork'] = type(sys)('remork')
    sys.modules['remork.router'] = sys.modules['__main__']


#==LOCAL==
SIDE = 'LOCAL'

class ResultException(Exception):
    pass


class Result:
    def __init__(self, router, msg_id, on_result=None):
        self.router = router
        self.msg_id = msg_id
        self.result = None
        self.error = None
        self.data = {}
        self.on_result = on_result
        self.done = False

    def ignore(self):  # pragma: no cover
        self.router.forget(self.msg_id)

    def wait(self):
        while not self.done:
            with self.router.result_cond:
                while not self.done:
                    self.router.result_cond.wait()
        if self.error:
            raise ResultException(self.error)
        return self.result

    def set_result(self, value):
        if self.on_result:
            self.result = self.on_result(self, value)
        else:
            self.result = value
        self.done = True

    def set_error(self, value):
        self.error = value
        self.done = True

    def write_data(self, data_type, data, compress=False):
        self.router.write_data(self.msg_id, data_type, data, compress)

    def end_data(self):
        self.router.end_data(self.msg_id)

    def feed(self, data_type, data):
        self.data.setdefault(data_type, []).append(data)


def msg_call(msg_id, module, fname, args, kwargs={}, compress=False):
    data = bstr(json.dumps({'module': module, 'fn': fname, 'args': args, 'kwargs': kwargs}))
    return msg_encode(msg_id, CALL, data, compress=compress)


class LocalRouter(Router):
    def check(self):
        pass

    def call(self, module, fname, *args, **kwargs):
        self.check()
        compress = kwargs.pop('compress_', False)
        on_result = kwargs.pop('on_result_', None)
        result = self.make_result(on_result)
        self.buffer.write(msg_call(result.msg_id, module, fname, args, kwargs, compress=compress))
        return result
