import os.path
from remork.router import bstr, nstr, btype, debug, simplecall


def upload_file(router, msg_id, dest, mode=None):
    dest = nstr(dest)
    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))

    f = open(dest, 'wb')
    if mode:
        os.fchmod(f.fileno(), mode)

    def handler(data_type, data):
        if data_type:
            f.write(data)
        else:
            f.close()
            router.done(msg_id)

    router.data_subscribe(msg_id, handler)


def read_file(path, default=None):
    if os.path.exists(path):
        with open(path) as fd:
            return fd.read()
    return default


def atomic_write(path, content):
    if type(content) is btype:
        mode = 'wb'
    else:
        mode = 'w'
    tmp = path + '.remork-tmp'
    with open(tmp, mode) as fd:
        fd.write(content)
    os.rename(tmp, path)


@simplecall
def lineinfile(path, line):
    line = nstr(line)
    path = nstr(path)
    lines = read_file(path, '').splitlines()
    found = False
    for it in lines:
        if it == line:
            found = True
            break

    if not found:
        lines.append(line+'\n')
        atomic_write(path, '\n'.join(lines))

    return not found


#==LOCAL==
from remork.router import iter_read

def upload_file_helper(router, dest, source=None, content=None, mode=None):
    rv = router.call('remork.files', 'upload_file', dest=dest, mode=mode)
    if content is not None:
        rv.write_data(1, bstr(content))
    elif source is not None:
        for data in iter_read(source, 1 << 18):
            rv.write_data(1, data, compress=len(data) > 512)
    rv.end_data()
    return rv
