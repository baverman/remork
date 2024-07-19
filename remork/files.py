import stat
import os.path
import json
from remork.router import bstr, nstr, btype, debug, simplecall

if False:
    from typing import IO
    from remork.router import Router


def upload_files(router, msg_id):
    # type: (Router, int) -> None
    cfile = [None]  # type: list[IO[bytes]] # type: ignore[list-item]
    created_dirs = set()

    def handler(data_type, data):
        # type: (int, bytes) -> None
        f = cfile[0]

        if data_type == 2:  # data for current file
            f.write(data)
        elif data_type == 1:  # new file
            if f:  # close previous file
                f.close()

            item = json.loads(nstr(data))
            dest = nstr(item['dest'])
            mode = item['mode']

            try:
                fmode = os.stat(dest).st_mode
                if not fmode & stat.S_IWUSR:
                    os.chmod(dest, stat.S_IMODE(fmode) | stat.S_IWUSR)
            except Exception as e:
                pass

            dname = os.path.dirname(dest)
            if dname not in created_dirs:
                if not os.path.exists(dname):
                    os.makedirs(dname)
                    created_dirs.add(dname)

            cfile[0] = f = open(dest, 'wb')
            if mode:
                os.fchmod(f.fileno(), mode)
        else:  # transfer end
            if f:
                f.close()
            router.done(msg_id)

    router.data_subscribe(msg_id, handler)


def read_file(path, default=''):
    # type: (str, str) -> str
    if os.path.exists(path):
        with open(path) as fd:
            return fd.read()
    return default


def atomic_write(path, content):
    # type: (str, str | bytes) -> None
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
    # type: (str, str) -> bool
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


def find_line(lines, line):
    # type: (list[str], str) -> int | None
    try:
        return lines.index(line)
    except ValueError:
        return None


@simplecall
def blockinfile(path, marker, block):
    # type: (str, str, str) -> bool
    path = nstr(path)
    marker = nstr(marker)
    block = nstr(block)
    startmarker = marker + ' REMORK BLOCK START'
    endmarker = marker + ' REMORK BLOCK END'

    content = read_file(path, '')
    lines = content.splitlines()

    head = lines
    tail = []
    start = find_line(lines, startmarker)
    if start is not None:
        end = find_line(lines, endmarker)
        if end is not None:
            head = lines[:start]
            tail = lines[end+1:]

    lines = head + [startmarker, block.rstrip('\n'), endmarker] + tail + ['']
    newcontent = '\n'.join(lines)
    changed = content != newcontent
    if changed:
        atomic_write(path, newcontent)
    return changed


#==LOCAL==
from remork.router import iter_read

if False:
    from typing import Iterable, Any
    from remork.router import Result, LocalRouter


def upload_files_helper(router, items):
    # type: (LocalRouter, Iterable[dict[str, Any]]) -> Result
    rv = router.call('remork.files', 'upload_files')

    for it in items:
        if rv.done:
            return rv

        fmode = None
        if it.get('copymode') and it.get('file'):
            fmode = os.stat(it['file']).st_mode

        dest = it['dest']
        rv.write_data(1, bstr(json.dumps({'dest': dest, 'mode': it.get('mode') or fmode})))
        if it.get('content') is not None:
            rv.write_data(2, bstr(it['content']))
        elif it.get('file'):
            with open(it['file'], 'rb') as f:
                for data in iter_read(f, 1 << 18):
                    rv.write_data(2, data, compress=len(data) > 512)
        else:
            for data in iter_read(it['buf'], 1 << 18):
                rv.write_data(2, data, compress=len(data) > 512)
    rv.end_data()
    return rv
