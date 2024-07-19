import pytest

from remork import files, process
from remork.router import nstr, bstr, ResultException


def some_fn(router, msg_id, boo, foo):
    router.done(msg_id, (boo, foo))


def some_err(router, msg_id):
    raise Exception('some-error')


def data_fn(r, msg_id):
    d = []

    def handler(data_type, data):
        if data_type:
            d.append(data)
        else:
            r.done(msg_id, nstr(b'|'.join(d)))

    r.data_subscribe(msg_id, handler)


def data_gen_fn(r, msg_id, chunks):
    for it in chunks:
        r.write_data(msg_id, 1, bstr(it))
    r.done(msg_id, None)


def upload_file_helper(router, dest, source=None, content=None, mode=None):
    item = {'dest': dest, 'mode': mode, 'content': content, 'copymode': True}
    if source and hasattr(source, 'read'):
        item['buf'] = source
    else:
        item['file'] = source
    return files.upload_files_helper(router, [item])


def test_router(router):
    rv = router.call('tests.test_router', 'some_fn', 'boo', foo='foo')
    assert rv.wait() == ['boo', 'foo']

    rv = router.call('tests.test_router', 'some_fn', 'zoo', foo='bar', compress_=True)
    assert rv.wait() == ['zoo', 'bar']

    rv = router.call('tests.test_router', 'some_err')
    with pytest.raises(ResultException) as ei:
        rv.wait()

    assert ei.match('some-error')

    rv = router.call('tests.test_router', 'data_fn')
    rv.write_data(1, b'boo')
    rv.write_data(1, b'foo')
    rv.end_data()
    assert rv.wait() == 'boo|foo'

    rv = router.call('tests.test_router', 'data_gen_fn', ['boo', 'foo'])
    assert rv.wait() is None
    assert rv.data[1] == [b'boo', b'foo']


def test_file_upload(tmpdir, router):
    destfile = tmpdir.join('boo')
    upload_file_helper(router, str(destfile), content=b'data').wait()
    assert destfile.read() == 'data'

    source = tmpdir.join('zoo')
    source.write('bazooka')
    upload_file_helper(router, str(destfile), source=open(str(source), 'rb')).wait()
    assert destfile.read() == 'bazooka'


def test_file_lineinfile(tmpdir, router):
    destfile = tmpdir.join('boo')

    rv = router.call('remork.files', 'lineinfile', str(destfile), 'boo')
    assert rv.wait() == True
    assert destfile.read() == 'boo\n'

    rv = router.call('remork.files', 'lineinfile', str(destfile), 'boo')
    assert rv.wait() == False
    assert destfile.read() == 'boo\n'


def test_process(router):
    rv = process.run_helper(router, 'echo "boo"; exit 2')
    assert rv.wait() == (2, b'boo\n', b'')
