import pytest

from remork import router, files, process


def make_router_pair():
    rl = router.LocalRouter()
    rr = router.Router()
    local_proto = router.msg_proto_decode(rl.handle_msg)
    remote_proto = router.msg_proto_decode(rr.handle_msg)
    router.bg(router.drain, rl.buffer, remote_proto)
    router.bg(router.drain, rr.buffer, local_proto)
    return rl, rr


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
            r.done(msg_id, router.nstr(b'|'.join(d)))

    r.data_subscribe(msg_id, handler)


def data_gen_fn(r, msg_id, chunks):
    for it in chunks:
        r.write_data(msg_id, 1, router.bstr(it))
    r.done(msg_id, None)


def test_router():
    rl, _rr = make_router_pair()

    rv = rl.call('tests.test_router', 'some_fn', 'boo', foo='foo')
    assert rv.wait() == ['boo', 'foo']

    rv = rl.call('tests.test_router', 'some_fn', 'zoo', foo='bar', compress_=True)
    assert rv.wait() == ['zoo', 'bar']

    rv = rl.call('tests.test_router', 'some_err')
    with pytest.raises(router.ResultException) as ei:
        rv.wait()

    assert ei.match('some-error')

    rv = rl.call('tests.test_router', 'data_fn')
    rv.write_data(1, b'boo')
    rv.write_data(1, b'foo')
    rv.end_data()
    assert rv.wait() == 'boo|foo'

    rv = rl.call('tests.test_router', 'data_gen_fn', ['boo', 'foo'])
    assert rv.wait() is None
    assert rv.data[1] == [b'boo', b'foo']


def test_file_upload(tmpdir):
    rl, _rr = make_router_pair()

    destfile = tmpdir.join('boo')
    files.upload_file_helper(rl, str(destfile), content=b'data').wait()
    assert destfile.read() == 'data'

    source = tmpdir.join('zoo')
    source.write('bazooka')
    files.upload_file_helper(rl, str(destfile), source=open(str(source), 'rb')).wait()
    assert destfile.read() == 'bazooka'


def test_file_lineinfile(tmpdir):
    rl, _rr = make_router_pair()
    destfile = tmpdir.join('boo')

    rv = rl.call('remork.files', 'lineinfile', str(destfile), 'boo')
    assert rv.wait() == True
    assert destfile.read() == 'boo\n'

    rv = rl.call('remork.files', 'lineinfile', str(destfile), 'boo')
    assert rv.wait() == False
    assert destfile.read() == 'boo\n'


def test_process():
    rl, _rr = make_router_pair()
    rv = process.run_helper(rl, 'echo "boo"; exit 2')
    assert rv.wait() == (2, b'boo\n', b'')
