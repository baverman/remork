import os
import sys
import pytest

from remork import client, process, router


def test_client():
    r = client.connect([sys.executable])
    rv = process.run_helper(r, 'echo foo; exit 1')
    assert rv.wait() == (1, b'foo\n', b'')


def test_broken_client():
    with pytest.raises(client.ConnectException):
        client.connect([sys.executable, '-m', 'non_existent'])

    r = client.connect([sys.executable], debug=2)
    os.kill(r.proc.pid, 15)
    rv = process.run_helper(r, 'echo foo; exit 1')

    with pytest.raises(router.ResultException) as ei:
        rv.wait()
    assert ei.match('Closed')

    rv = process.run_helper(r, 'echo foo; exit 1')
    assert rv.wait() == (1, b'foo\n', b'')
