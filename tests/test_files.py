from remork import files
from textwrap import dedent


def test_blockinfile(tmpdir):
    f = tmpdir.join('boo')

    assert files.blockinfile(str(f), '# boo', 'line1')
    assert f.read() == dedent('''\
        # boo REMORK BLOCK START
        line1
        # boo REMORK BLOCK END
    ''')

    assert not files.blockinfile(str(f), '# boo', 'line1')
    assert f.read() == dedent('''\
        # boo REMORK BLOCK START
        line1
        # boo REMORK BLOCK END
    ''')

    assert files.blockinfile(str(f), '# bar', 'line2')
    assert f.read() == dedent('''\
        # boo REMORK BLOCK START
        line1
        # boo REMORK BLOCK END
        # bar REMORK BLOCK START
        line2
        # bar REMORK BLOCK END
    ''')
