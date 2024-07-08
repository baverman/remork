import io

def test_result_str(tmpdir, host):
    tmpdir.join('boo').write('foo')
    rv = host.run(f'ls -la {tmpdir}')
    assert 'boo' in str(rv)

    rv = host.run('echo foo >&2')
    assert 'foo' in str(rv)
    assert 'STDERR' in str(rv)


def test_upload(tmpdir, host):
    src = tmpdir.mkdir('sdir')
    dst = tmpdir.mkdir('ddir')

    src.join('file1').write('')
    src.join('file2').write('boo')
    ro = src.join('file3')
    ro.write('foo')
    ro.chmod(0o444)

    host.remork.upload(f'{src}/.', str(dst))

    assert dst.join('file1').read() == ''
    assert dst.join('file2').read() == 'boo'
    assert dst.join('file3').read() == 'foo'
    assert dst.join('file3').stat().mode & 0o700 == 0o400

    # repeat
    host.remork.upload(f'{src}/.', str(dst))
    assert dst.join('file1').read() == ''
    assert dst.join('file2').read() == 'boo'
    assert dst.join('file3').read() == 'foo'
    assert dst.join('file3').stat().mode & 0o700 == 0o400

    # recreate directories
    host.remork.upload(f'{src}', f'{dst}')
    assert dst.join('sdir/file1').read() == ''
    assert dst.join('sdir/file2').read() == 'boo'
    assert dst.join('sdir/file3').read() == 'foo'
    assert dst.join('sdir/file3').stat().mode & 0o700 == 0o400

    host.remork.upload(dest=f'{dst}/file', content='some-content')
    assert dst.join('file').read() == 'some-content'

    host.remork.upload(dest=f'{dst}/file', source=io.BytesIO(b'foo'))
    assert dst.join('file').read() == 'foo'

    host.remork.upload(dest=f'{dst}/some-dir/', source=f'{src}/file2')
    assert dst.join('some-dir/file2').read() == 'boo'

    host.remork.upload(dest=f'{dst}/some-file', source=f'{src}/file2')
    assert dst.join('some-file').read() == 'boo'
