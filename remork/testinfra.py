import os
import sys
import urllib.parse

from testinfra import backend, host, modules
from testinfra.backend import ssh, docker
from testinfra.modules.base import InstanceModule

try:
    from testinfra.utils import cached_property
except ImportError:
    from functools import cached_property


from remork import client, process, files, utils

AUTODETECT_PY = [
    'python',
    'python3',
    'python2',
    '/usr/libexec/platform-python',
]


class attrdict(dict):
    __getattr__ = dict.__getitem__


class RemorkBackendMixin(object):
    def __init__(self, *args, **kwargs):
        self.remork_config = {
            'python': kwargs.pop('remork_python', None),
        }
        super().__init__(*args, **kwargs)

    @cached_property
    def autodetected_python(self):
        rv = super().run('sh -c "{}"'.format(' || '.join(f'command -v {it}' for it in AUTODETECT_PY)))
        result = rv.stdout.strip()
        assert result, "Can't detect python interpeter: " + rv.stderr
        return result

    def run(self, command, *args, **kwargs):
        command = self.get_command(command, *args)
        rv = process.run_helper(self.router, command, **kwargs)
        exit_code, stdout, stderr = rv.wait()
        result = self.result(exit_code, command, stdout, stderr)
        return result


class SshBackend(RemorkBackendMixin, ssh.SshBackend):
    NAME = "remork+ssh"

    @cached_property
    def router(self):
        cmd, args = self._build_ssh_command(self.remork_config['python'] or self.autodetected_python)
        command = self.quote(' '.join(cmd), *args)
        return client.connect(command, double_quote=True)


class DockerBackend(RemorkBackendMixin, docker.DockerBackend):
    NAME = "remork+docker"

    @cached_property
    def router(self):
        if self.user:
            shell = self.get_command(
                'docker exec -i -u %s %s /bin/sh -c {cmd}', self.user, self.name)
        else:
            shell = self.get_command('docker exec -i %s /bin/sh -c {cmd}', self.name)
        return client.connect(self.remork_config['python'] or self.autodetected_python, shell_cmd=shell)


class RemorkModule(InstanceModule):
    def upload(self, source=None, dest=None, content=None):
        assert dest
        assert source or content
        items = []
        if source and hasattr(source, 'read'):
            items.append({'dest': dest, 'buf': source})
        elif source:
            if os.path.isfile(source):
                if dest[-1] == os.sep:
                    dest = dest + os.path.basename(source)
                items.append({'dest': dest, 'file': source, 'copymode': True})
            elif os.path.isdir(source):
                root = os.path.basename(source.rstrip(os.sep))
                fsource = source
                if root == '.':
                    root = ''
                else:
                    fsource = os.path.dirname(source)
                for fname in utils.walkdir(source, root):
                    sfname = os.path.join(fsource, fname)
                    items.append({'dest': os.path.join(dest, fname), 'file': sfname, 'copymode': True})
            else:
                raise OSError('only file and dir sources are supported')
        else:
            items.append({'dest': dest, 'content': content})

        rv = files.upload_files_helper(self._host.backend.router, items)
        rv.wait()

    def lineinfile(self, path, line):
        rv = self._host.backend.router.call('remork.files', 'lineinfile', path, line)
        return attrdict({'changed': rv.wait()})

    def blockinfile(self, path, marker, block):
        rv = self._host.backend.router.call('remork.files', 'blockinfile', path, marker, block)
        return attrdict({'changed': rv.wait()})


modules.modules['remork'] = 'remork:RemorkModule'
sys.modules['testinfra.modules.remork'] = sys.modules['remork.testinfra']


class HostMixin:
    def run_check(self, *args, **kwargs):
        return self.run_expect([0], *args, **kwargs)


host.Host.run_check = HostMixin.run_check

backend.BACKENDS[SshBackend.NAME] = 'remork.testinfra.SshBackend'
backend.BACKENDS[DockerBackend.NAME] = 'remork.testinfra.DockerBackend'
BACKENDS = ['ssh', 'docker']


def init(spec):
    backends = set(filter(None, (it.strip() for it in spec.split())))
    for it in backends:
        if it in BACKENDS:
            backend.BACKENDS[it] = backend.BACKENDS[f'remork+{it}']


def command_result_repr(self):
    def decode(data):
        if type(data) is type(''):
            return data
        return data.decode('utf-8', errors='ignore')

    def indent(text, prefix):
        return '\n'.join(prefix + it for it in text.splitlines())

    out = []
    if self._stdout_bytes:
        out.append('  STDOUT:\n' + indent(decode(self._stdout_bytes.rstrip()), '    '))
    if self._stderr_bytes:
        out.append('  STDERR:\n' + indent(decode(self._stderr_bytes.rstrip()), '    '))
    if out:
        out = [','] + out + ['']

    return (
        "CommandResult(command=%s, exit_status=%s%s)"
    ) % (
        repr(self.command),
        self.exit_status,
        '\n'.join(out),
    )

backend.base.CommandResult.__repr__ = command_result_repr


parse_host_spec_orig = backend.parse_hostspec
def parse_hostspec(hostspec):
    host, kw = parse_host_spec_orig(hostspec)

    if hostspec is not None and "://" in hostspec:
        url = urllib.parse.urlparse(hostspec)
        query = urllib.parse.parse_qs(url.query)
        for k, v in query.items():
            if k not in kw:
                kw[k] = v[0]
    return host, kw

backend.parse_hostspec = parse_hostspec


if os.environ.get('REMORK_OVERRIDE_BACKEND'):
    init(os.environ['REMORK_OVERRIDE_BACKEND'])
