import os
import sys
from testinfra import backend, host, modules
from testinfra.backend import ssh, docker
from testinfra.utils import cached_property
from testinfra.modules.base import InstanceModule

from remork import client, process, files, utils


class attrdict(dict):
    __getattr__ = dict.__getitem__


class RemorkBackendMixin(object):
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
        cmd, args = self._build_ssh_command('python')
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
        return client.connect('python', shell_cmd=shell)


class RemorkModule(InstanceModule):
    def upload(self, source=None, dest=None, content=None):
        assert dest
        assert source or content
        rv = None
        if source:
            if os.path.isfile(source):
                if dest[-1] == os.sep:
                    dest = dest + os.path.basename(source)
                if hasattr(source, 'read'):
                    rv = files.upload_file_helper(self._host.backend.router, dest, source=source)
                else:
                    with open(source, 'rb') as fd:
                        rv = files.upload_file_helper(self._host.backend.router, dest, source=fd)
            elif os.path.isdir(source):
                root = os.path.basename(source.rstrip(os.sep))
                fsource = source
                if root == '.':
                    root = ''
                else:
                    fsource = os.path.dirname(source)
                for fname in utils.walkdir(source, root):
                    sfname = os.path.join(fsource, fname)
                    rv = files.upload_file_helper(
                        self._host.backend.router,
                        os.path.join(dest, fname),
                        source=sfname, mode=os.stat(sfname).st_mode)
            else:
                raise OSError('only file and dir sources are supported')
        else:
            rv = files.upload_file_helper(self._host.backend.router, dest, content=content)

        if rv:
            rv.wait()

    def lineinfile(self, path, line):
        rv = self._host.backend.router.call('remork.files', 'lineinfile', path, line)
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


if os.environ.get('REMORK_OVERRIDE_BACKEND'):
    init(os.environ['REMORK_OVERRIDE_BACKEND'])
