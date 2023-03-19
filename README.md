# Remork

Accelerator for testinfra. Remork allows to greatly speedup testinfra
operations by exploiting single ssh or docker connection.

It provides own extensions to upload files, change files on remote
host and other configuration management tasks.

## Installation

```
pip install remork
```

## Usage

Import `remork.testinfra` somewhere in `conftest.py`:

```python
import remork.testinfra
```

After that you can use hostspecs like `remork+ssh://host` or
`remork+docker://container`.

Additionally you can override default backends using `remork.testinfra.init`
call:

```python
import remork.testinfra
remork.testinfra.init('ssh docker')
```

It allows to use unmodified `ssh://` and `docker://` hostspecs.


## Custom python interpreter path

Remork relies on availability of `python` executable (any python >= 2.6 would work).
But recent RHEL based distros come without system wide python and provide platform
python only (`/usr/libexec/platform-python`).

You can pass `remork_python` param with hostspec:
`remork+ssh://host?remork_python=/usr/libexec/platform-python`).

Or pass additional options into get_host:
`testinfra.get_host(spec, remork_python='/usr/libexec/platform-python')`


## TODO

* Inventory.

* File change transactions as a sane way to apply set of configuration changes,
  validate configuration as a whole (like complex nginx config with multiple
  templates) and revert all files back if anything goes wrong.

* Ansible vault compatible encryption/decryption.

* Roles and ability to target roles from CLI runner.

* Ansible module to call remork files.

* Use remork router with paramiko backend.

