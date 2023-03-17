# Remork

Accelerator for testinfra. Remork allows to greatly speedup testinfra
operations by exploiting single ssh or docker (possibly paramiko) connection.

It provides own extensions to upload files, change files on remote
host and other configuration management tasks.

## TODO

* Inventory.

* File change transactions as a sane way to apply set of configuration changes,
  validate configuration as a whole (like complex nginx config with multiple
  templates) and revert all files back if anything goes wrong.

* Ansible vault compatible encryption/decryption.

* Roles and ability to target roles from CLI runner.
