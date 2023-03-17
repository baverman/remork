import os.path


def walkdir(top, root='', follow_symlinks=False):
    with os.scandir(top) as iterator:
        dirs = []
        for entry in iterator:
            if entry.is_dir():
                if not follow_symlinks and entry.is_symlink():
                    continue
                dirs.append(entry.name)
            else:
                yield os.path.join(root, entry.name)

        for d in dirs:
            yield from walkdir(os.path.join(top, d), os.path.join(root, d))
