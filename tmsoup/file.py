import os
import time
from .util import do_commit

def delete_file_taggings(cursor, file_id):
    """Delete all taggings relating to a specific file_id

    Returns
    ========
    The number of affected rows (number of taggings removed)
    """
    cursor.execute('DELETE FROM file_tag WHERE file_id = ?', (file_id,))
    r = cursor.rowcount
    do_commit(cursor)
    return r


def file_info(path):
    """Given a path to a file, return a (dirname, file) tuple
    that could be used to lookup that file's id in `file` table.
    (using 'SELECT id FROM file WHERE directory=?, name=?')

    Notes
    ======
    The resultant path is absolute but not fully normalized
    (that is, no attempt is made to resolve symlinks).
    Use os.path.realpath() before passing the path to file_info,
    if that is what you need.
    """
    return os.path.split(os.path.abspath(path))


def file_mtime(path):
    """Return the file's mtime, as a string suitable for storing in the database.
    We cannot return a datetime object, because datetimes
    do not support nanosecond resolution.

    Trailing zeros are truncated, to match TMSU's implementation.
    If nanoseconds == 0, the decimal part is omitted entirely.
    """
    base = os.stat(path)
    t = time.gmtime(base.st_mtime)
    nano = base.st_mtime_ns % 1000000000
    if nano > 0:
        nano = str(nano)
        while nano[-1] == '0':
            nano = nano[:-1]
        nano = '.' + nano
    else:
        nano = ''

    return time.strftime('%Y-%m-%d %H:%M:%S' + nano, t)


def file_id(cursor, path):
    """Return the file.id of the given path.

    If the path is not yet tagged, return None.
    """
    dirname, filename = file_info(path)
    results = list(cursor.execute('SELECT id FROM file'
                  ' WHERE directory=? AND name=?', (dirname, filename)))
    if len(results) > 1:
        raise ValueError('Duplicate entry in file table')
    if results:
        return results[0][0]
    return None


def file_ids(cursor, paths):
    """Return a path: file.id map for the given paths."""
    # XXX is slow?
    return {p: file_id(cursor, p) for p in paths}


def dir_contains(path, querypath):
    """Given two paths, return whether querypath is inside path.

    This does not check whether either path exists on disk, it is
    purely a logical operation.

    Examples
    =========

    >>> dir_contains('/foo/bar', '/foo/bar/1/2/3/4')
    True

    >>> dir_contains('/foo/bar', '/foo/ba')
    False

    >>> dir_contains('/foo/bar', '/foo/bar')
    False
    """
    slashed = os.path.abspath(path) + os.path.sep
    querypath = os.path.abspath(querypath)
    return querypath.startswith(slashed)


def rename_path(cursor, oldpath, newpath, update_only=False):
    """Rename a path, updating the database to match.

    If the specified path is a directory, then info for all files
    contained in that directory branch will also be updated.

    Parameters
    ===========
    update_only     Do not modify the filesystem, only the database.
                    Useful when you need to fix the database to conform
                    to a rename that's already happened.

    Returns
    ========
    n               The number of `file` table rows affected.
                    1 for renaming a file, 1+N for renaming a directory,
                    where N is the number of tagged files/directories
                    within that directory, recursively.
                    (Note that N is specifically -not- the number of files
                    /directories within that directory; only the ones that
                    are currently tagged.)

    Raises
    =======
    KeyError        If there is no record of this path in the database.
    OSError         If the database records that the given path is a directory
                    but the OS says it is not, or vice versa.
    OSError         If the destination path doesn't exist (eg.
                    you are trying to rename /foo/bar.png to /foo/baz/bar.png,
                    but /foo/baz doesn't exist.)
    FileNotFoundError   If oldpath doesn't exist on disk, and you haven't
                        specified update_only=True.


    """
    if (not update_only) and (not os.path.exists(oldpath)):
        raise FileNotFoundError(oldpath)

    oldpath = os.path.abspath(oldpath)
    newpath = os.path.abspath(newpath)
    isdir = os.path.isdir(oldpath)
    db_isdir = cursor.execute('SELECT is_dir FROM file'
                              ' WHERE directory = ? AND name = ?',
                              file_info(oldpath)).fetchone()
    if db_isdir:
        db_isdir = db_isdir[0]
    else:
        db_isdir = isdir

    if isdir != db_isdir:
        raise OSError('OS reports isdir=%r,'
                      ' but database reports isdir=%r' % (isdir, db_isdir))

    if isdir:
        if not os.path.exists(os.path.dirname(newpath)):
            raise OSError('Attempt to move {} into a nonexistent'
                          ' directory {} with name {}'.format(
                              oldpath,
                              os.path.dirname(newpath),
                              os.path.basename(newpath)))
        id = cursor.execute('SELECT id FROM file'
                            ' WHERE directory=? AND name = ?',
                            file_info(oldpath)).fetchone()
        if id:
            id = id[0]
        pattern = oldpath + os.path.sep + '%'
        idmap = {}
        for id, directory in cursor.execute('SELECT id, directory FROM file'
                                            ' WHERE directory=?'
                                            ' OR directory like ?',
                                            (oldpath, pattern)):

            if dir_contains(oldpath, directory):
                tmp = directory.split(oldpath, 1)
                if len(tmp) != 2:
                    raise ValueError('Attempted to split %r by %r,'
                                     ' but got %r!' % (directory,
                                                       oldpath,
                                                       tmp))
                tmp[0] = newpath
                rewritten = "".join(tmp)
                import sys
                print ('%r -> %r' % (directory, rewritten), file=sys.stderr)
                idmap[id] = (directory, rewritten)
            elif directory == oldpath:
                idmap[id] = (oldpath, newpath)
        print ('idmap: %r' % (idmap,))
        # XXX actually make changes.
    else:
        import sys
        sys.exit(1)
        # blocked off for now.
        id = cursor.execute('SELECT id FROM file'
                            ' WHERE directory=? AND name = ?',
                            file_info(oldpath)).fetchone()
        if not id:
            raise KeyError('No record referring to %r found.' % (oldpath,))
        id = id[0]
        if not update_only:
            newdir = os.path.dirname(newpath)
            if not os.path.exists(newpath):
                raise OSError('Attempt to move {} into a nonexistent'
                              ' directory {}'.format(oldpath, newdir))
            os.rename(oldpath, newpath)
        cursor.execute('UPDATE file SET directory=?, name=? WHERE id=?',
                       file_info(newpath) + (id,))
        do_commit()


def move_paths(cursor, paths, destdir):
    """Move all `paths` to destdir, updating database accordingly.

    Files that are not currently tagged will just be moved, with no updates to
    the database.

    Raises
    =======
    ValueError      If a path points to a directory, but other paths point at
                    files inside that directory.
                    move_files() does not do recursive moves,
                    use rename_path() on the parent directory for that.

    """
    raise NotImplementedError('tmsoup.file.move_paths()')

def parse_args(args):
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Rename or get info about files/directories')
    subp = parser.add_subparsers(dest='cmd')
    subp.required = True
    rename = subp.add_parser('rename',
                             help='Rename a file or directory')
    rename.add_argument('oldname')
    rename.add_argument('newname')
    return parser.parse_args(args)

def main(argv):
    import sqlite3
    from tmsoup.core import get_db_path
    args = parse_args(argv)
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    if not os.path.isdir(args.oldname):
        raise ValueError('directories only, for now.')
    rename_path(cursor, args.oldname, args.newname)

__all__ = ('delete_file_taggings', 'file_id', 'file_ids', 'file_mtime',
           'file_info')

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
