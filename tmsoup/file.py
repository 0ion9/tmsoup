import os
import time

def delete_file_taggings(cursor, file_id):
    """Delete all taggings relating to a specific file_id

    Returns
    ========
    The number of affected rows (number of taggings removed)
    """
    cursor.execute('DELETE FROM file_tag WHERE file_id = ?', (file_id,))
    r = cursor.rowcount
    cursor.commit()
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
    """Return the file's mtime, in a string format suitable for storing in the file table."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(os.path.getmtime(path)))


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


__all__ = ('delete_file_taggings', 'file_id', 'file_ids', 'file_mtime',
           'file_info')
