import os
from functools import partial
from .file import (delete_file_taggings, file_info, file_id, file_ids,
                  file_mtime)
from .tag import (create_tag, delete_tag, rename_tag, tag_names, tag_id,
                  id_tag_map, tag_id_map)
from .util import (rename, delete, validate_name)

_RESERVEDNAMES = set('. .. and or not eq ne lt gt le ge'.split(' '))
_RESERVEDNAMES.update({v.upper() for v in _RESERVEDNAMES})


class KeyExists(Exception):
    def __init__(self, keyclass, name):
        self.keyclass = keyclass
        self.name = name

    def __str__(self):
        return '%s : a %s named %s already exists.' %\
        (self.__class__.__name__, self.keyclass, self.name)


_registry = {}


def register_hook(role, callback):
    """Register a callback hook.

    Valid roles:

    after-tag-delete
    """
    if role not in _registry:
        _registry[role] = []
    if callback not in _registry[role]:
        _registry[role].append(callback)


def dispatch_hook(role, *args, **kwargs):
    """Call all registered callback hooks for the specified role.
    """
    for callback in _registry.get(role, []):
        callback(*args, **kwargs)


def get_db_path(path=None):
    """Make a best guess at correct db path,
    given a nominal path (which can be None).

    Selects the path itself,
    the contents of TMSU_DB environment variable,
    or ~/.tmsu/default.db, in that order.
    """
    return path or os.getenv('TMSU_DB',
                             os.path.expanduser('~/.tmsu/default.db'))


def tag_values(cursor):
    """Return a list of all tag values defined in this database"""
    return list(v[0] for v in cursor.execute('SELECT name FROM value'
                                             ' ORDER BY name').fetchall())


def resolve_tag_value(cursor, tagid, valueid):
    if valueid == 0:
        return cursor.execute('SELECT name FROM tag WHERE id = ?',
                              (tagid,)).fetchone()[0]
    else:
        tmp = cursor.execute('SELECT T.name,V.name FROM tag AS T,'
                             ' value AS V where T.id=? and V.id=?',
                             (tagid, valueid)).fetchall()[0]
        return '%s=%s' % tmp



def file_tags(cursor, paths):
    """Return path,  tags tuples for each of the specified paths/ids.

    """
    # XXX could be faster -- query all paths in one go
    id_path_map = {file_id(cursor, p): p for p in paths}
    map = {}
    cursor.execute('CREATE TEMPORARY TABLE fileidtmp(id INTEGER)')
    idlist = sorted(id_path_map.keys())

    for start in range(0, len(idlist) + 1, 499):
        tmp = idlist[start:start+499]
        if tmp:
            cursor.execute('INSERT INTO fileidtmp VALUES {}'.format(
                ", ".join("(%d)" % v for v in tmp)))
    for fid, tid, vid in cursor.execute('SELECT F.file_id,'
                             ' F.tag_id,'
                             ' F.value_id'
                             ' FROM file_tag AS F JOIN fileidtmp AS I'
                             ' ON F.file_id = I.id'):
        map.setdefault(id_path_map[fid],[]).append((tid, vid))
    cursor.execute('DROP TABLE fileidtmp')
    return map



def tag_files(cursor, fids, taggings):
    """Tag each of the specified files (must already be known to TMSU)
    with the given taggings -- (tag_id, value_id) pairs.

    """
    all_values = []
    for fid in fids:
        all_values.extend((fid, tid, vid) for tid, vid in taggings)
    all_values = ",".join("(%d,%d,%d)" % v for v in all_values)
    print(all_values)
    cursor.execute('replace into file_tag(file_id, tag_id, value_id)'
        ' values %s' % all_values)
    cursor.connection.commit()

def untag_files(cursor, fids, taggings):
    """Remove tags from each of the specified files

    Raises
    =======
    IOError      When the number of effected rows > (len(fids) * len(taggings))
    ValueError   When the input is invalid

    """

    if any(type(v) != int for v in fids):
        raise ValueError('All file ids must be integers, not [' +
            ("".join(str(type(v)) for v in fids if type(v) != int)) + "]")

    if any(len(v) != 2 for v in taggings):
        raise ValueError('All taggings must be 2-tuples (tagid, valueid),'
            ' not %r' % ([v for v in taggings if len(v) != 2],))

    cursor.execute('CREATE TEMPORARY TABLE fileidtmp(id INTEGER)')
    idlist = ",".join(['(%d)' % v for v in fids])
    cursor.execute('INSERT INTO fileidtmp VALUES %s' % idlist)
    max_affected = len(fids) * len(taggings)
    # XXX this is not as well error-checked as TMSU's code
    # (storage/database/filetag.go:DeleteFileTag())
    cursor.commit()
    total_affected = 0
    for params in taggings:
        cursor.execute('DELETE FROM file_tag'
            ' WHERE file_id IN (select * from fileidtmp)'
            ' AND tag_id = ? AND value_id = ?)', params)
        if cursor.rowcount > max_affected:
            cursor.rollback()
            raise IOError('Too many rows (%d > max %d) affected'
                ' by deletion' % (cursor.rowcount, max_affected))
        total_affected += cursor.rowcount
    cursor.execute('DROP TABLE fileidtmp')
    cursor.connection.commit()
    return total_affected


def delete_file_tag(cursor, tag_id):
    """Delete all usages of the given tag from the file_tag table.

    Normally used just prior to deleting the record of the tag from the tag
    table.

    Returns
    ========
    The number of affected rows
    """
    cursor.execute('DELETE FROM file_tag WHERE tag_id = ?')
    r = cursor.rowcount
    cursor.commit()
    return r



# useful for turning a path into a dirname, basename tuple.
splitpath = os.path.split

def _add_database_option(parser):
    parser.add_argument('-D', '--database', default=None, type=str,
                        help='Use the specified database.')


__all__ = ('validate_name', 'get_db_path', 'tag_names', 'tag_values',
           'tag_id_map', 'id_tag_map', 'rename_tag', 'delete_tag',
           'register_hook', 'dispatch_hook', 'KeyExists', 'file_id',
           'file_ids', 'file_tags', 'tag_files', 'untag_files',
           'delete_file_tag',
           'delete_file_taggings', 'splitpath', 'resolve_tag_value')
