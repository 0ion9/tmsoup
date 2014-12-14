import os
from functools import partial

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


def validate_name(name):
    """Return if a name is valid per TMSU rules.

    The same rules are applied to tag names and tag value names:

    1 or more Letter, Number, Punctuation, or Symbol, excluding whitespace
    and the characters ,/=()<>.

    (Where Letter, Number, Punctuation, or Symbol-ness is defined by Unicode
    character category (L*, N*, P* or S* respectively))

    The names:
         . and or not eq ne lt gt le ge
         .. AND OR NOT EQ NE LT GT LE GE
    are also specifically disallowed.

    (note that that means that aND, Or, lT, etc.. are allowed)


    Raises
    =======

    ValueError when the name is not valid


    """
    import unicodedata

    if name in _RESERVEDNAMES:
        raise ValueError('%r conflicts with reserved symbol name' % name)

    invalid = {c for c in ' \t,/=()<>/' if c in name}
    invalid.update(c for c in name
                   if unicodedata.category(c)[0] not in 'LNPS')

    if invalid:
        invalid = "".join(sorted(invalid))
        raise ValueError('The following characters are'
                         ' not permitted in names: %s' % invalid)

    return


def tag_names(cursor):
    """Return a list of all tag names defined in this database"""
    return list(v[0] for v in cursor.execute('SELECT name FROM tag'
                                             ' ORDER BY name').fetchall())


def tag_values(cursor):
    """Return a list of all tag values defined in this database"""
    return list(v[0] for v in cursor.execute('SELECT name FROM value'
                                             ' ORDER BY name').fetchall())


def tag_id_map(cursor):
    "Return a dictionary mapping tag name to id,"
    " for all tag names defined by this database"
    return dict(cursor.execute('SELECT name, id FROM tag'))


def id_tag_map(cursor):
    "Return a dictionary mapping tag id to name,"
    " for all tag ids defined by this database"
    return {v: k for k, v in tag_id_map(cursor).items()}


def tag_id(cursor, name):
    "Return the id of the named tag"
    return cursor.execute('SELECT id FROM tag'
                          ' WHERE name = ?', (name,)).fetchone()[0]


def rename(cursor, tablename, oldname, newname):
    """Generic renaming for tables with a 'name' field and no name duplication.

    Returns
    ========

    True if the rename succeeded.
    """
    if newname == '':
        raise ValueError('New name cannot be empty')
    if not any(cursor.execute('select name from ' +
                              tablename + ' where name = ?',
                              (oldname,))):
        raise KeyError('Attempt to rename nonexistent %s %r' %
                       (tablename, oldname))

    if any(cursor.execute('select name from ' +
                          tablename + ' where name = ?',
                          (newname,))):
        raise KeyExists(tablename, newname)

    conn = cursor.connection
    oldchanges = conn.total_changes
    conn.commit()
    cursor.execute('UPDATE ' + tablename +
                   'SET name = ? where name = ?',
                   (newname, oldname))

    if (conn.changes - oldchanges) != 1:
        conn.rollback()
        return False

    conn.commit()
    return True


def delete(cursor, tablename, name):
    """Generic removal for tables with a 'name' field and no name duplication.

    Returns
    ========
    True if the removal succeeded (ie. exactly one row was removed.)

    """
    if not any(cursor.execute('select name from ' + tablename +
                              ' where name = ?',
                              (oldname,))):
        raise KeyError('Attempt to delete nonexistent %s %r' %
                       (tablename, oldname))

    conn = cursor.connection
    oldchanges = conn.total_changes
    conn.commit()
    cursor.execute('DELETE FROM ' + tablename +
                   ' where name = ?',
                   (newname, oldname))

    if conn.changes - oldchanges != 1:
        conn.rollback()
        return False

    conn.commit()
    return True


def create_tag(cursor, name, reuse_old=False):
    """Create a new tag, and return the tag id.

    Arguments
    ==========
    reuse_old   bool    Try to reuse ids of tags that have been deleted.
                        If this is True (default=False), the lowest ID > 1
                        that is not currently in use will be used.
                        If it is False, (current_highest_tag_id + 1)
                        will be used [default SQLite behaviour].

    Notes
    ======
    Using reuse_old=True obfuscates implicit information about the order
    in which tags have been created. If you want to use that information,
    avoid using this option.

    """
    if not name:
        raise ValueError('Tag name cannot be empty.')
    if len(list(
        cursor.execute('select id from tag where name = ?', (name,)))):
        raise ValueError('Tag name %r is already in use' % name)

    if reuse_old:
        used = set(cursor.execute('select id from tag'))
        maxv = max(used)
        possible = set(range(1, maxv+2))
        available = possible.difference(used)
        if not available:
            # This should never happen,
            # unless the TMSU db exceeds ((2**64)-1) unique tags.
            raise ValueError('Everyone out of the universe!')
        id = min(available)
        cursor.execute('insert into tag(id, name) values (?,?)', (id, name))
        cursor.connection.commit()
        actualid = cursor.execute('select id from tag where name = ?',
                       (name,)).fetchone()[0][0]
        if actualid != id:
            raise ValueError('Asked for unused id %r, but got %r' % (id, actualid))
    else:
        cursor.execute('insert into tag(name) values (?)', (name,))
        cursor.connection.commit()
        id = cursor.execute('select id from tag where name = ?',
                 (name,)).fetchone()[0][0]
    return id


def rename_tag(cursor, oldname, newname):
    """Rename tag from oldname to newname.
    """
    return rename(cursor, 'tag', oldname, newname)


def delete_tag(cursor, name):
    raise NotImplementedError('tag deletion')
    # XXX we also need to 'delete from file_tag where tag_id = ?'
    delete(cursor, 'tag', name)
    dispatch_hook('after-tag-delete')


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


def file_tags(cursor, paths):
    """Return path,  tags tuples for each of the specified paths.

    """
    # XXX could be faster -- query all paths in one go
    id_path_map = {file_id(cursor, p): p for p in paths}
    map = {}
    cursor.execute('CREATE TEMPORARY TABLE fileidtmp(id INTEGER)')
    idlist = ",".join(['(%d)' % v for v in sorted(id_path_map.keys())])
    cursor.execute('INSERT INTO fileidtmp VALUES %s' % idlist)
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

# useful for turning a path into a dirname, basename tuple.
splitpath = os.path.split

__all__ = ('validate_name', 'get_db_path', 'tag_names', 'tag_values',
           'tag_id_map', 'id_tag_map', 'rename_tag', 'delete_tag',
           'register_hook', 'dispatch_hook', 'KeyExists', 'file_id',
           'file_tags', 'tag_files', 'untag_files', 'delete_file_tag',
           'delete_file_taggings')
