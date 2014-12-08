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
    return dict(v[0] for v in cursor.execute('SELECT name, id'
                                             ' FROM tag').fetchall())


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


def rename_tag(cursor, oldname, newname):
    return rename(cursor, 'tag', oldname, newname)


def delete_tag(cursor, name):
    raise NotImplementedError('tag deletion')
    # XXX we also need to 'delete from file_tag where tag_id = ?'
    delete(cursor, 'tag', name)
    dispatch_hook('after-tag-delete')


__all__ = ('validate_name', 'get_db_path', 'tag_names', 'tag_values',
           'tag_id_map', 'id_tag_map', 'rename_tag', 'delete_tag',
           'register_hook', 'dispatch_hook', 'KeyExists')
