_RESERVEDNAMES = set('. .. and or not eq ne lt gt le ge'.split(' '))
_RESERVEDNAMES.update({v.upper() for v in _RESERVEDNAMES})

defer_commit = False

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
    ValueError       when the name is not valid
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
    do_commit(cursor)
    cursor.execute('UPDATE ' + tablename +
                   'SET name = ? where name = ?',
                   (newname, oldname))

    if (conn.changes - oldchanges) != 1:
        conn.rollback()
        return False

    do_commit(cursor)
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
    do_commit(cursor)
    cursor.execute('DELETE FROM ' + tablename +
                   ' where name = ?',
                   (newname, oldname))

    # XXX rollback vs commit asymmetry..
    if conn.changes - oldchanges != 1:
        conn.rollback()
        return False

    do_commit(cursor)
    return True


def do_commit(cursor):
    if defer_commit:
        # don't commit yet, we are running in CLI automation mode
        import sys
        sys.stderr.write('.')
        return
    else:
        conn = cursor.connection
        conn.commit()
