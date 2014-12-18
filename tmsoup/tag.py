from .util import delete, rename, do_commit

def tag_names(cursor):
    """Return a list of all tag names defined in this database"""
    return list(v[0] for v in cursor.execute('SELECT name FROM tag'
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

def tag_exists(cursor, name=None, id=None):
    "Check for a tag's existence, either by name or id"

    if not name and not id:
        raise ValueError('One of (name, id) must be specified')

    if name:
        querypart = 'name = ?'
        variable = name
    else:
        querypart = 'id = ?'
        variable = id

    return cursor.execute('SELECT id FROM tag WHERE %s' % querypart,
                          (variable,)).fetchone() is not None


def discontiguous_tag_ids(cursor):
    "Return the set of all unused tag ids < the current max tag id value"
    used = set(v[0] for v in cursor.execute('select id from tag'))
    maxv = max(used)
    possible = set(range(1, maxv+2))
    available = possible.difference(used)
    return available


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
        available = discontiguous_tag_ids(cursor)
        if not available:
            # This should never happen,
            # unless the TMSU db exceeds ((2**64)-1) unique tags.
            raise ValueError('Everyone out of the universe!')
        id = min(available)
        cursor.execute('insert into tag(id, name) values (?,?)', (id, name))
        do_commit(cursor)
        actualid = cursor.execute('select id from tag where name = ?',
                       (name,)).fetchone()[0][0]
        if actualid != id:
            raise ValueError('Asked for unused id %r, but got %r' % (id, actualid))
    else:
        cursor.execute('insert into tag(name) values (?)', (name,))
        do_commit(cursor)
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


def parse_args(args):
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Rename or get info about tags')
    subp = parser.add_subparsers(dest='cmd')
    subp.required = True
    rename = subp.add_parser('rename',
                             help='Rename a tag')
    rename.add_argument('tagname')
    rename.add_argument('newname')
    stats = subp.add_parser('stats',
                            help='Show statistics about tags')
    return parser.parse_args(args)


if __name__ == "__main__":
    import sys
    import sqlite3
    from tmsoup.core import KeyExists, get_db_path
    from tmsoup.util import validate_name
    args = parse_args(sys.argv[1:])
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    if args.cmd == 'rename':
        if not tag_exists(cursor, name=args.tagname):
            print('Tag %r not found in database' % args.tagname,
                  file=sys.stderr)
            sys.exit(1)
        try:
            validate_name(cursor, args.newname)
        except ValueError as m:
            print(str(m), file=sys.stderr)
            sys.exit(1)
        try:
            rename_tag(cursor, args.tagname, args.newname)
        except KeyExists as m:
            print(str(m), file=sys.stderr)
            sys.exit(1)
    elif args.cmd == 'stats':
        dti = discontiguous_tag_ids(cursor)
        print('Discontiguous available tag id count: %d' % len(dti))

