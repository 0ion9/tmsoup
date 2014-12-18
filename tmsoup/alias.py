#!/usr/bin/env python3
"""Alias API + CLI tool for TMSU
"""

import os
import sys
import argparse
from .core import (get_db_path, validate_name,
                   tag_names, rename_tag,
                   KeyExists, tag_id, register_hook, resolve_tag_value)
from .util import do_commit

_DB_PATH = get_db_path()
_autoremoved_aliases = {}


def alias_tag_was_deleted(cursor, tagname):
    tid = tag_id(tagname)
    aids = set([v[0]
                for v in cursor.execute(
                    'SELECT DISTINCT alias_id'
                    'FROM alias_tag WHERE tag_id = ?').fetchall()])
    if aids:
        _autoremoved_aliases.update({k: " ".join(sorted(v[2])) for k, v in
                                     describe_aliases(cursor).items()
                                     if v[1] in aids})


def init(cursor):
    c = cursor
    c.execute('CREATE TABLE IF NOT EXISTS'
              ' alias (id INTEGER PRIMARY KEY,'
              ' name TEXT NOT NULL,'
              ' CONSTRAINT con_alias_name_unique UNIQUE(name))')
    c.execute('CREATE TABLE IF NOT EXISTS'
              ' alias_tag (alias_id INTEGER NOT NULL,'
              ' tag_id INTEGER NOT NULL,'
              ' value_id INTEGER NOT NULL,'
              ' FOREIGN KEY (alias_id) REFERENCES alias(id),'
              ' FOREIGN KEY (tag_id) REFERENCES tag(id),'
              ' FOREIGN KEY (value_id) REFERENCES value(id))')
    register_hook('after-tag-delete', alias_tag_was_deleted)


def db_connect(path=None):
    import sqlite3
    conn = sqlite3.connect(path or _DB_PATH)
    return conn, conn.cursor()


def msg(*args):
    print(*args, file=sys.stderr)


def alias_names(cursor):
    return sorted(v[0] for v in cursor.execute('select name from alias'))


def alias_id_map(cursor):
    return dict(cursor.execute('select name, id from alias'))


def resolve_aliases(cursor, items):
    amap = alias_id_map(cursor)
    _alias_names = set(amap.keys())
    c = cursor
    aliased_items = set(_alias_names).intersection(items)

    if not aliased_items:
        return items

    unaliased_items = set(items).difference(aliased_items)
    added_taggings = set()

    for a in aliased_items:
        dest_taggings = list(c.execute('SELECT tag_id, value_id'
                                       ' FROM alias_tag WHERE'
                                       ' alias_id = ?', (amap[a],)))
        added_taggings.update(dest_taggings)

    string_taggings = {resolve_tag_value(c, t, v) for t, v in added_taggings}
    string_taggings.update(unaliased_items)
    return sorted(string_taggings)


def alias_id(cursor, name):
    return cursor.execute('select id from alias'
                          ' where name = ?', (name,)).fetchone()[0]


def check_names(cursor, *names, alias_conflict=True, tag_conflict=True):
    _alias_names = set(alias_names(cursor))
    _tag_names = set(tag_names(cursor))
    if alias_conflict:
        conflicting_aliases = _alias_names.intersection(names)
        if conflicting_aliases:
            raise KeyExists('alias' +
                            ('es' if len(conflicting_aliases) > 1 else ''),
                            " ".join(conflicting_aliases))
    if tag_conflict:
        conflicting_tags = _tag_names.intersection(names)
        if conflicting_tags:
            raise KeyExists('tag' +
                            ('s' if len(conflicting_tags) > 1 else ''),
                            " ".join(conflicting_tags))


def describe_aliases(cursor):
    """Return a dict name: (id, taggings) describing every defined alias."""
    c = cursor
    desc = {}
    alias_name = dict(c.execute('SELECT id, name FROM alias'))
    naliases = len(alias_name)

    for id, name in alias_name.items():
        pairs = list(c.execute('SELECT tag_id, value_id FROM alias_tag'
                               ' WHERE alias_id=?', (id,)))
        results = [resolve_tag_value(c, *v) for v in pairs]
        desc[name] = (id, set(results))

    return desc


def list_aliases(cursor, alias_filter=None, tag_filter=None, oneline=False):
    "Display a list of 'alias > tagging' mappings,"
    " optionally filtered by glob patterns."
    # XXX collapse aliases with matching RHS,
    # eg.
    #
    # foo -> bar
    # fool -> bar
    # >>>>
    # foo, fool -> bar
    #

    import fnmatch
    descriptions = describe_aliases(cursor)
    for name, v in sorted(descriptions.items()):
        id, tagnames = v
        show = True

        if alias_filter and not fnmatch.fnmatch(name, alias_filter):
            show = False
        elif tag_filter and not any(fnmatch.fnmatch(t, tag_filter)
                                    for t in tagnames):
            show = False

        if show:
            if oneline:
                print(name)
            else:
                print("%20s    > %-20s" % (name, " ".join(tagnames)))


def alias_tuples(cursor, name, tags):
    """Return (tag_id, value_id) tuples for insertion of the proposed alias.

    * all referenced tags exist
    * all referenced tag values exist [1]
    * the alias name doesn't already exist
    * the alias name is not identical to an existing tag name.

    Raises:
    ========

    * KeyExists if the alias name already exists or exactly matches a tag name
    * KeyError if a referenced tag or tag value does not exist.

    """
    c = cursor
    check_names(c, name)

#    print ('AN: %r' % _alias_names)
#    print ('TN: %s' % (" ".join(_tag_names)))
    tags = resolve_aliases(c, tags)
    tagval_pairs = []
    missing_values = set()
    missing_tags = set()
    for unparsed in tags:
        tmp = unparsed.split('=')

        if len(tmp) == 1:
            value_id = 0
        else:
            # XXX check value validity here
            value_id = c.execute('SELECT id FROM value'
                                 ' WHERE name = ?', (tmp[-1],)).fetchone()

            if value_id is None:
                missing_values.add(tmp[1])
            else:
                value_id = value_id[0]

        tag_id = c.execute('SELECT id FROM tag'
                           ' WHERE name = ?', (tmp[0],)).fetchone()

        if tag_id is None:
            missing_tags.add(tmp[0])
        else:
            tag_id = tag_id[0]

        tagval_pairs.append((tag_id, value_id))

    if missing_tags:
        raise KeyError('The following tags do not'
                       ' exist: %s' % (" ".join(sorted(missing_tags)),))

    if missing_values:
        raise KeyError('The following values do not'
                       ' exist: %s' % (" ".join(sorted(missing_values)),))

    return tagval_pairs


def add_alias(cursor, name, tags):
    validate_name(name)
    try:
        pairs = alias_tuples(cursor, name, tags)
    except Exception as e:
        print(e)
        raise
    c = cursor
    msg(name, ':', pairs)

    c.execute('insert into alias(name) values (?)', (name,))
    do_commit(cursor)
    new_alias_id = alias_id(c, name)
    msg('new alias id: %r' % new_alias_id)

    for tag_id, value_id in pairs:
        c.execute('insert into alias_tag (alias_id, tag_id, value_id)'
                  ' values (?, ?, ?)', (new_alias_id, tag_id, value_id))

    do_commit(cursor)


def check_aliases():
    conn = db_connect()
    c = conn.cursor()
    _alias_names = alias_names()
    tag_ids = {v[0] for v in c.execute('SELECT id FROM tag').fetchall()}
    value_ids = {v[0] for v in c.execute('SELECT id FROM value').fetchall()}
    value_ids.add(0)
    unknown_values = set()
    unknown_tags = set()
    bad_aliases = set()

    for name in _alias_names:
        validate_name(name)
        alias_id = get_alias_id(name)
        data = c.execute('SELECT tag_id, value_id'
                         ' FROM alias_tag WHERE alias_id=?', (alias_id,))
        data = list(data.fetchall())
        these_tag_ids = {d[0] for d in data}
        these_value_ids = {d[1] for d in data}
        missing_tags = these_tag_ids.difference(tag_ids)
        missing_values = these_value_ids.difference(value_ids)

        if missing_tags or missing_values:
            unknown_tags.update(missing_tags)
            unknown_values.update(missing_values)
            missing_tags = " ".join(str(v) for v in sorted(missing_tags))
            missing_values = " ".join(str(v) for v in sorted(missing_values))

            if missing_tags:
                missing_tags = 'Tags: ' + missing_tags

            if missing_values:
                missing_values = '; Values: ' + missing_values

            print('%s: Missing %-20s %22s' % (name,
                                              missing_tags,
                                              missing_values))
            bad_aliases.add(name)

    if unknown_tags or unknown_values:
        print('\n Summary:')
        unknown_tags = " ".join(str(v) for v in sorted(unknown_tags))
        unknown_values = " ".join(str(v) for v in sorted(unknown_values))
        affected_aliases = " ".join(sorted(bad_aliases))
        if unknown_tags:
            print('Unknown tags: %60s' % unknown_tags)
        if unknown_values:
            print('Unknown values: %60s' % unknown_values)


def copy_alias(cursor, name, *destnames):
    for dest in destnames:
        validate_name(dest)
    _alias_names = alias_names(cursor)

    if name not in _alias_names:
        raise KeyError('Attempt to copy nonexistent alias %r' % name)

    check_names(cursor, *destnames)
    tagset = resolve_aliases(cursor, [name])

    for newname in destnames:
        add_alias(cursor, newname, tagset)


def alias_away(cursor, oldname, newname, path=None):
    validate_name(newname)
    _alias_names = alias_names(cursor)
    _tag_names = tag_names(cursor)

    if oldname not in _tag_names:
        raise KeyError('Tried to alias away tag %r,'
                       ' but no such tag exists!' % oldname)

    if newname in _alias_names or oldname in _alias_names:
        raise KeyExists('alias', name)

    if newname in _tag_names:
        raise KeyExists('tag', name)

    return
    from subprocess import call

    rename_tag(cursor, oldname, newname)
    _tag_names = tag_names(cursor)

    if oldname in _tag_names or newname not in tag_names:
        raise ValueError('Renaming tag %r -> %r  failed!' %
                         (oldname, newname))

    add_alias(cursor, oldname, [newname])
    _alias_names = alias_names()

    if oldname not in _alias_names:
        raise KeyError('Tried to add an alias %r -> %r,'
                       ' but couldn\'t find it afterwards!' %
                       (oldname, newname))


def rename_alias(cursor, oldname, newname):
    from .core import rename
    _tag_names = tag_names(cursor)

    if newname in _tag_names:
        raise KeyExists('tag', newname)

    validate_name(oldname)
    validate_name(newname)
    rename(cursor, 'alias', oldname, newname)
    do_commit(cursor)


def delete_alias(cursor, name):
    c = cursor
    if not any(c.execute('SELECT name FROM alias WHERE name = ?', (name,))):
        raise KeyError('Tried to remove alias %r, but'
                       ' no such alias exists!' % name)

    _alias_id = alias_id(c, name)
    c.execute('delete from alias_tag where alias_id = ?', (_alias_id,))
    c.execute('delete from alias where id = ?', (_alias_id,))
    do_commit(cursor)


def unknown_symbols(symbols, reverse=False):
    """Return the set of all symbols that are not yet known.

    That is, symbols that do not match a tag name or alias name.
    """
    available_symbols = set(alias_names())
    available_symbols.update(tag_names())
    symbols_to_check = {s.split('=', 1)[0] for s in symbols}
    if reverse:
        return symbols_to_check.intersection(available_symbols)
    return symbols_to_check.difference(available_symbols)


def parse_args(args):
    parser = argparse.ArgumentParser(description=
                                     'Simple alias CLI tool for TMSU')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='Explain what is being done.')
    parser.add_argument('-D', '--database', default=None, type=str,
                        help='Use the specified database.')
    subp = parser.add_subparsers(dest='command')
    subp.required = True

    resolve = subp.add_parser('resolve',
                              help='Resolve any aliases'
                              ' found in a list of tags',
                              aliases=('res',))
    resolve.add_argument('name', type=str, nargs='+',
                         help='tag (with optional =value)'
                         ' and/or alias name(s)')

    add = subp.add_parser('add', help='Add an alias')
    add.add_argument('aliasname', type=str)
    add.add_argument('tagname', type=str, nargs='+')

    rm = subp.add_parser('remove', help='Remove an alias',
                         aliases=('rm',))
    rm.add_argument('aliasname', type=str, nargs='+')

    madd = subp.add_parser('multi_add',
                           help='Add multiple aliases pointing at'
                           ' a single set of tags')
    madd.add_argument('tags', type=str,
                      help='Tag set. Should usually be quoted,'
                      ' to allow multi-tag sets like "foo bar"')
    madd.add_argument('aliasname', type=str, nargs='+')

    away = subp.add_parser('away',
                           help='Alias a tag away. The tag is renamed,'
                           ' then an alias pointing from the old tag'
                           ' to the new tag is added.')
    away.add_argument('oldname', type=str)
    away.add_argument('newname', type=str)

    copy = subp.add_parser('copy', help='Copy an alias')
    copy.add_argument('source', type=str)
    copy.add_argument('dest', type=str, nargs='+')

    ren = subp.add_parser('rename', help='Rename an alias')
    ren.add_argument('oldname', type=str)
    ren.add_argument('newname', type=str)

    check = subp.add_parser('check',
                            help='Check the validity of all aliases,'
                            ' generating a command usable to'
                            ' remove any detected'
                            ' invalid aliases. Invalid aliases are aliases'
                            ' which point at tags or'
                            ' values that no longer exist.')

    _list = subp.add_parser('list', help='List currently defined aliases.',
                            aliases=('ls',))
    _list.add_argument('-n', '--name',
                       help='Glob pattern to filter alias name by',
                       default=None)
    _list.add_argument('-1', '--oneline',
                       help='Print only alias names, one per line',
                       default=False,
                       action='store_true')
    _list.add_argument('-a', '--aliased-to',
                       help='Glob pattern to filter alias output tagnames by.'
                       ' If 1+ of the tagnames matches the pattern, '
                       ' it will be listed in the output.',
                       default=None)
    known = subp.add_parser('known',
                            help='Filter a list of symbols,'
                                 ' display only the known symbols')
    known.add_argument('symbol', type=str, nargs='+')
    unknown = subp.add_parser('unknown',
                              help='Filter a list of symbols,'
                              ' display only the unknown symbols')
    unknown.add_argument('symbol', type=str, nargs='+')
    return parser.parse_args(args)


def uncomma(tags):
    """Convert a list of tags which may include
    single tags and foo,bar,baz groups of tags, into uniform
    single tags"""
    import re
    return re.split('[ ,]+', " ".join(tags))


def main(arguments, cursor=None):
    import re
    args = parse_args(arguments)
    c = args.command

    if args.database:
        print('using database %r' % args.database, file=sys.stderr)
    else:
        print('using default database', file=sys.stderr)

    if not cursor:
        print ('making own cursor')
        _, cursor = db_connect(args.database)
    init(cursor)

    if c in ('list', 'ls'):
        list_aliases(cursor, args.name, args.aliased_to, args.oneline)
    elif c == 'add':
        add_alias(cursor, args.aliasname, uncomma(args.tagname))
    elif c == 'multi_add':
        import re
        tagset = uncomma(args.tags)

        for aliasname in args.aliasname:
            add_alias(cursor, aliasname, tagset)
    elif c == 'rename':
        rename_alias(cursor, args.oldname, args.newname)
    elif c == 'copy':
        copy_alias(cursor, args.source, *args.dest)
    elif c in ('resolve', 'res'):
        # make sure any foo,bar,baz are properly handled
        # rather than treating them as a single tag/alias name.
        names = uncomma(args.name)
        print(" ".join(resolve_aliases(cursor, names)))
    elif c == 'away':
        alias_away(cursor, args.oldname, args.newname)
    elif c in ('remove', 'rm'):
        for name in args.aliasname:
            delete_alias(cursor, name)
    elif c == 'unknown':
        items = unknown_symbols(cursor, args.symbol)
        print(" ".join(sorted(items)))
    elif c == 'known':
        items = unknown_symbols(cursor, args.symbol, True)
        print(" ".join(sorted(items)))
    elif c == 'check':
        check_aliases(cursor)
    else:
        raise ValueError('Something weird happened,'
                         ' this line should never be reached.')



if __name__ == "__main__":
    # testing commit isolation
    import sqlite3
    from .core import get_db_path
    import tmsoup.util as u
    u.defer_commit = True
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    main(sys.argv[1:], cursor=cursor)
    # XXX adding an alias should not work, now -- it will not be committed,
    # until I do conn.commit() here
    conn.commit()

__all__ = ('list_aliases', 'remove_alias', 'add_alias', 'rename_alias',
           'alias_away', 'copy_alias', 'check_aliases',)
