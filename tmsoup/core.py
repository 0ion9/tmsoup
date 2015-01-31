import os
import sqlite3
from functools import partial
from .file import (delete_file_taggings, file_info, file_id, file_ids,
                  file_mtime)
from .tag import (create_tag, delete_tag, rename_tag, tag_names, tag_id,
                  id_tag_map, tag_id_map)
from .util import (rename, delete, validate_name, do_commit)


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


def get_db_path(path=None, basedir=None):
    """Make a best guess at correct db path,
    given a nominal path (which can be None), and an optional basedir.

    If path is not None, simply returns path.

    If path is None, basedir and its parents are searched
    for .tmsu/db files, in the same manner as git -- deepest-first.

    If no .tmsu/db files are found
    either the content of the TMSU_DB environment variable,
    or if that is unset, ~/.tmsu/default.db, is returned.

    Parameters
    ===========
    path        None or str
                Path to database
    basedir     None or str
                Base directory.
                If None, CWD is used.
                    When

    Notes
    ======
    For interactive use, set `basedir` thoughtfully. If your program (eg file browser)
    has a 'current directory', this is what you should pass as `basedir`.
    If it does not, but operates on one or more files, pass
    the os.path.dirname() of the first of those files as `basedir`.
    This includes use in CLI utilities.

    (see https://github.com/oniony/TMSU/issues/15)



    the contents of TMSU_DB environment variable,
    or ~/.tmsu/default.db, in that order.
    """
    if path:
        return path
    else:
        if not basedir:
            basedir = os.getcwd()
        path = os.path.abspath(basedir)
        parts = path.split(os.path.sep)
        candidate = os.path.sep + os.path.join(*(parts + ['.tmsu/db']))
        if os.path.isfile(candidate):
            return candidate
        for i in range(-1, -(len(parts) - 1), -1):
            candidate = os.path.sep + os.path.join(*(parts[:i] + ['.tmsu/db']))
            if os.path.isfile(candidate):
                return candidate
        return os.getenv('TMSU_DB',
                         os.path.expanduser('~/.tmsu/default.db'))


def relpath(database):
    """Return the path to relativize paths to for the given database path,
    or None if the final two path elements are not .tmsu/db"""
    path = os.path.abspath(database)
    if path == '.tmsu'+ os.path.sep + 'db' or path.endswith(os.path.sep + '.tmsu' + os.path.sep + 'db'):
        tmp = (os.path.sep.join(path.split(os.path.sep)[:-2]))
        if not tmp.startswith(os.path.sep):
            tmp = os.path.sep + tmp
        return tmp
    return None


def connect(database, *args, **kwargs):
    "Connect to database, initializing it if it doesn't exist."
    class TMSUConnection(sqlite3.Connection):
        _relpath = relpath(database)
        def normalize_path(self, p):
            if self._relpath:
                # this is the flying fuckup:
                print ('cwd is %r' % os.getcwd())
                p2 = os.path.abspath(p)
                # ^^^
                print ('relpath %r %r : %r -> %r' % (p, p2, self._relpath, os.path.relpath(p2, self._relpath)))
                return os.path.relpath(p2, self._relpath)
            else:
                return os.path.abspath(p)

    exists = os.path.exists(database)
    conn = sqlite3.connect(database, *args, factory=TMSUConnection, **kwargs)
    if not exists:
        conn.cursor().executescript("""
CREATE TABLE tag (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
CREATE INDEX idx_tag_name
           ON tag(name);
CREATE TABLE file (
                id INTEGER PRIMARY KEY,
                directory TEXT NOT NULL,
                name TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                mod_time DATETIME NOT NULL,
                size INTEGER NOT NULL,
                is_dir BOOLEAN NOT NULL,
                CONSTRAINT con_file_path UNIQUE (directory, name)
            );
CREATE INDEX idx_file_fingerprint
           ON file(fingerprint);
CREATE TABLE value (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                CONSTRAINT con_value_name UNIQUE (name)
            );
CREATE TABLE file_tag (
                file_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                value_id INTEGER NOT NULL,
                PRIMARY KEY (file_id, tag_id, value_id),
                FOREIGN KEY (file_id) REFERENCES file(id),
                FOREIGN KEY (tag_id) REFERENCES tag(id)
                FOREIGN KEY (value_id) REFERENCES value(id)
            );
CREATE INDEX idx_file_tag_file_id
           ON file_tag(file_id);
CREATE INDEX idx_file_tag_tag_id
           ON file_tag(tag_id);
CREATE INDEX idx_file_tag_value_id
           ON file_tag(value_id);
CREATE TABLE implication (
                tag_id INTEGER NOT NULL,
                implied_tag_id INTEGER NOT NULL,
                PRIMARY KEY (tag_id, implied_tag_id)
            );
CREATE TABLE query (
                text TEXT PRIMARY KEY
            );
CREATE TABLE setting (
                name TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
""")
    return conn


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
    do_commit(cursor)

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
    do_commit(cursor)
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
    do_commit(cursor)
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
    do_commit(cursor)
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
