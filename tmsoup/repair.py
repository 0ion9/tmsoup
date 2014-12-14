import os
import glob
import re
import sys
from .core import splitpath, KeyExists, file_mtime, file_ids, file_tags, resolve_tag_value
from .fingerprint import set_fingerprint_algorithm, get_fingerprint_algorithm, fingerprint

INVALID_SIZE = -1

def unknown_paths(cursor):
    """Generate a list of all paths referenced in the database that cannot be found on disk.

    Note
    =====
    This only checks the existence of paths.

    """
    for dir, name in cursor.execute('select directory, name from file'):
        p = os.path.join(dir, name)
        if not os.path.exists(p):
            yield p


def invalidated_paths(cursor):
    """Generate all paths whose fingerprint is invalid.

    Invalidated paths' recorded size does not match their on-disk size,
    or their recorded mtime does not match their on-disk mtime.

    Fingerprints are normally invalidated for the entire database
    during modification of the fingerprint algorithm.

    invalidated_paths() returns only paths that exist on disk.
    Use unknown_paths() if you need a list of paths that do not.
    """
    for dir, name, size, mtime in cursor.execute('select directory, name, size, mod_time'
        ' from file'):
        p = os.path.join(dir, name)
        try:
            realsize = 0 if os.path.isdir(p) else os.path.getsize(p)
        except FileNotFoundError:
            continue
        if realsize != size:
            yield p
            continue
        realmtime = file_mtime(p)
        if realmtime != mtime:
            yield p


def duplicated_paths(cursor):
    """Return a fingerprint: paths map of duplicated paths (paths sharing the same fingerprint)

    Note
    =====
    Some of the paths may refer to files that no longer exist. It is left as an exercise for the
    caller to choose whether to filter out sets where only 1 path in the set actually exists.
    """
    from collections import Counter
    counter = Counter()

    counter.update(v[0] for v in cursor.execute('select fingerprint from file'))
    duplicated = {fp for fp, count in counter.items() if count > 1 and fp}
    print('ndupes = %d' % len(duplicated), file=sys.stderr)
    duplicates = {}
    for dir, name, fingerprint in cursor.execute('select directory,'
        ' name, fingerprint from file'):
        if fingerprint in duplicated:
            p = os.path.join(dir, name)
            group = duplicates.setdefault(fingerprint, [])
            group.append(p)
    return duplicates


# some example fd scores:
#
# 'foo bar' -> 3*3*2 -> 18
# 'foobar' -> 6*6 -> 36
# 'cereal bar' -> 6*6 + 3*3 -> 54
# 'foo baz baz' -> 3*3 * 3 -> 27
# '1d0f3fe45350f09bbf0453,foo,bar' -> 3*3 + 3*3 + 3*3 -> 27
#
def _filename_descriptiveness(filename):
    filename = os.path.splitext(filename)[0]
    words = re.findall('\\b[A-Za-z][a-z]{2,30}\\b', filename)
    return sum(len(v) * len(v) for v in words)

# A linearly interpolated function, peaking at c=30 -> 2.0 multiplier and clipped on each end

def _filecount_multiplier(c):
    if c <= 10:
        return max(0.1, 1.3 * (c / 10))
    elif c <= 30:
        return 1.3 + (0.7 * ((c - 10) / 20))
    else:
        return 2.0 - (1.75 * (min(c - 30, 170) / 170))


def value_of_duplicate(path, path_cache = {}):
    """Ranks a duplicate, returning a score value for it.

    Ranking is based on:
    * number of other files in the directory
      * files in subdirectories are not counted, as they generally do not aid in categorizing the file.
    * filename descriptiveness
      * score increases with longer words
      * score increases with more words
      * words < 2 characters are ignored
      * the immediate parent directory factors into score. Directories above that do not.
    """
    dirname = os.path.dirname(path)
    if dirname in path_cache:
        nfiles = path_cache[dirname]
    else:
        nfiles = sum(1 for p in glob.glob(os.path.join(dirname, '*')) if not os.path.isdir(p))
        path_cache[dirname] = nfiles
    base = os.path.basename(path)
    if '?' in base:
        base = base.split('?', 1)[0]
    measured_part = os.path.join(os.path.basename(dirname), os.path.basename(path))
    filename_value = _filename_descriptiveness(measured_part)
    return filename_value * _filecount_multiplier(nfiles)



def rank_duplicates(dupes):
    """Order the filenames in a fingerprint:filenames duplicates-map.
    Return a new map with the updated orders.

    Ordering is by 'categorizability' and 'filename quality'.
    """
    return {k: sorted(v, key = value_of_duplicate) for k, v in dupes.items()}



def repair_path(cursor, oldpath, newpath, ignore_fingerprint=False):
    """Repair a single path in the database.

    Oldpath is changed to newpath.

    Raises
    =======
    FileNotFoundError    if newpath does not exist
    ValueError           if the recorded hash for oldpath does not
                         match the calculated hash for newpath
                         (unless ignore_fingerprint=True)
    ValueError           if the file type of newpath differs from
                         the recorded file type of oldpath
    KeyExists            if a record referring to newpath already exists.
    KeyError             if there is no record for oldpath in the database.
    """
    if not os.path.exists(newpath):
        raise FileNotFoundError(newpath)
    odirname, obasename = splitpath(oldpath)
    tmp = cursor.execute('select id, is_dir, fingerprint from file'
                         ' where directory = ?, name = ?',
                         (odirname, obasename)).fetchone()

    if tmp is None:
        raise KeyError('No `file` record found for path %r' % oldpath)

    ndirname, nbasename = splitpath(newpath)
    if cursor.execute('select is_dir from file where directory = ?, name = ?',
                      (ndirname, nbasename)).fetchone():
        raise KeyExists('file', newpath)

    id, isdir, fp = tmp[0]
    isdir = (isdir != 0)
    new_isdir = os.path.isdir(newpath)
    if new_isdir != isdir:
        raise ValueError('Recorded isdir={} does not match'
                         'isdir={} of new path {}'.format(isdir,
                             new_isdir,
                             newpath))

    new_fp = fingerprint(cursor, get_fingerprint_algorithm(cursor))
    if new_fp != fp:
        raise ValueError('Recorded hash {} does not match hash {}'
                         ' of new path {}'.format(fp, new_fp, newpath))
    cursor.execute('UPDATE file SET directory = ?, name = ? WHERE id = ?',
        (ndirname, nbasename, id))

    assert (type(fp) == str)


def merge_files(cursor, main_file_id, *dupes):
    """Merge file ids specifieds in *dupes with main_file_id.



    """
    pass

def change_fingerprint_algorithm(cursor, algorithm):
    """Change the configured fingerprint algorithm, and invalidate existing fingerprints."""
    try:
        set_fingerprint_algorithm(cursor, algorithm)
    except ValueError:
        cursor.connection.rollback()
    cursor.execute('UPDATE file SET size = -1')
    cursor.connection.commit()


def parse_args(args):
    from tmsoup.core import _add_database_option, get_db_path
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Repair or check duplicates in TMSU database')
    _add_database_option(parser)
    subp = parser.add_subparsers(dest='cmd')
    subp.required = True
    resolve = subp.add_parser('dupes',
                              help='Find and resolve duplicate files.'
                              ' During resolution, the taggings on each file'
                              ' in a set of duplicates is tracked. '
                              ' When the set is reduced to 1 member, '
                              ' via deletion, taggings from the other'
                              ' members are merged into the taggings of the'
                              ' remaining member.')
    group = resolve.add_mutually_exclusive_group()
    group.add_argument('-H', '--keep-highest',
                         help='Rather than interactively deciding what to'
                         ' delete, select the member with the highest score'
                         ' (categorizability * descriptiveness of filename)'
                         ' and remove all the other items in each set')
    group.add_argument('-c', '--command',
                         help='Invoke this command to interactively'
                         ' remove duplicates. It should include a function'
                         ' to delete files from disk.',
                         default=None)
    group.add_argument('-s', '--stats',
                         help='Show stats about duplicates')

    resolve.add_argument('-1', '--single',
                         default=False, action='store_true',
                         help='Invoke the command once per set of dupes,'
                         ' rather than accumulating several sets in a batch')
    resolve.add_argument('-i', '--stdin',
                         default=False, action='store_true',
                         help='Pass file list to command via stdin, '
                         'rather than via commandline arguments.')
    resolve.add_argument('-m', '--minimum',
                         default=2, type=int,
                         help='Minimum items that actually exist in'
                         ' a set of files, before that set will be considered'
                         ' for interactive processing. Default %(default)d.'
                         ' Use 0 if you want all sets, even ones that consist'
                         ' solely of deleted files.'
                         )

    parser.set_defaults(limit=300)
    args = parser.parse_args(args)
    args.database = get_db_path(args.database)
    if not args.command and (args.single or args.stdin):
        print ('--single and --stdin require --command')
        import sys
        sys.exit(1)
    if args.stdin:
        args.limit = 0xffff
    return args


def _msg(*args,**kwargs):
    import sys
    print(*args, file=sys.stderr, **kwargs)

def _interactive_duplicate_removal(cursor, command, limit, minimum=2):
    from itertools import chain

    def removed(files):
        return [f for f in files if not os.path.exists(f)]

    def invoke(buffer):
        from plumbum import local
        files = list(chain(*(v[1] for v in buffer)))
        cmd = local[command[0]]
        print(cmd)
        cmd = cmd[command[1:]]
        _msg('files[0] = {}'.format(files[0]))
        cmd = cmd << ("\n".join(files))
        cmd()
        gone = removed(files)
        if gone:
            for item in gone:
                id = fidmap[item]
                q = tagging_queue.setdefault(hashmap[item], set())
                q.update(taggings[id])
            untagging_queue.update(gone)

    def formatted_taggings(taggings):
        return " ".join(resolve_tag_value(cursor, *v) for v in taggings)

    def apply_queued_taggings():
        for hash, toapply in tagging_queue:
            members = [f for f in dupes[hash] if os.path.exists(pathmap[f])]
            if not members:
                raise ValueError('set reduced to 0 members(?)')
            target = members[-1]
            tag_files(cursor, [target], toapply)
            _msg('Tagged file {} () : {}'.format(target,
                 pathmap[target], formatted_taggings(toapply)))
        for deadid in untagging_queue:
            _msg('Untagging file {}: {}'.format(deadid, pathmap[deadid]))
            delete_file_taggings(cursor, deadid)


    untagging_queue = set()
    tagging_queue = {}
    _msg('getting dupes')
    dupes = {k: v for k,v in duplicated_paths(cursor).items()
             if sum(1 for v2 in v if os.path.exists(v2)) >= minimum}
    _msg('ranking')
    dupes = rank_duplicates(dupes)
    # ranking is by number of -actual- dupes, not counting items that don't
    # exist on-disk.
    dupes_by_setsize = sorted(dupes.items(),
                              key=lambda v:
                                  sum(1 for v2 in v[1] if os.path.exists(v2)),
                              reverse=True)
    hashmap = {}

    _msg('making hashmap')
    for hash, members in dupes.items():
        for m in members:
            hashmap[m] = hash

    alldupes = set(chain(*dupes.values()))
    _msg('getting fids')
    fidmap = file_ids(cursor, alldupes)
    pathmap = {v:k for k,v in fidmap.items()}
    _msg('ndupes {}; nsets {}'.format(len(alldupes), len(dupes)))
    _msg('nfidmap {}'.format(len(fidmap)))
    _msg('nfidmap-null {}'.format(sum(1 for k,v in fidmap.items() if v is None)))
    #_msg(list(fidmap.items())[:2])

    _msg('getting current taggings')
    _msg('fidmap keys sample: {}'.format(list(fidmap.keys())[:8]))
    taggings = {fidmap[k]: v
                for k,v in file_tags(cursor, fidmap.keys()).items()}
    _msg('Average {} taggings'.format(
        sum(len(v) for v in taggings.values()) / len(taggings)))
    _msg('taggings sample {}'.format(
        list(taggings.items())[:10]))
    it = iter(dupes_by_setsize)
    buffer = [next(it)]
    total = sum(len(v[1]) for v in buffer)

    try:
        while total < limit:
            readied = next(it)
            if total + len(readied[1]) > limit:
                invoke(buffer)
                buffer = [readied]
            else:
                buffer.append(readied)

    except StopIteration:
        if buffer:
            invoke(buffer)
    _msg('tagging queue has {} items'.format(len(tagging_queue)))
    _msg('first 10 are: {}'.format(['%s : %s' % (k,
                                formatted_taggings(v)) for k, v in list(
                                    tagging_queue.items())[:10]]))
    _msg('untagging queue has {} items'.format(len(untagging_queue)))
    _msg('first 10 are: {}'.format(" ".join(k for k in list(untagging_queue)[:10])))
    sys.exit(0)
    apply_queued_taggings()

def main(argv):
    from plumbum.cmd import which
    import sqlite3

    def explode(*args,**kwargs):
        _msg(*args, **kwargs)
        sys.exit(1)

    args = parse_args(argv)

    if args.cmd == 'dupes':

        if args.command:
            args.command = args.command.split(" ")
            executable = which[args.command]().rstrip()
            print ('trying dupes')
            if not os.path.exists(executable):
                explode('Command {} not found'.format(executable))

            conn = sqlite3.connect(args.database)
            _interactive_duplicate_removal(conn.cursor(),
                args.command,
                args.limit,
                args.minimum)



if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
