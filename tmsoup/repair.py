import os
import glob
import re
from .core import splitpath, KeyExists, file_mtime
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
    print('ndupes = %d' % len(duplicated))
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
