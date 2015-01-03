import os
from math import sqrt

# taken from http://stackoverflow.com/questions/10029588/python-implementation-of-the-wilson-score-interval


def wilson_confidence(ups, downs):
    n = ups + downs

    if n == 0:
        return 0

    z = 1.0 #1.44 = 85%, 1.96 = 95%
    phat = float(ups) / n
    return ((phat + z*z/(2*n) - z * sqrt((phat*(1-phat)+z*z/(4*n))/n))/(1+z*z/n))


def counts_to_ranks(data, total):
    return [(id, wilson_confidence(c, total - c)) for id, c in data]


def count_directory_tags(cursor, directory, limit=None, names=False, recursive=False):
    """Return [ntaggedfiles, [(id, count),...]], with taggings sorted by count, highest first.

    ids may be returned as integer tag ids (default) or string tag names (if names=True)

    If recursive is True, taggings in subdirectories are also counted. Otherwise, only
    files that are in that exact directory (eg. foo/bar, but not foo/bar/baz) are counted.

    """
    limit = limit or 0xffffff
    while directory[-1] == os.path.sep:
        directory = directory[:-1]
    print ('relativization test:')
    normed = cursor.connection.normalize_path(directory)
    print (directory, cursor.connection.normalize_path(directory))
    directory = normed
    if recursive:
        total = cursor.execute('select count(*) from file'
                               ' where directory = ? or directory like ?',
                               (directory, directory + os.path.sep + '%')).fetchone()[0]
    else:
        total = cursor.execute('select count(*) from file where directory = ?', (directory,)).fetchone()[0]

    idfield = 'T.id' if not names else 'T.name'
    direxpr = 'directory = ?'
    params = (directory, limit)
    if recursive:
        direxpr = 'directory = ? or directory like ?'
        params = (directory, directory + os.path.sep + '%', limit)

    return total, sorted(cursor.execute('select ' + idfield + ', count(*)'
        ' from file_tag'
        ' join tag as T on tag_id=T.id'
        ' join (select id from file where ' + direxpr + ') as F on file_id=F.id'
        ' group by tag_id'
        ' order by count(*)'
        ' desc limit ?', params).fetchall(), key=lambda v:(v[1], v[0]))


if __name__ == '__main__':
    import sys
    from .core import connect
    conn = connect(os.path.realpath(sys.argv[1]))
    print(conn._relpath)
    sample = os.path.realpath(sys.argv[2])
    print ('relativizing %r ... -> %r' % (sample, conn.normalize_path(sample)))
    cursor = conn.cursor()
    print(count_directory_tags(cursor, sample, names=True, recursive=True))
