#!/usr/bin/env python3
"""Annotation support for TMSU
"""

import os
import sys
import argparse
import re
from .core import (get_db_path, connect, validate_name,
                   tag_names, rename_tag,
                   KeyExists, tag_id, register_hook, resolve_tag_value)
from .util import do_commit


_DB_PATH = get_db_path()


def init(cursor):
    c = cursor
    c.execute('CREATE TABLE IF NOT EXISTS'
              ' annotation (id INTEGER PRIMARY KEY,'
              ' file_id INTEGER NOT NULL,'
              ' x INTEGER NOT NULL,'
              ' y INTEGER NOT NULL,'
              ' z INTEGER NOT NULL,'
              ' w INTEGER NOT NULL,'
              ' h INTEGER NOT NULL,'
              ' d INTEGER NOT NULL,'
              ' text TEXT NOT NULL,'
              ' CONSTRAINT con_ann_roi_unique UNIQUE(file_id,x,y,z,w,h,d))')


def db_connect(path=None):
    import sqlite3
    conn = sqlite3.connect(path or _DB_PATH)
    return conn, conn.cursor()


def msg(*args):
    print(*args, file=sys.stderr)


def description(cursor, path, index = None):
    """Return zero or more descriptions of the given path.
    
    If index is not specified, all available descriptions of the path are selected 
    Otherwise, a 1-long list containing the specified description is returned
    (ValueError is raised if this does not exist)
    
    If no description is available, an empty list is returned.
    """
    from .file import file_id
    fid = file_id(cursor, path)
    if fid is None:
        return []
    if index and index > 0:
        data = cursor.execute('SELECT text from annotation'
                              ' WHERE file_id = ? and x = ?',
                              (-index,)).fetchall()
        if len(data) != 1:
            raise ValueError('1 result expected, got %d' % len(data))
    else:
         data = cursor.execute('SELECT text from annotation'
                               ' WHERE file_id = ? and x < 0'
                               ' ORDER BY x DESC').fetchall()
    return [v[0] for v in data]


def annotations(cursor, path):
    """Return all annotations (not descriptions) of the given path,
    as (id, x, y, z, w, h , d, text) tuples."""
    from .file import file_id
    fid = file_id(cursor, path)
    if fid is None:
        return []
    return cursor.execute('SELECT id, x, y, z, w, h, d, text from annotation'
                          ' WHERE file_id = ? and x > -1 and y > -1 and z > -1'
                          ' ORDER BY x, y, z, w, h, d, text', (fid,)).fetchall()

def annotate(cursor, path, roi, text, id = None):
    from .file import file_id
    if len(roi) != 6:
        raise ValueError('roi %r is wrong length, should be 6' % (roi,))
#    normp = cursor.connection.normalize_path(path)
#    print('NP',normp)
    fid = file_id(cursor, path)
    print(fid)
    if fid is None:
        raise ValueError('Cannot annotate file not in database')
    proctext = tagnames_to_tagids(cursor, text)
    if id:
        cursor.execute('REPLACE INTO annotation(file_id,x,y,z,w,h,d,text) WHERE id=? VALUES(?,?,?,?,?,?,?,?)',
                       (id, fid) + roi + (proctext,))
    else:
        cursor.execute('INSERT INTO annotation(file_id,x,y,z,w,h,d,text) VALUES(?,?,?,?,?,?,?,?)',
                       (fid, ) + roi + (proctext,))

def parse_roi(text):
    """Parse a x,y,z:wxhxd ROI spec into a (x, y, z, w, h, d) tuple.
    
    """
    if text.count(':') != 1:
        raise ValueError('ROI %r contains != 1 ":"')
    if text.count('x') > 2 or text.count(',') > 2:
        raise ValueError('ROI %r references more than 3 dimensions.')
    lhs, rhs = text.split(':')
    print ("LHS, RHS %r .. %r" % (lhs, rhs))
    ltuple = tuple(0 if not v else int(v) for v in lhs.split(','))
    while len(ltuple) < 3:
        ltuple = ltuple + (0,)
    rtuple = tuple(0 if not v else int(v) for v in rhs.split('x'))
    while len(rtuple) < 3:
        rtuple = rtuple + (0,)
    return ltuple + rtuple
    

def stringify_annotations(cursor, annotations):
    """Return string form '+x+y+z:wxhxd : text' formatted versions of the annotation tuples."""
    results = []
    for id, x, y, z, w, h, d, text in annotations:
        if d < 0:
            depth = ':p%d' % d
        else:
            depth = 'x%d' % d
        if x == y and y == z and z == w and w == h and x == 0:
            coords = ''
        else:
            coords = '+%d+%d+%d:%dx%d' % (x, y, z, w, h)
        results.append('%s%s : %s' % (coords, depth, tagids_to_tagnames(cursor, text)))


def describe(cursor, path, description, index = 1):
    """Add a description to the given path.
    A description is an annotation with negative x,y,z values corresponding to index
    (so, a file can have multiple descriptions)
    
    If a description with the given index (default 1) exists, it is replaced.
    """
    if index < 1:
        raise ValueError('Description index must be >= 1, not %r' % index)
    cursor.execute('replace into annotations (file_id, x, y, z, w, h, d, text)'
                   ' VALUES(?, 0, 0, 0, 0, 0, ?)', (-index, description))
    cursor.connection.commit()



def tagnames_to_tagids(cursor, ann_text):
    """Convert tag names to tag id codes eg foo -> {#123} in the annotation text, for storage"""
    from .tag import tag_id_map
    map = tag_id_map(cursor)
    def encode(m):
        main = m.expand('\\2')
        if main in map:
            return m.expand('\\1{#%d}') % map[main]
        return m.expand('\\1\\2')

    return re.sub('(^|[ ,])([^ ,]+)(?=$|[ ,])', encode, ann_text)

def tagids_to_tagnames(cursor, ann_text):
    """Convert tag id codes '{#123}' -> 'foo' in the annotation text, for display.
    Codes that don't match tags are replaced like '{#123}' -> '{#123?}' instead.
    """
    from .tag import id_tag_map
    map = id_tag_map(cursor)
    return re.sub("\{#([0-9]+)\}", lambda m: map.get(int(m.expand('\\1')), '{#%s?}' % m.expand('\\1')), ann_text)

def parse_args(_args):
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Annotate files")
    subp = parser.add_subparsers(dest='command')
    subp.required = True

    add = subp.add_parser('add', help='Annotate a file')
    add.add_argument('path', type=str, help='path to annotate')
    add.add_argument('annotation', type=str, nargs='+',
                         help='[ROI TEXT] pairs')

    rm = subp.add_parser('rm', help='remove an annotation')
    rm.add_argument('path', type=str, nargs='+',
                    help='path to annotate')
    rm.add_argument('which', type=str, nargs='+',
                         help='annotation id, or text search string')
    return parser.parse_args(_args)


def main(args):
    arg = parse_args(args)
    p = get_db_path()
    print(p)
    conn = connect(p)
    cursor = conn.cursor()
    init(cursor)
    if arg.command == 'add':
        from itertools import groupby
        annotation_tuples = tuple(tuple(v[1]) for v in groupby(arg.annotation, lambda k:arg.annotation.index(k) // 2))
        annotation_tuples = [(parse_roi(v[0]), v[1]) for v in annotation_tuples]
        print ('add got %r ' % (annotation_tuples,))
        arg.path = arg.path
        print('path is %r' % arg.path)
        for roi, text in annotation_tuples:
            annotate(cursor, arg.path, roi, text)
            print ("%r -> %r" % (roi, text))
        conn.commit()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])