# see tmsu/common/fingerprinter.go
import os
from hashlib import sha1, sha256, md5


SPARSE_FINGERPRINT_THRESHOLD = 5 * 1024 * 1024
SPARSE_FINGERPRINT_SIZE = 512 * 1024

CONFIG_KEY = 'fingerprintAlgorithm'
DEFAULT_ALGORITHM = 'dynamic:SHA256'

from .config import get_config

def fingerprint(path, algorithm=None):
    algorithm = algorithm or DEFAULT_ALGORITHM
    try:
        func = fingerprinters[algorithm]
    except KeyError:
        raise ValueError('Unknown fingerprinting algorithm %r' % algorithm)

    if (not os.path.exists(path)) or os.path.isdir(path):
        return None
    # XXX there is another possible case -- 'can't stat this' -- in the TMSU code. 
    # But, how can that possibly occur? If you can't stat it, that means it doesn't exist, no?
    data = func(path)
    return data

def get_fingerprint_algorithm(cursor):
    """Return the fingerprinting algorithm configured for the given database.
    
    """
    return get_config(cursor, CONFIG_KEY, DEFAULT_ALGORITHM)


def set_fingerprint_algorithm(cursor, algo):
    """
    
    Warning
    ========
    
    Do not simply call this on an already-populated database -- it may not do what you expect.
    Since changing the fingerprint algorithm renders existing fingerprints invalid,
    it is better to use tmsoup.repair.change_fingerprint_algorithm().
    
    Raises
    =======
    ValueError       When the specified algorithm is not recognized as a valid one
    
    """

def fullhash(path, hasher):
    hasher = hasher()
    with open(path, 'rb') as f:
        chunk = 'dummy'
        while chunk:
            chunk = f.read(0xffff)
            if chunk:
                hasher.update(chunk)
    return hasher.hexdigest()


def sparsehash(path, hasher):
    size = os.path.getsize(path)
    if size < SPARSE_FINGERPRINT_THRESHOLD:
        return fullhash(path, hasher)

    hasher = hasher()
    with open(path, 'rb') as f:
        chunk = f.read(SPARSE_FINGERPRINT_SIZE)
        hasher.update(chunk)
        f.seek(size - SPARSE_FINGERPRINT_SIZE)

    

def fp_sha256(path):
    return fullhash(path, sha256)


def fp_sha1(path):
    return fullhash(path, sha1)


def fp_md5(path):
    return fullhash(path, md5)


def fp_dynamic_sha256(path):
    return sparsehash(path, sha256)


def fp_dynamic_sha1(path):
    return sparsehash(path, sha1)


def fp_dynamic_md5(path):
    return sparsehash(path, md5)


# XXX these two need specific testing to make sure they work exactly the same as TMSU's implementation.
#
# particularly:
#  * when it's a symlink, are we supposed to just read the link, or take the real path 
#    (os.path.realpath(p), which is always an absolute path)


def fp_symlink_targetname(path):
    if os.path.islink(path):
        return os.readlink(path)
    else:
        return os.path.basename(path)


def fp_symlink_targetname_noext(path):
    return os.path.splitext(fp_symlink_targetname(path))[0]


fingerprinters = {'dynamic:SHA256': fp_dynamic_sha256,
                  'dynamic:SHA1': fp_dynamic_sha1,
                  'dynamic:MD5': fp_dynamic_md5,
                  'sha256': fp_sha256,
                  'sha1': fp_sha1,
                  'md5': fp_md5,
                  'symlinkTargetName': fp_symlink_targetname,
                  'symlinkTargetNameNoExt': fp_symlink_targetname_noext}

__all__ = ('fingerprint', 'fingerprinters')
