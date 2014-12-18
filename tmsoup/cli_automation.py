"""CLI automation for tmsoup

Based on simple multiple-dispatch and subparsers.

Output is delimited by NULLs (one follows each command's output,
even if there was no output.)

Modules are expected to register any subcommand executors at import time.
This allows you to include handling for whatever categories of functionality
you want just by importing the relevant modules.

"""

_registry = {}

# XXX need a way to improve CLI help so it includes automation

def register(subcommands, cli_executor):
    """Register a CLI executor handling the specified subcommands.

    Subcommands may be a string or a sequence.

    The executor must accept the keyword-args `subcommand_id` and
    `cursor`. `cursor` should be used instead of making its own connection to
    the database.

    Raises
    =======
    ValueError      when an existing registration for a given subcommand
                    already exists.

    Notes
    ======
    Subcommand names are case-folded;
    eg. 'Untag' and 'UNTAG' are the same subcommand.

    """
    if isinstance(subcommands, str):
        subcommands = [subcommands]
    for subc in subcommmands:
        subc = subc.lower()
        if subc in _registry:
            raise ValueError('Attempt to register subcommand {} -> {},'
                             ' which would conflict with existing'
                             ' registration {} -> {}'.format(subc,
                                                             cli_executor,
                                                             subc,
                                                             _registry[subc]))
        _registry[subc] = cli_executor


def unregister(subcommands, must_exist=True):
    """Remove existing registration of subcommand executor(s)

    Raises
    =======
    KeyError        when the specified subcommand is not registered,
                    unless must_exist=False
    """
    if isinstance(subcommands, str):
        subcommands = [subcommands]
    for subc in subcommands:
        subc = subc.lower()
        if subc in _registry:
            del _registry[subc]
        elif not must_exist:
            raise KeyError('Executor for {} not registered.')


def automate(shared_args, args, cursor=None):
    """Given shared_args and args, examine them and
    dispatch to the correct sub-CLI-handler, based
    on args[0] value.
    """
    dispatch_on = args[0].lower()
    if dispatch_on not in _registry:
        raise ValueError('unknown subcommand {}'.format(dispatch_on))
    used_args = shared_args + args
    func = _registry[dispatch_on]
    func(used_args, subcommand_id=dispatch_on, cursor=cursor)


def read_args(file):
    """Read a sequence of arguments from file.

    Returns
    ========
    arguments       list of strings, one per line
    is_eof          True if there is no more data to parse

    Argument sequences:
      * should begin with a suitable argument to select a subcommand, like
        'untag'
      * separate arguments by newlines -- 'foo bar' is one argument.
        'foo
        bar' is two arguments.
      * are terminated by a blank line or EOF:
        'foo
        bar

        baz
        bam
        '
        Expresses two argument sequences, each containing two arguments.

    After calling read_args(), the file cursor will be either at the
    beginning of a new argument sequence, or at EOF.

    This function is somewhat slow. It could be improved using array('u'),
    probably. It must not use seek(), in order to support reading from stdin.
    """
    text = []
    eof = False
    c = 'dummy'
    while c:
        c = f.read(1)
        if c:
            # newline immediately followed by newline?
            # end of this commandline.
            if not (c == '\n' and (not text or text[-1] == '\n')):
                text.append(c)
            else:
                break
        else: # EOF
            eof = True
            break
    return ("".join(text)).splitlines(), eof


def parse_file(file):
    """Parse a CLI automation file, yielding (shared_args, args) tuples

    Examples
    =========

    Format looks like:

    shared_argument
    shared_argument[...]
    BLANK LINE
    argument
    argument[...]
    BLANK_LINE_OR_EOF

    An example might be (with EOF representing .. end of file)

    -D=/home/me/.my.db

    tag
    mysong.mp3
    metal

    tag
    --tags=apple banana orange iguana
    mypicture.jpg

    stats
    EOF


    Note that spaces and other 'shell characters' are not treated specially.

    -D=/home/me/.my spaced out.db

    is one argument.

    Similarly,

    "foo"

    is a five-character string beginning with " and ending with ", not a
    three-character string beginning with f and ending with o.

    The list of shared_arguments may be empty.
    that looks like this:

    --- file start ---

    tag
    mysong.mp3
    metal

    tag
    --tags=apple banana orange iguana
    mypicture.jpg
    --- file end ---

    (ie. just include a single blank line at start of file,
    if you don't have any shared args.)

    """
    eof = False
    shared_args, eof = read_args(file)
    if eof:
        raise

    while not eof:
        args, eof = read_args(file)
        # ignore empty commands (no args)
        # this allows you to separate commands by as many blank lines
        # as desired.
        if args:
            yield (shared_args, args)



def add_shared_options(parser):
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='Explain what is being done.')
    parser.add_argument('-D', '--database', default=None, type=str,
                        help='Use the specified database.')
    parser.add_argument('-t','--single-transaction', default=False,
                        action='store_true',
                        help='Wrap all actions in a single transaction.'
                        ' This can speed up actions that modify the database,'
                        ' like tagging or untagging.'
                        ' The entire set of actions must succeed for the'
                        ' modifications to be successfully committed')


def get_cursor(parsed_args):
    from .core import get_db_path
    parsed_args.database = get_db_path(parsed_args.database)


def parse_shared_args(args):
    parser = argparse.ArgumentParser('CLI Automation for tmsoup')
    add_shared_options(parser)
    return parser.parse_known_args(args)


def automate_from_file(file, single_transaction=False):
    """Parse and dispatch a complete file"""
    for shared, args in parse_file(file):
        automate(shared, args)


if __name__ == "__main__":
    # simple testing of parsing.
    import sys
    from io import StringIO
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        f = open(filename, 'r')
    else:
        f = StringIO("""two
shared args

a
command
with
four args

a
shorter
command

spaces are okay, this is a one argument command

this is
the final command, eof follows.
""")
    for shared, args in parse_file(f):
        print ('%r\t%r' % (shared, args))
