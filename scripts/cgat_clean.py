'''
cgat_clean.py - remove incomplete files
=======================================


Purpose
-------

This script looks at files matching a certain pattern and will remove
incomplete files. File completeness is determined by the file itself
or an associated log-file.

For example, the file :file:`sample1.tsv` is deemed complete if:

1. :file:`sample1.tsv.log` exists and ends in
   ``# job finished ...``,
2. :file:`sample1.tsv` exists and ends in ``# job finished ...``

Usage
-----

Example::

   python cgat_clean.py *.tsv

Type::

   python cgat_clean.py --help

for command line help.

Command line options
--------------------

'''

import os
import sys

import cgatcore.experiment as E
import cgatcore.iotools as iotools


def main(argv=None):
    """script main.

    parses command line options in sys.argv, unless *argv* is given.
    """

    if argv is None:
        argv = sys.argv

    # setup command line parser
    parser = E.OptionParser(description=__doc__)

    parser.add_argument("-n", "--dry-run", dest="dry_run", action="store_true",
                        help="dry run, do not delete any files [%default]")

    parser.set_defaults(dry_run=False)

    # add common options (-h/--help, ...) and parse command line
    (args, unknown) = E.Start(parser, argv=argv, unknowns=True)

    filenames = unknown

    c = E.Counter()
    for filename in filenames:
        c.checked += 1
        if os.path.exists(filename + ".log"):
            if iotools.isComplete(filename + ".log"):
                c.complete += 1
                continue

        if iotools.isComplete(filename):
            c.complete += 1
            continue

        c.incomplete += 1
        E.info('deleting %s' % filename)
        if args.dry_run:
            continue
        os.unlink(filename)
        c.deleted += 1

    E.info(c)

    # write footer and output benchmark information.
    E.Stop()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
