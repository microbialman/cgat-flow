'''cgat_tsv2links.py - create a series of file links with renaming
==================================================================


Purpose
-------

Create links from a tab-separated table. The table should have two
columns, for example::

   source     dest
   abc.fq.gz  sample1-condition-R1.fastq.1.gz
   def.fq.gz  sample1-condition-R2.fastq.2.gz

If the options ``--source`` is given, filenams will be matched by
walking through ``--source``. Otherwise, filenames in the source
column need to contain the paths relative to the current working
directory.

To create such a table, use the unix ``find`` command, for example::

   find /ifs/projects/proj013/backup/ -name "*.sanfastq.gz" > input_file.tsv

and then manually add table headers and a second column with the
sample name.

Further ways to develop the script:

   * paired files - files might be grouped (read pairs), make sure
            they are all there. Use pattern matching to identify a
            group and create all appropriate links.

    *  requires exception handling if no options of files are provided

Usage
-----

For example::

   python cgat_tsv2links.py --source=../backup/dataset1 < input_file.tsv

Type::

   python cgat_tsv2links.py --help

for command line help.

Command line options
--------------------

'''

import os
import sys
import cgatcore.experiment as E


def main(argv=None):
    """script main.

    parses command line options in sys.argv, unless *argv* is given.
    """

    if not argv:
        argv = sys.argv

    # setup command line parser
    parser = E.OptionParser(description=__doc__)

    parser.add_argument("-s", "--source", dest="source_directory",
                        type=str, default=False,
                        help="The directory in which data"
                        "files are held")

    parser.add_argument("-d", "--dest", dest="dest_directory",
                      type=str, default=False,
                      help="The directory in which links"
                      "are created")

    parser.set_defaults(source_directory=None,
                        dest_directory=".")

    # add common options (-h/--help, ...) and parse command line
    (args) = E.start(parser, argv=argv)

    # read a map of input files to links with sanity checks
    map_filename2link = {}
    links = set()
    for line in args.stdin:
        if line.startswith("#"):
            continue

        # ignore header
        if line.startswith("source"):
            continue

        filename, link = line[:-1].split()[:2]
        if filename in map_filename2link:
            raise ValueError("duplicate filename '%s' " % filename)
        if link in links:
            raise ValueError("duplicate link '%s' " % link)
        map_filename2link[filename] = link
        links.add(link)

    counter = E.Counter()
    counter.input = len(map_filename2link)

    def _createLink(src, dest, counter):
        src = os.path.abspath(src)
        dest = os.path.abspath(os.path.join(args.dest_directory, dest))
        if os.path.exists(dest):
            E.warn("existing symlink %s" % dest)
            counter.link_exists += 1
        elif not os.path.exists(src):
            counter.file_not_found += 1
            E.warn("did not find %s" % src)
        else:
            try:
                os.symlink(src, dest)
                counter.success += 1
            except OSError:
                pass

    if not args.source_directory:
        # no source directory given, filenames must have complete path
        for filename, link in list(map_filename2link.items()):
            _createLink(filename, link, counter)
    else:
        # walk through directory hierchy and create links
        # for files matching filenames in map_filename2link
        found = set()
        for dirName, subdirList, fileList in os.walk(args.source_directory):
            for f in fileList:
                if f in map_filename2link:
                    if f in found:
                        E.warn("found multiple files with "
                               "the same name %s" % f)
                    else:
                        _createLink(os.path.join(dirName, f),
                                    map_filename2link[f], counter)
                        found.add(f)
                else:
                    E.info("Filename %s not in map" % f)

        notfound = set(map_filename2link.keys()).difference(found)
        counter.notfound = len(notfound)
        if notfound:
            E.warn("did not find %i files: %s" % (len(notfound),
                                                  str(notfound)))

    E.info(counter)
    # write footer and output benchmark information
    E.stop()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
