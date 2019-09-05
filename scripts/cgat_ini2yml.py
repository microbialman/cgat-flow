'''
cgat_ini2yml
=======================================

'''

import sys
import re
import cgatcore.experiment as E


def main(argv=None):
    """script main.

    parses command line options in sys.argv, unless *argv* is given.
    """

    if argv is None:
        argv = sys.argv

    # setup command line parser
    parser = E.OptionParser(description=__doc__)

    parser.add_argument("-n", "--dry-run", dest="dry_run", action="store_true",
                        help="dry run, do not delete any files")

    parser.set_defaults(dry_run=False)

    # add common options (-h/--help, ...) and parse command line
    (args) = E.start(parser, argv=argv)

    indent = 0
    for line in args.stdin:

        if not line.strip():
            args.stdout.write("\n")
            continue

        if line.startswith("["):
            section = re.search("\[(.*)\]", line).groups()[0]
            if section == "general":
                indent = 0
                args.stdout.write("\n")
            else:
                indent = 4
                args.stdout.write("{}:\n".format(line.strip()[1:-1]))

        elif line.startswith("#"):
            args.stdout.write("{}{}".format(" " * indent, line))

        elif "=" in line:
            key, val = re.search("([^=]+)=(.*)", line).groups()
            key = key.strip()
            val = val.strip()

            if "," in val:
                val = "[{}]".format(val)

            if "!?" in val:
                val = re.sub("!?", "?!", val)

            if val is None:
                val = ''

            if val == "":
                val = "''"

            args.stdout.write("{}{}: {}\n".format(" " * indent, key, val))
    # write footer and output benchmark information.
    E.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
