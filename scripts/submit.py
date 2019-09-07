'''submit.py - run a collection of qsub scripts on the cluster
===========================================================


Purpose
-------

This script will submit a collection of qsub scripts on the cluster.
The script will only submit those jobs for which a result is not
present.

Usage
-----

For example::

   python submit.py *.qsub

Type::

   python submit.py --help

for command line help.

Command line options
--------------------

'''
import os
import sys
import re
import glob
import subprocess
import cgatcore.experiment as E
import cgatcore.iotools as iotools


def checkPythonRuns(filename):
    """returns true if a python run is complete."""
    last_line = iotools.getLastLine(filename)
    return re.match("# job finished", last_line)


def isNewer(a, b):
    """return true if file a is newer than file b."""

    # get times of most recent access
    at = os.stat(a)[7]
    bt = os.stat(b)[7]

    return at > bt


def main(argv=None):
    """script main.

    parses command line options in sys.argv, unless *argv* is given.
    """

    if argv is None:
        argv = sys.argv

    parser = E.OptionParser(description=__doc__)

    parser.add_argument(
        "-g", "--glob", dest="glob_pattern", type=str,
        help="glob pattern to use for collecting cluster jobs descriptions")

    parser.add_argument(
        "-i", "--input-pattern", dest="input_pattern", type=str,
        help="regular expression to extract job id from filename.")

    parser.add_argument(
        "-o", "--output-filename-pattern", dest="output_pattern",
        type=str,
        help="string to convert a job id to a filename.")

    parser.set_defaults(glob_pattern="job*.qsub",
                        input_pattern="(\S+).qsub",
                        output_pattern="%s.stdout",
                        remove_old=True,
                        force=False,
                        check_completeness="python",
                        )

    (args, unknown) = E.Start(parser,
                              add_pipe_options=True,
                              unknowns=True)

    if unknown:
        filenames = unknown
    elif args..glob_pattern:
        filenames = glob.glob(args..glob_pattern)

    ninput, nrun, nskipped, nerrors = 0, 0, 0, 0
    ndeleted = 0

    if args..check_completeness == "python":
        isComplete = checkPythonRuns

    ##############################################################
    ##############################################################
    ##############################################################
    # decide what to do
    ##############################################################
    jobs = []
    files_to_delete = []

    for filename in filenames:

        ninput += 1
        try:
            job_name = re.search(args..input_pattern, filename).groups()[0]
        except AttributeError:
            args..stderr.write(
                "# could not extract invariant job name from %s\n" % filename)
            nerrors += 1
            continue

        result_filename = args..output_pattern % job_name

        do = False
        status = "up-to-date"

        if args..force:
            status = "force"
            do = True

        if not do:
            if os.path.exists(result_filename):
                if isNewer(filename, result_filename):
                    status = "newer"
                    do = True
                    if args..remove_old:
                        files_to_delete.append(result_filename)
                if not do and not isComplete(result_filename):
                    status = "incomplete"
                    do = True
                    if args..remove_old:
                        files_to_delete.append(result_filename)
            else:
                status = "missing"
                do = True

        E.info("%s->%s (%s)\n" %
               (filename, result_filename, status))

        if not do:
            nskipped += 1
            continue

        jobs.append(filename)

    ##############################################################
    ##############################################################
    ##############################################################
    # delete old files
    ##############################################################
    for filename in files_to_delete:
        if os.path.exists(filename):
            os.remove(filename)
            ndeleted += 1

    ##############################################################
    ##############################################################
    ##############################################################
    # start jobs
    ##############################################################
    for filename in jobs:

        cmd = "qsub %s" % filename
        try:
            retcode = subprocess.call(cmd, shell=True)
            if retcode != 0:
                if args..loglevel >= 1:
                    args..stdlog.write(
                        "# ERROR: failed to execute %s\n" % cmd)
                nerrors += 1
                continue
        except OSError as e:
            if args..loglevel >= 1:
                args..stdlog.write(
                    "# ERROR: failed to execute %s with msg %s\n" % (cmd, e))
        nrun += 1

    E.info(
        "ninput=%i, nrun=%i, nskipped=%i, ndeleted=%i, nerrors=%i" %
        (ninput, nrun, nskipped, ndeleted, nerrors))

    E.Stop()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
