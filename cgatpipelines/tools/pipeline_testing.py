"""=================================================
pipeline_testing - automated testing of pipelines
=================================================


This pipeline executes other pipelines for testing purposes.

Overview
========

This pipeline implements automated testing of cgat pipelines. The
pipeline downloads test data from a specified URL, runs the associated
pipeline for each data set and compares the output with a reference.
The results are collected in a report.

Tests are setup in the pipeline configuration file.

Usage
=====

See :ref:`PipelineSettingUp` and :ref:`PipelineRunning` on general
information how to use cgat pipelines.

In order to run all tests, simply enter an empty directory and type::

   python <srcdir>/pipeline_testing.py config

Edit the config files as required and then type::

   python <srcdir>/pipeline_testing.py make full
   python <srcdir>/pipeline_testing.py make build_report

The first command will download the data and run the pipelines while
the second will build a summary report.

Configuration
-------------

The pipeline requires a configured :file:`pipeline.yml` file.

Tests are described as section in the configuration file. A test
section starts with the prefix ``test_``. For following example is a
complete test setup::

   [test_mytest1]
   # pipeline to run
   pipeline=pipeline_mapping

   # pipeline target to run (default is 'full')
   # multiple targets can be specified as a comma separated list.
   target=full

   # filename suffixes to checksum
   regex_md5=gtf.gz,bed.gz,tsv.gz,bam,nreads

   # regular expression of files to be excluded from
   # test for difference. Use | to separate multiple
   # regular expressions.
   regex_only_exist=rates.gff.gz

This configuration will run the test ``mytest1``. The associated
pipeline is :doc:`pipeline_mapping` and it will execute the target
``make full``. To check if the pipeline has completed successfully, it
will compare all files ending with any of the suffixes specified
(``gtf.gz``, ``bed.gz``, etc). The comparison will be done by building
a checksum of the whole file ignoring any comments (lines starting
with a ``#``).

Some files will be different at every run, for example if they use
some form of random initialization. Thus, the exact test can be
relaxed for groups of files. Files matching the regular expression in
``regex_linecount`` will test if a file exists and the number of lines
are identitical.  Files matching the regular expressions in
``regex_exist`` will thus only be tested if they exist or not.

The test expects a file called :file:`test_mytest1.tgz` with the
test data at the download URL (parameter ``data_url``).

To define a default test for a pipeline, simply name the
test ``test_<pipeline name>``, for example::

   [test_mapping]
   regex_md5=gtf.gz,bed.gz,tsv.gz,bam,nreads

Note that setting the ``target`` and ``pipeline`` options is
not necessary in this case as the default values suffice.

Input data
----------

The input data for each test resides a compressed tar-ball. The input
data should uncompress in a directory called :file:`<testname>.dir`
The tar-ball need also contain a file :file:`<testname>.ref`
containing the md5 checksums of files of a previous run of the test
that is being used as a reference.

The input data should contain all the data that is required for
running a test within a directory. It is best to minimize dependencies
between tests, though there is a mechanism for this (see below).

For example, the contents of a tar-ball will look light this::

   test_mytest1.dir/                     # test data root
   test_mytest1.dir/Brain-F2-R1.fastq.gz # test data
   test_mytest1.dir/Brain-F1-R1.fastq.gz
   test_mytest1.dir/hg19.fasta           # genomic data
   test_mytest1.dir/hg19.idx
   test_mytest1.dir/hg19.fa
   test_mytest1.dir/hg19.fa.fai
   test_mytest1.dir/pipeline.yml  # pipeline configuration file
   test_mytest1.dir/indices/      # configured to work in test dir
   test_mytest1.dir/indices/bwa/  # bwa indices
   test_mytest1.dir/indices/bwa/hg19.bwt
   test_mytest1.dir/indices/bwa/hg19.ann
   test_mytest1.dir/indices/bwa/hg19.pac
   test_mytest1.dir/indices/bwa/hg19.sa
   test_mytest1.dir/indices/bwa/hg19.amb
   test_mytest1.ref   # reference file

The reference file looks like this::

   test_mytest1.dir/bwa.dir/Brain-F2-R2.bwa.bam 0e1c4ee88f0249c21e16d93ac496eddf
   test_mytest1.dir/bwa.dir/Brain-F1-R2.bwa.bam 01bee8af5bbb5b1d13ed82ef1bc3620d
   test_mytest1.dir/bwa.dir/Brain-F2-R1.bwa.bam 80902c87519b6865a9ca982787280972
   test_mytest1.dir/bwa.dir/Brain-F1-R1.bwa.bam 503c99ab7042a839e56147fb1a221f27
   ...

This file is created by the test pipeline and called
:file:`test_mytest1.md5`.  When setting up a test, start with an empty
files and later add this file to the test data.

Pipeline dependencies
---------------------

Some pipelines depend on the output of other pipelines, most notable
is :doc:`pipeline_annotations`. To run a set of pipelines before other
pipelines name them in the option ``prerequisites``, for example::

   prerequisites=prereq_annnotations

Pipeline output
===============

The major output is in the database file :file:`csvdb`.

Code
====

"""
from ruffus import files, transform, suffix, follows, merge, collate, regex, mkdir, jobs_limit
import sys
import pipes
import os
import re
import glob
import tarfile
import pandas
import cgatcore.experiment as E
import cgatcore.iotools as iotools

###################################################
###################################################
###################################################
# Pipeline configuration
###################################################

# load options from the config file
from cgatcore import pipeline as P
PARAMS = P.get_parameters(
    ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
     "../pipeline.yml",
     "pipeline.yml"])

# WARNING: pipeline names with underscores in their name are not allowed
TESTS = sorted(set(["test_{}".format(x.split("_")[1])
                    for x in PARAMS.keys() if x.startswith("test_")]))


# obtain prerequisite generic data
@files([(None, "%s.tgz" % x)
        for x in P.as_list(PARAMS.get("prerequisites", ""))])
def setupPrerequisites(infile, outfile):
    '''setup pre-requisites.

    These are tar-balls that are unpacked, but not run.
    '''

    #to_cluster = False
    track = P.snip(outfile, ".tgz")

    # obtain data - should overwrite pipeline.yml file
    statement = '''
    wget --no-check-certificate -O %(track)s.tgz %(data_url)s/%(track)s.tgz'''
    P.run(statement)

    tf = tarfile.open(outfile)
    tf.extractall()


@files([(None, "{}.tgz".format(x)) for x in TESTS])
def setupTests(infile, outfile):
    '''setup tests.

    This method creates a directory in which a test will be run
    and downloads test data with configuration files.
    '''
    #to_cluster = False

    track = P.snip(outfile, ".tgz")

    if os.path.exists(track + ".dir"):
        raise OSError('directory %s.dir already exists, please delete' % track)

    # create directory
    os.mkdir(track + ".dir")

    # run pipeline config
    pipeline_name = PARAMS.get("%s_pipeline" % track, track[len("test_"):])

    statement = (
        "(cd %(track)s.dir; "
        "cgatflow %(pipeline_name)s "
        "%(pipeline_options)s "
        "%(workflow_options)s "
        "config "
        "2> %(outfile)s.stderr "
        "1> %(outfile)s.log)")
    P.run(statement)

    # obtain data - should overwrite pipeline.yml file
    statement = '''
    wget --no-check-certificate -O %(track)s.tgz %(data_url)s/%(track)s.tgz'''
    P.run(statement)

    tf = tarfile.open(outfile)

    tf.extractall()

    if not os.path.exists("%s.dir" % track):
        raise ValueError(
            "test package did not create directory '%s.dir'" % track)


def run_test(infile, outfile):
    '''run a test.

    Multiple targets are run iteratively.
    '''

    track = P.snip(outfile, ".log")
    pipeline_name = PARAMS.get("%s_pipeline" % track, track[len("test_"):])

    pipeline_targets = P.as_list(PARAMS.get("%s_target" % track, "full"))

    # do not run on cluster, mirror
    # that a pipeline is started from
    # the head node
    #to_cluster = False

    template_statement = (
        "cd %%(track)s.dir; "
        "xvfb-run -d cgatflow %%(pipeline_name)s "
        "%%(pipeline_options)s "
        "%%(workflow_options)s make %s "
        "-L ../%%(outfile)s "
        "-S ../%%(outfile)s.stdout "
        "-E ../%%(outfile)s.stderr")

    if len(pipeline_targets) == 1:
        statement = template_statement % pipeline_targets[0]
        P.run(statement, ignore_errors=True, job_memory="unlimited")
    else:
        statements = []
        for pipeline_target in pipeline_targets:
            statements.append(template_statement % pipeline_target)
        P.run(statement, ignore_errors=True, job_memory="unlimited")


# @follows(setupTests)
# @files([("%s.tgz" % x, "%s.log" % x)
#         for x in P.as_list(PARAMS.get("prerequisites", ""))])
# def runPreparationTests(infile, outfile):
#     '''run pre-requisite pipelines.'''
#     run_test(infile, outfile)


@follows(setupTests, setupPrerequisites)
@files([("%s.tgz" % x, "%s.log" % x) for x in TESTS if x not in P.as_list(PARAMS.get("prerequisites", ""))])
def run_tests(infile, outfile):
    '''run a pipeline with test data.'''
    run_test(infile, outfile)


@transform(run_tests,
           suffix(".log"),
           ".report")
def run_reports(infile, outfile):
    '''run a pipeline report.'''

    track = P.snip(outfile, ".report")

    pipeline_name = PARAMS.get("%s_pipeline" % track, track[len("test_"):])

    statement = '''
    cd %(track)s.dir &&
    xvfb-run -d cgatflow %(pipeline_name)s
    %(pipeline_options)s %(workflow_options)s make build_report
    -L ../%(outfile)s
    -S ../%(outfile)s.stdout
    -E ../%(outfile)s.stderr
    '''

    P.run(statement, ignore_errors=True)


def compute_file_metrics(infile, outfile, metric, suffixes):
    """apply a tool to compute metrics on a list of files matching
    regex_pattern."""

    if suffixes is None or len(suffixes) == 0:
        E.info("No metrics computed for {}".format(outfile))
        iotools.touch_file(outfile)
        return

    track = P.snip(infile, ".log")

    # convert regex patterns to a suffix match:
    # prepend a .*
    # append a $
    regex_pattern = " -or ".join(["-regex .*{}$".format(pipes.quote(x))
                                  for x in suffixes])

    E.debug("applying metric {} to files matching {}".format(metric,
                                                             regex_pattern))

    if metric == "file":
        statement = '''find %(track)s.dir
        -type f
        -not -regex '.*\/report.*'
        -not -regex '.*\/_.*'
        \( %(regex_pattern)s \)
        | sort -k1,1
        > %(outfile)s'''
    else:
        statement = '''find %(track)s.dir
        -type f
        -not -regex '.*\/report.*'
        -not -regex '.*\/_.*'
        \( %(regex_pattern)s \)
        -exec %(test_scriptsdir)s/cgat_file_apply.sh {} %(metric)s \;
        | perl -p -e "s/ +/\\t/g"
        | sort -k1,1
        > %(outfile)s'''

    P.run(statement)


@follows(run_reports)
@transform(run_tests,
           suffix(".log"),
           ".md5")
def buildCheckSums(infile, outfile):
    '''build checksums for files in the build directory.

    Files are uncompressed before computing the checksum
    as gzip stores meta information such as the time stamp.
    '''
    track = P.snip(infile, ".log")
    compute_file_metrics(
        infile,
        outfile,
        metric="md5sum",
        suffixes=P.as_list(P.as_list(PARAMS.get('%s_regex_md5' % track, ""))))


@transform(run_tests,
           suffix(".log"),
           ".lines")
def buildLineCounts(infile, outfile):
    '''compute line counts.

    Files are uncompressed before computing the number of lines.
    '''
    track = P.snip(infile, ".log")
    compute_file_metrics(
        infile,
        outfile,
        metric="wc -l",
        suffixes=P.as_list(P.as_list(
            PARAMS.get('%s_regex_linecount' % track, ""))))


@transform(run_tests,
           suffix(".log"),
           ".exist")
def checkFileExistence(infile, outfile):
    '''check whether file exists.

    Files are uncompressed before checking existence.
    '''
    track = P.snip(infile, ".log")
    compute_file_metrics(
        infile,
        outfile,
        metric="file",
        suffixes=P.as_list(P.as_list(PARAMS.get('%s_regex_exist' % track, ""))))


@collate((buildCheckSums, buildLineCounts, checkFileExistence),
         regex("([^.]*).(.*)"),
         r"\1.stats")
def mergeFileStatistics(infiles, outfile):
    '''merge all file statistics.'''

    to_cluster = False
    infiles = " ".join(sorted(infiles))

    statement = '''
    %(test_scriptsdir)s/merge_testing_output.sh
    %(infiles)s
    > %(outfile)s'''
    P.run(statement)


@merge(mergeFileStatistics,
       "md5_compare.tsv")
def compareCheckSums(infiles, outfile):
    '''compare checksum files against existing reference data.
    '''

    outf = iotools.open_file(outfile, "w")
    outf.write("\t".join((
        ("track", "status",
         "job_finished",
         "nfiles", "nref",
         "missing", "extra",
         "different",
         "different_md5",
         "different_lines",
         "same",
         "same_md5",
         "same_lines",
         "same_exist",
         "files_missing",
         "files_extra",
         "files_different_md5",
         "files_different_lines"))) + "\n")

    for infile in infiles:
        E.info("working on {}".format(infile))
        track = P.snip(infile, ".stats")

        logfiles = glob.glob(track + "*.log")
        job_finished = True
        for logfile in logfiles:
            is_complete = iotools.is_complete(logfile)
            E.debug("logcheck: {} = {}".format(logfile, is_complete))
            job_finished = job_finished and is_complete

        reffile = track + ".ref"

        # regular expression of files to test only for existence
        regex_exist = PARAMS.get('%s_regex_exist' % track, None)
        if regex_exist:
            regex_exist = re.compile("|".join(P.as_list(regex_exist)))

        regex_linecount = PARAMS.get('%s_regex_linecount' % track, None)
        if regex_linecount:
            regex_linecount = re.compile("|".join(P.as_list(regex_linecount)))

        regex_md5 = PARAMS.get('%s_regex_md5' % track, None)
        if regex_md5:
            regex_md5 = re.compile("|".join(P.as_list(regex_md5)))

        if not os.path.exists(reffile):
            raise ValueError('no reference data defined for %s' % track)

        cmp_data = pandas.read_csv(iotools.open_file(infile),
                                   sep="\t",
                                   index_col=0)

        ref_data = pandas.read_csv(iotools.open_file(reffile),
                                   sep="\t",
                                   index_col=0)

        shared_files = set(cmp_data.index).intersection(ref_data.index)
        missing = set(ref_data.index).difference(cmp_data.index)
        extra = set(cmp_data.index).difference(ref_data.index)

        different = set(shared_files)

        # remove those for which only check for existence
        if regex_exist:
            same_exist = set([x for x in different
                              if regex_exist.search(x)])

            different = set([x for x in different
                             if not regex_exist.search(x)])
        else:
            same_exist = set()

        # select those for which only check for number of lines
        if regex_linecount:
            check_lines = [x for x in different
                           if regex_linecount.search(x)]

            dd = (cmp_data['nlines'][check_lines] !=
                  ref_data['nlines'][check_lines])
            different_lines = set(dd.index[dd])
            different = different.difference(check_lines)

            dd = (cmp_data['nlines'][check_lines] ==
                  ref_data['nlines'][check_lines])
            same_lines = set(dd.index[dd])

        else:
            different_lines = set()
            same_lines = set()

        # remainder - check md5
        if regex_md5:
            check_md5 = [x for x in different
                         if regex_md5.search(x)]

            dd = (cmp_data['md5'][check_md5] !=
                  ref_data['md5'][check_md5])
            different_md5 = set(dd.index[dd])

            dd = (cmp_data['md5'][check_md5] ==
                  ref_data['md5'][check_md5])
            same_md5 = set(dd.index[dd])

        else:
            different_md5 = set()
            same_md5 = set()

        if job_finished and (len(missing) + len(extra) +
                             len(different_md5) + len(different_lines) == 0):
            status = "OK"
        else:
            status = "FAIL"

        outf.write("\t".join(map(str, (
            track,
            status,
            job_finished,
            len(cmp_data),
            len(ref_data),
            len(missing),
            len(extra),
            len(different_md5) + len(different_lines),
            len(different_md5),
            len(different_lines),
            len(same_md5) + len(same_lines) + len(same_exist),
            len(same_md5),
            len(same_lines),
            len(same_exist),
            ",".join(missing),
            ",".join(extra),
            ",".join(different_md5),
            ",".join(different_lines),
        ))) + "\n")

    outf.close()


@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@transform(compareCheckSums,
           suffix(".tsv"),
           ".load")
def loadComparison(infile, outfile):
    '''load comparison data into database.'''
    P.load(infile, outfile)


@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@transform(mergeFileStatistics,
           suffix(".stats"),
           "_results.load")
def loadResults(infile, outfile):
    '''load comparison data into database.'''
    P.load(infile, outfile, options="--add-index=filename")


@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@transform(mergeFileStatistics,
           suffix(".ref"),
           "_reference.load")
def loadReference(infile, outfile):
    '''load comparison data into database.'''
    P.load(infile, outfile, options="--add-index=filename")


@follows(run_tests)
def run_components():
    pass


@follows(run_components, loadComparison, loadResults, loadReference)
def full():
    pass


@files(None, 'reset.log')
def reset(infile, outfile):
    '''remove all data in pipeline.'''

    to_cluster = False

    statement = '''
    rm -rf prereq_* ctmp* &&
    rm -rf test_* _cache _static _templates _tmp report &&
    rm -f *.log csvdb *.load *.tsv'''
    P.run(statement)

###################################################################
###################################################################
###################################################################
# primary targets
###################################################################


def renderJupyter():
    '''builds a Jupyter report of csvdb output'''

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                  "pipeline_docs",
                                  "pipeline_testing"))

    statement = '''cp %(report_path)s/* . &&
                   jupyter nbconvert
                           --ExecutePreprocessor.timeout=None
                           --to html
                           --execute *.ipynb
                           --allow-errors'''

    P.run(statement)


@follows(renderJupyter)
def build_report():
    '''dummy task to build report'''

    pass


def main(argv=None):
    workflow_options = []
    if "--local" in argv:
        workflow_options.append("--local")
    workflow_options.append("-p {}".format(P.get_params()["cluster"]["num_jobs"]))

    P.get_params()["workflow_options"] == "".join(workflow_options)
    # manually set location of test scripts - this needs to be better organized
    # 1. make scripts live alongside pipeline_testing.py
    # 2. make scripts available via cgatflow CLI
    # 3. include scripts in pipeline_testing
    P.get_params()["test_scriptsdir"] = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts")
    P.main(argv)


if __name__ == "__main__":
    sys.exit(main())
