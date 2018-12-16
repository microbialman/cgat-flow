"""
===========================
Pipeline bamstats
===========================

:Author: Adam Cribbs
:Release: $Id$
:Date: |today|
:Tags: Python

The intention of this pipeline is to perform QC statistics on
   `.bam` files that are produced following mapping of fastq
   files.

The pipeline requires a `.bam` file as an input.

Overview
========

The pipeline perform the following stats in each folder:
    * IdxStats     -Samtools idxstats is ran and this calculates
                    the number of mapped and unmapped reads per contig.
    * BamStats     -This is a cgat script (bam2stats) that performs stats
                    on a bam file and outputs alignment statistics.
    * PicardStats  -this runs to CollectRnaSeqMetrics picard tools.
    * StrandSpec   -Gives a measure of the proportion of reads that map to
                    each strand. Is used to work out strandness of library
                    if unknown.
    * nreads       -Calculates the number of reads in the bam file.
    * Paired_QC    -This contains metrics that are only required for paired
                    end. Most of the statistics collate metrics regarding
                    splicing.
                    Transcript profile is across the upstream,exons and
                    downstream because this is usually specific to rna seq
                    analysis. ### May need to remove this to make it single ended...........



This pipeline computes the word frequencies in the configuration
files :file:``pipeline.yml` and :file:`conf.py`.

Usage
=====

See :ref:`PipelineSettingUp` and :ref:`PipelineRunning` on general
information how to use cgat pipelines.


Configuration
-------------

This pipeline requires the user to run pipeline_gtf_subset.py. The
location of the database then needs to be set in the pipeline.yml
file.

The pipeline requires a configured :file:`pipeline.yml` file.
cgatReport report requires a :file:`conf.py` and optionally a
:file:`cgatreport.yml` file (see :ref:`PipelineReporting`).

Default configuration files can be generated by executing:

   python <srcdir>/pipeline_bamstats.py config

Input files
-----------

The pipeline configuration files need to be generated by running:

   python <srcdir>/pipeline_bamstats.py config

Once the config file  (pipeline.yml) is generated this should be modified
before running the pipeline.


The pipeline requires `.bam` files to be loacted within the same directory
that the piepline is ran.

Requirements
------------

The pipeline requires the gtf file produced from
:doc:`pipeline_gtf_subset`. Set the configuration variable
:py:data:`gtf_database`.

On top of the default cgat setup, the pipeline requires the following
software to be in the path:


+--------------+----------+------------------------------------+
|*Program*     |*Version* |*Purpose*                           |
+--------------+----------+------------------------------------+
|samtools      |>=0.1.16  |bam/sam files                       |
+--------------+----------+------------------------------------+
|cgat tools    |          |bam2stats script                    |
+--------------+----------+------------------------------------+
|picard        |>=1.42    |bam/sam files. The .jar files need  |
|              |          |to be in your CLASSPATH environment |
|              |          |variable.                           |
+--------------+----------+------------------------------------+
|bamstats_     |>=1.22    |from CGR, Liverpool                 |
+--------------+----------+------------------------------------+




Pipeline output
===============

The major output of the pipeline is the database file :file:`csvdb`.

SQL query of this database forms the basis of the final reports.

The following reports are generated as part of running:

    python <srcdir>/pipeline_bamstats.py make build_report

    * Jupyter notebook - a python implimentation. The output files
                         are located in Jupyter_report.dir. To view
                         the report open the _site/cgat_FULL_BAM_STATS_REPORT.html.
                         You can navigate throught the various report
                         pages through here.

    * Rmarkdown        - an R markdown report implimentation.The output
                         report os located in the R_report.dir/_site
                         directory and can be accessed by opening any of
                         the html files.

    * multiQC          - this builds a basic report using the multiqc -
                         http://multiqc.info/ external tool. There is the
                         potential for customising multiQC so it can be used
                         to generate reports from cgat tools, however at presnt this
                         is not possible because of development stage of multiQC.

Example
=======

Example data is available at:
..........Add data...............

python <srcdir>/pipeline_bamstats.py config
python <srcdir>/pipeline_bamstats.py make full


Glossary
========

.. glossary::

.. _bamstats: http://www.agf.liv.ac.uk/454/sabkea/samStats_13-01-2011


Code
====

"""

# load modules for use in the pipeline

import re
import sys
import os
import sqlite3
import cgatcore.iotools as iotools

from ruffus import transform, merge, mkdir, regex, suffix, follows, add_inputs,\
    active_if, jobs_limit, originate

import cgatcore.pipeline as P
import cgatpipelines.tasks.bamstats as bamstats


# load options from the config file
P.get_parameters(
    ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
     "../pipeline.yml",
     "pipeline.yml"])

PARAMS = P.PARAMS

# Add parameters from the gtf_subset pipeline, but
# only the interface section. All PARAMS options
# will have the prefix `annotations_`
PARAMS.update(P.peek_parameters(
    PARAMS["gtf_dir"],
    "genesets",
    prefix="annotations_",
    update_interface=True,
    restrict_interface=True))

# -----------------------------------------------
# Utility functions

PICARD_MEMORY = PARAMS["picard_memory"]


def connect():
    '''utility function to connect to database.

    Use this method to connect to the pipeline database.
    Additional databases can be attached here as well.

    Returns an sqlite3 database handle.
    '''

    dbh = sqlite3.connect(PARAMS["database_name"])

    if not os.path.exists(PARAMS["gtf_database"]):
        raise ValueError(
            "can't find database '%s'" %
            PARAMS["gtf_database"])

    statement = '''ATTACH DATABASE '%s' as annotations''' % \
                (PARAMS["gtf_database"])

    cc = dbh.cursor()
    cc.execute(statement)
    cc.close()

    return dbh

# Determine whether the gemone is paired


SPLICED_MAPPING = PARAMS["bam_paired_end"]


#########################################################################
# Count reads as some QC targets require it
#########################################################################


@follows(mkdir("nreads.dir"))
@transform("*.bam",
           suffix(".bam"),
           r"nreads.dir/\1.nreads")
def countReads(infile, outfile):
    '''Count number of reads in input files.'''

    statement = '''printf "nreads \\t" >> %(outfile)s'''

    P.run(statement)

    statement = '''samtools view %(infile)s | wc -l | xargs printf >> %(outfile)s'''

    P.run(statement)

#########################################################################
# QC tasks start here
#########################################################################


@follows(mkdir("StrandSpec.dir"))
@transform("*.bam",
           suffix(".bam"),
           r"StrandSpec.dir/\1.strand")
def strandSpecificity(infile, outfile):
    '''This function will determine the strand specificity of your library
    from the bam file'''

    statement = (
        "cgat bam2libtype "
        "--max-iterations 10000 "
        "< {infile} "
        "> {outfile}".format(**locals()))
    return P.run(statement)


@follows(mkdir("BamFiles.dir"))
@transform("*.bam",
           regex("(.*).bam$"),
           r"BamFiles.dir/\1.bam")
def intBam(infile, outfile):
    '''make an intermediate bam file if there is no sequence infomation.
    If there is no sequence quality then make a softlink. Picard tools
    has an issue when quality score infomation is missing'''

    if PARAMS["bam_sequence_stripped"] is True:
        bamstats.addPseudoSequenceQuality(infile,
                                          outfile)
    else:
        bamstats.copyBamFile(infile,
                             outfile)


@follows(mkdir("Picard_stats.dir"))
@P.add_doc(bamstats.buildPicardAlignmentStats)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(os.path.join(PARAMS["genome_dir"],
                                   PARAMS["genome"] + ".fa")),
           r"Picard_stats.dir/\1.picard_stats")
def buildPicardStats(infiles, outfile):
    ''' build Picard alignment stats '''
    infile, reffile = infiles

    # patch for mapping against transcriptome - switch genomic reference
    # to transcriptomic sequences
    if "transcriptome.dir" in infile:
        reffile = "refcoding.fa"

    bamstats.buildPicardAlignmentStats(infile,
                                       outfile,
                                       reffile,
                                       PICARD_MEMORY)


@P.add_doc(bamstats.buildPicardInsertSizeStats)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(os.path.join(PARAMS["genome_dir"],
                                   PARAMS["genome"] + ".fa")),
           r"Picard_stats.dir/\1.insert_stats")
def buildPicardInserts(infiles, outfile):
    ''' build Picard alignment stats '''
    infile, reffile = infiles

    if "transcriptome.dir" in infile:
        reffile = "refcoding.fa"

    bamstats.buildPicardInsertSizeStats(infile,
                                        outfile,
                                        reffile,
                                        PICARD_MEMORY)


@P.add_doc(bamstats.buildPicardDuplicationStats)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           r"Picard_stats.dir/\1.picard_duplication_metrics")
def buildPicardDuplicationStats(infile, outfile):
    '''Get duplicate stats from picard MarkDuplicates '''
    bamstats.buildPicardDuplicationStats(infile, outfile,
                                         PICARD_MEMORY)


@follows(mkdir("BamStats.dir"))
@follows(countReads)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(r"nreads.dir/\1.nreads"),
           r"BamStats.dir/\1.readstats")
def buildBAMStats(infiles, outfile):
    '''count number of reads mapped, duplicates, etc.

    Excludes regions overlapping repetitive RNA sequences

    Parameters
    ----------
    infiles : list
    infiles[0] : str
       Input filename in :term:`bam` format
    infiles[1] : str
       Input filename with number of reads per sample

    outfile : str
       Output filename with read stats

    annotations_interface_rna_gtf : str
        :term:`PARMS`. :term:`gtf` format file with repetitive rna
    '''
    rna_file = PARAMS["annotations_interface_rna_gff"]

    job_memory = "32G"

    bamfile, readsfile = infiles

    nreads = bamstats.getNumReadsFromReadsFile(readsfile)
    track = P.snip(os.path.basename(readsfile),
                   ".nreads")

    # if a fastq file exists, submit for counting
    if os.path.exists(track + ".fastq.gz"):
        fastqfile = track + ".fastq.gz"
    elif os.path.exists(track + ".fastq.1.gz"):
        fastqfile = track + ".fastq.1.gz"
    else:
        fastqfile = None

    if fastqfile is not None:
        fastq_option = "--fastq-file=%s" % fastqfile
    else:
        fastq_option = ""

    statement = '''
    cgat bam2stats
         %(fastq_option)s
         --force-output
         --mask-bed-file=%(rna_file)s
         --ignore-masked-reads
         --num-reads=%(nreads)i
         --output-filename-pattern=%(outfile)s.%%s
    < %(bamfile)s
    > %(outfile)s
    '''

    P.run(statement)


@follows(intBam)
@transform(PARAMS["annotations_interface_genomic_context_bed"],
           regex("^\/(.+\/)*(.+).bed.gz"),
           r"BamStats.dir/\2.bed.gz")
def processGenomicContext(infile, outfile):
    '''
    This module process genomic context file.
    It assigns each and every features of context
    file to a specific catagory. It helps us to
    understand hiearchical classification
    of features.
    '''
    bamstats.defineBedFeatures(infile, outfile)


@P.add_doc(bamstats.summarizeTagsWithinContext)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(processGenomicContext),
           r"BamStats.dir/\1.contextstats.tsv.gz")
def buildContextStats(infiles, outfile):
    ''' build mapping context stats '''
    bamstats.summarizeTagsWithinContext(
        infiles[0], infiles[1], outfile,
        job_memory="8G")


@follows(mkdir("IdxStats.dir"))
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           r"IdxStats.dir/\1.idxstats")
def buildIdxStats(infile, outfile):
    '''gets idxstats for bam file so number of reads per chromosome can
    be plotted later'''

    statement = '''samtools idxstats %(infile)s > %(outfile)s'''

    P.run(statement)


@follows(mkdir("SamtoolsStats.dir"))
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           r"SamtoolsStats.dir/\1.samtoolsstats")
def buildSamtoolsStats(infile, outfile):
    '''gets idxstats for bam file so number of reads per chromosome can
    be plotted later'''

    statement = '''samtools stats %(infile)s > %(outfile)s'''

    P.run(statement)

# ------------------------------------------------------------------
# QC specific to spliced mapping
# ------------------------------------------------------------------


@follows(mkdir("Paired_QC.dir"))
@active_if(SPLICED_MAPPING)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(PARAMS["annotations_interface_geneset_coding_exons_gtf"]),
           r"Paired_QC.dir/\1.exon.validation.tsv.gz")
def buildExonValidation(infiles, outfile):
    '''Compare the alignments to the exon models to quantify exon
    overrun/underrun

    Expectation is that reads should not extend beyond known exons.

    Parameters
    ----------
    infiles : list
    infiles[0] : str
       Input filename in :term:`bam` format
    infiles[1] : str
       Input filename in :term:`gtf` format

    outfile : str
       Output filename in :term:`gtf` format with exon validation stats
    '''

    infile, exons = infiles
    statement = '''cat %(infile)s
    | cgat bam_vs_gtf
         --exons-file=%(exons)s
         --force-output
         --log=%(outfile)s.log
         --output-filename-pattern="%(outfile)s.%%s.gz"
    | gzip
    > %(outfile)s
    '''

    P.run(statement)


@active_if(SPLICED_MAPPING)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(PARAMS["annotations_interface_geneset_coding_exons_gtf"]),
           r"Paired_QC.dir/\1.transcript_counts.tsv.gz")
def buildTranscriptLevelReadCounts(infiles, outfile):
    '''count reads in gene models

    Count the reads from a :term:`bam` file which overlap the
    positions of protein coding transcripts in a :term:`gtf` format
    transcripts file.

    Parameters
    ----------
    infiles : list of str
    infiles[0] : str
       Input filename in :term:`bam` format
    infiles[1] : str
       Input filename in :term:`gtf` format

    outfile : str
       Output filename in :term:`tsv` format


    .. note::
       In paired-end data sets each mate will be counted. Thus
       the actual read counts are approximately twice the fragment
       counts.

    '''
    infile, geneset = infiles

    job_memory = "8G"

    statement = '''
    zcat %(geneset)s
    | cgat gtf2table
    --reporter=transcripts
    --bam-file=%(infile)s
    --counter=length
    --column-prefix="exons_"
    --counter=read-counts
    --column-prefix=""
    --counter=read-coverage
    --column-prefix=coverage_
    -v 0
    | gzip
    > %(outfile)s
    ''' % locals()

    P.run(statement)


@active_if(SPLICED_MAPPING)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(PARAMS["annotations_interface_geneset_intron_gtf"]),
           r"Paired_QC.dir/\1.intron_counts.tsv.gz")
def buildIntronLevelReadCounts(infiles, outfile):
    '''count reads in gene models
    Count the reads from a :term:`bam` file which overlap the
    positions of introns in a :term:`gtf` format transcripts file.
    Parameters
    ----------
    infiles : list of str
       infile :term:`str`
          Input filename in :term:`bam` format
       geneset :term:`str`
          Input filename in :term:`gtf` format
    outfile : str
       Output filename in :term:`tsv` format
    .. note::
       In paired-end data sets each mate will be counted. Thus
       the actual read counts are approximately twice the fragment
       counts.
    '''
    infile, exons = infiles

    job_memory = "4G"

    if "transcriptome.dir" in infile:
        iotools.touch_file(outfile)
        return

    statement = '''
    zcat %(exons)s
    | awk -v OFS="\\t" -v FS="\\t" '{$3="exon"; print}'
    | cgat gtf2table
          --reporter=genes
          --bam-file=%(infile)s
          --counter=length
          --column-prefix="introns_"
          --counter=read-counts
          --column-prefix=""
          --counter=read-coverage
          --column-prefix=coverage_
    | gzip
    > %(outfile)s
    '''

    P.run(statement)


@active_if(SPLICED_MAPPING)
@transform(intBam,
           regex("BamFiles.dir/(\S+).bam$"),
           add_inputs(PARAMS["annotations_interface_geneset_coding_exons_gtf"]),
           r"Paired_QC.dir/\1.transcriptprofile.gz")
def buildTranscriptProfiles(infiles, outfile):
    '''build gene coverage profiles

    PolyA-RNA-Seq is expected to show a bias towards the 3' end of
    transcripts. Here we generate a meta-profile for each sample for
    the read depth from the :term:`bam` file across the gene models
    defined in the :term:`gtf` gene set

    In addition to the outfile specified by the task, plots will be
    saved with full and focus views of the meta-profile

    Parameters
    ----------
    infiles : list of str
    infiles[0] : str
       Input filename in :term:`bam` format
    infiles[1] : str`
       Input filename in :term:`gtf` format

    outfile : str
       Output filename in :term:`tsv` format
    '''

    bamfile, gtffile = infiles

    job_memory = "8G"

    statement = '''cgat bam2geneprofile
    --output-filename-pattern="%(outfile)s.%%s"
    --force-output
    --reporter=transcript
    --use-base-accuracy
    --method=geneprofileabsolutedistancefromthreeprimeend
    --normalize-profile=all
    %(bamfile)s %(gtffile)s
    | gzip
    > %(outfile)s
    '''

    P.run(statement)


@active_if(SPLICED_MAPPING)
@P.add_doc(bamstats.buildPicardRnaSeqMetrics)
@transform(intBam,
           regex("BamFiles.dir/(.*).bam$"),
           add_inputs(PARAMS["annotations_interface_ref_flat"]),
           r"Picard_stats.dir/\1.picard_rna_metrics")
def buildPicardRnaSeqMetrics(infiles, outfile):
    '''Get duplicate stats from picard RNASeqMetrics '''
    # convert strandedness to PICARD library type
    if PARAMS["strandedness"] == ("RF" or "R"):
        strand = "SECOND_READ_TRANSCRIPTION_STRAND"
    elif PARAMS["strandedness"] == ("FR" or "F"):
        strand = "FIRST_READ_TRANSCRIPTION_STRAND"
    else:
        strand = "NONE"
    bamstats.buildPicardRnaSeqMetrics(infiles, strand, outfile,
                                      PICARD_MEMORY)


##########################################################################
# Database loading statements
##########################################################################


@P.add_doc(bamstats.loadPicardAlignmentStats)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@merge(buildPicardStats, "Picard_stats.dir/picard_stats.load")
def loadPicardStats(infiles, outfile):
    '''merge alignment stats into single tables.'''
    bamstats.loadPicardAlignmentStats(infiles, outfile)


@P.add_doc(bamstats.loadPicardDuplicationStats)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@merge(buildPicardDuplicationStats, ["picard_duplication_stats.load",
                                     "picard_duplication_histogram.load"])
def loadPicardDuplicationStats(infiles, outfiles):
    '''merge alignment stats into single tables.'''

    bamstats.loadPicardDuplicationStats(infiles, outfiles)


@P.add_doc(bamstats.loadBAMStats)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@merge(buildBAMStats, "bam_stats.load")
def loadBAMStats(infiles, outfile):
    ''' load bam statistics into bam_stats table '''
    bamstats.loadBAMStats(infiles, outfile)


@P.add_doc(bamstats.loadSummarizedContextStats)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@follows(loadBAMStats)
@merge(buildContextStats, "context_stats.load")
def loadContextStats(infiles, outfile):
    ''' load context mapping statistics into context_stats table '''
    bamstats.loadSummarizedContextStats(infiles, outfile)


@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@merge(buildIdxStats, "idxstats_reads_per_chromosome.load")
def loadIdxStats(infiles, outfile):
    '''merge idxstats files into single dataframe and load
    to database

    Loads tables into the database
       * mapped_reads_per_chromosome

    Arguments
    ---------
    infiles : list
        list where each element is a string of the filename containing samtools
        idxstats output. Filename format is expected to be 'sample.idxstats'
    outfile : string
        Logfile. The table name will be derived from `outfile`.'''

    bamstats.loadIdxstats(infiles, outfile)


@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@active_if(SPLICED_MAPPING)
@merge(buildExonValidation, "exon_validation.load")
def loadExonValidation(infiles, outfile):
    ''' load individual and merged exon validation stats

    For each sample, the exon validation stats are loaded into a table
    named by sample and mapper
    [sample]_[mapper]_overrun

    The merge alignment stats for all samples are merged and loaded
    into single table called exon_validation

    Parameters
    ----------
    infiles : list
       Input filenames with exon validation stats
    outfile : str
       Output filename
    '''

    suffix = ".exon.validation.tsv.gz"

    P.merge_and_load(infiles, outfile, suffix=suffix)
    for infile in infiles:
        track = P.snip(infile, suffix)
        o = "%s_overrun.load" % track
        P.load(infile + ".overrun.gz", o)


@P.add_doc(bamstats.loadPicardRnaSeqMetrics)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@merge(buildPicardRnaSeqMetrics, ["picard_rna_metrics.load",
                                  "picard_rna_histogram.load"])
def loadPicardRnaSeqMetrics(infiles, outfiles):
    '''merge alignment stats into single tables.'''
    bamstats.loadPicardRnaSeqMetrics(infiles, outfiles)


@P.add_doc(bamstats.loadCountReads)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@follows(loadPicardRnaSeqMetrics)
@merge(countReads, "count_reads.load")
def loadCountReads(infiles, outfile):
    ''' load read counts count_reads table '''
    bamstats.loadCountReads(infiles, outfile)


@active_if(SPLICED_MAPPING)
@P.add_doc(bamstats.loadTranscriptProfile)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@follows(loadCountReads)
@merge(buildTranscriptProfiles, "transcript_profile.load")
def loadTranscriptProfile(infiles, outfile):
    ''' merge transcript profiles into a single table'''
    bamstats.loadTranscriptProfile(infiles, outfile)


@merge(buildPicardInserts, "picard_insert_metrics.csv")
def mergePicardInsertMetrics(infiles, outfile):
    ''' merge insert stats into a single table'''
    bamstats.mergeInsertSize(infiles, outfile)


@P.add_doc(bamstats.loadStrandSpecificity)
@jobs_limit(PARAMS.get("jobs_limit_db", 1), "db")
@follows(loadTranscriptProfile)
@merge(strandSpecificity, "strand_spec.load")
def loadStrandSpecificity(infiles, outfile):
    ''' merge strand specificity data into a single table'''
    bamstats.loadStrandSpecificity(infiles, outfile)


@merge((loadBAMStats, loadPicardStats, loadContextStats), "view_mapping.load")
def createViewMapping(infile, outfile):
    '''create view in database for alignment stats.

    This view aggregates all information on a per-track basis.

    The table is built from the following tracks:

       context_stats
       bam_stats
    '''

    dbh = connect()

    tablename = P.to_table(outfile)
    view_type = "TABLE"
    tables = (("bam_stats", "track", ),
              ("context_stats", "track", ))

    # do not use: ("picard_stats_alignment_summary_metrics", "track"),)
    # as there are multiple rows per track for paired-ended data.

    P.create_view(dbh, tables, tablename, outfile, view_type)


@follows(createViewMapping)
def views():
    pass

# ---------------------------------------------------
# Generic pipeline tasks
# These tasks allow ruffus to pipeline tasks together


@follows(buildTranscriptProfiles,
         loadPicardStats,
         loadPicardDuplicationStats,
         loadBAMStats,
         loadContextStats,
         buildIntronLevelReadCounts,
         loadIdxStats,
         loadExonValidation,
         loadPicardRnaSeqMetrics,
         loadTranscriptProfile,
         loadStrandSpecificity,
         mergePicardInsertMetrics)
def full():
    '''a dummy task to run all tasks in the pipeline'''
    pass


# --------------------------------------------------
# Reporting tasks
# --------------------------------------------------
@follows(mkdir("R_report.dir"))
def renderRreport():
    '''build R markdown report '''

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               'pipeline_docs',
                                               'pipeline_bamstats',
                                               'R_report'))

    statement = '''cp %(report_path)s/* R_report.dir && cd R_report.dir && R -e "rmarkdown::render_site()"'''

    P.run(statement)


@follows(mkdir("Jupyter_report.dir"))
def renderJupyterReport():
    '''build Jupyter notebook report'''

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               'pipeline_docs',
                                               'pipeline_bamstats',
                                               'Jupyter_report'))

    statement = ''' cp %(report_path)s/* Jupyter_report.dir/ && cd Jupyter_report.dir/ &&
                    jupyter nbconvert --ExecutePreprocessor.timeout=None --to html --execute *.ipynb --allow-errors &&
                    mkdir _site &&
                    mv -t _site *.html cgat_logo.jpeg oxford.png'''

    P.run(statement)


@follows(mkdir("MultiQC_report.dir"))
@originate("MultiQC_report.dir/multiqc_report.html")
def renderMultiqc(infile):
    '''build mulitqc report'''

    statement = (
        "export LC_ALL=en_GB.UTF-8 && "
        "export LANG=en_GB.UTF-8 && "
        "multiqc . -f && "
        "mv multiqc_report.html MultiQC_report.dir/")
    P.run(statement)


@follows(renderRreport,
         renderJupyterReport,
         renderMultiqc)
def build_report():
    '''report dummy task to build reports'''
    pass


def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)


if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
