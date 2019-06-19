#!/bin/bash 

BASEDIR=$(dirname "$0")
$BASEDIR/run_kc.sh 1 && \
$BASEDIR/run_kmr.sh 1 && \
$BASEDIR/run_edges.sh 1 && \
$BASEDIR/run_lpa.sh 1 && \
$BASEDIR/run_lpa_add_seq.sh  1


