#!/usr/bin/env python

# prepare data for Spark-shared-kmer
# input is fasta files or gzipped fasta files
# output .seq file

import os
import gzip
import sys, getopt
import random

def extract_data(in_file, out_file, paired, shuffle):
    """
    Extract certain number of bases from a list of gzip files
    output format: ID    Seq
    """
    out = open(out_file, 'w')
    seq_list = []
    
    if paired:
        """
            join pairs together with 'NNNN'. use first read ID as the ID
        """
        lineno = 0    
        if in_file[-3:] == ".gz":
            seq = gzip.open(in_file, 'r')
        else:
            seq = open(in_file, 'r')
        for line in seq:
            lineno += 1
            if lineno%4 == 1: 
                seqID = line[1:-1]
            if lineno%4 == 2: 
                seqSeq = line[:-1]
            if lineno%4 == 4:    
                seq_list.append(seqID + "\t" + seqSeq.upper() + 'N' + line[:-1].upper() + "\n")     
        seq.close()
    else:
        lineno = 0  
        if in_file[-3:] == ".gz":
            seq = gzip.open(in_file, 'r')
        else:
            seq = open(in_file, 'r')
        for line in seq:
            lineno += 1
            if lineno%2 == 1: 
                seqID = line[1:-1]
            if lineno%2 == 0: 
                seq_list.append(seqID + "\t" + line[:-1].upper() + "\n")
        seq.close()
    if shuffle:
        random.shuffle(seq_list)
    
    out.writelines(seq_list)
    out.close()

def main(argv):
    in_file = ''
    paired = False
    shuffle = False
    out_file = ''
    help_msg = sys.argv[0] + ' -i <fasta_file> -p <1 is paired, 0 otherwise> -o <outfile> -s <1 to shuffle, 0 otherwise>'
    if len(argv) < 2:
        print help_msg
        sys.exit(2) 
    else:
        try:
            opts, args = getopt.getopt(argv, "hi:p:o:s:", ["in_file=","paired=", "out_file=", "shuffle="])
        except getopt.GetoptError:
            print help_msg
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print help_msg
                sys.exit()
            elif opt in ("-i", "--in_file"):
                in_file = arg
            elif opt in ("-p", "--paired"):
                if int(arg) == 1 :
                    paired = True
            elif opt in ("-s", "--shuffle"):
                if int(arg) == 1 :
                    shuffle = True
            elif opt in ("-o", "--out_file"):
                out_file = arg  
    extract_data(in_file, out_file, paired, shuffle)

if __name__ == "__main__":
    main(sys.argv[1:])




