#!/bin/bash
buildcmd="./make.sh analysis"
now=`date +%Y%m%d_%H%M`
outfile="fortify_$now.pdf"

cd ..
rm build.log* 
sourceanalyzer -b test -clean
sourceanalyzer -verbose -b test -debug -logfile ./build.log touchless $buildcmd
sourceanalyzer -verbose -b test -scan -f $now.fpr
ReportGenerator -format pdf -f "$outfile" -source $now.fpr

