#!/bin/bash

#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

ex=$@
tm=$(date +%Y%m%d%H%M%S)
logfile="./testlogs/$tm"

if [ -z "$ex" ]; then
    echo "No executable found"
    exit 1
fi

mkdir -p ./testlogs

$ex > $logfile 2>&1
ret=$(grep ERROR $logfile)


if [ ! -z "$ret" ]; then
    echo "ERROR: Test Failed"
    cat $logfile
    exit 1
else 
    echo "Test Passed"
    rm -f $logfile
fi
