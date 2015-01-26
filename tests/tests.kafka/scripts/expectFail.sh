#!/bin/bash

#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

ex=$@

if [ -z "$ex" ]; then
    echo "No executable found"
    exit 1
fi

$ex
ret=$?

if [ $ret -eq 0 ]; then
    echo "ERROR: Test Failed. Command passed but was expected to fail"
    exit 1
else
    echo "Test Passed. Command has failed as expected"
fi
