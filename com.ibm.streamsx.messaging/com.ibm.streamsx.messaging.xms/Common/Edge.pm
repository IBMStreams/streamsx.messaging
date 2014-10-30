package Edge;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################                             
                            

use strict;
use warnings;
use SPL::CodeGen;

my %XMStypeTable = (
					"int8"=>"Byte",
  					"uint8"=>"Short",
	                "int16"=>"Short",
	                "uint16"=>"Int",
	                "int32"=>"Int",
	                "uint32"=>"Long",
	                "int64"=>"Long",
	                "float32"=>"Float",
	                "float64"=>"Double",
	                "boolean"=>"Boolean",
                    "blob"=>"Bytes",
                    "rstring"=>"String"
                    ); 

# Get the connection and access objects named in an operator's parameters.
sub connectionSetup {
    my ($model, $connTag, $accessTag) = @_;

    # Check whether this operator has specified a connectionDocument parameter, which redirects
    # where the Connection and Access modules look for connection and access specifications.
    my @splitted;
    my $connDocName;
    my $connDocParam = $model->getParameterByName('connectionDocument');
    if (defined $connDocParam) {
        my $quotedConnDoc = SPL::CodeGen::compileTimeExpression($model, $connDocParam->getValueAt(0)); 
        @splitted = split(/"/, $quotedConnDoc);
        if (@splitted != 2) {       
            SPL::CodeGen::exitln("Value of parameter \'connectionDocument\' has unexpected format: $quotedConnDoc");
        }
        $connDocName= $splitted[1];
    }
    else {
        $connDocName = "";
    }
    
    # Note: existence of connection parameter in the operator is guarenteed by operator.xml
    my $quotedConnName = $model->getParameterByName('connection')->getValueAt(0)->getSPLExpression();
    @splitted = split(/"/, $quotedConnName);   
    if (@splitted > 2) {       
        SPL::CodeGen::exitln("Value of parameter \'connection\' has unexpected format: $quotedConnName");
    }
    my $connName= $splitted[1];
    
    # Get connection and verify
    my $conn;
    if ($connDocName) {
        eval {
            $conn = Connection->new($connTag, $connName, $connDocName);
        };
    }
    else {
        eval {
            $conn = Connection->new($connTag, $connName);
        };
    }
    if ($@) {
        my $msg = Connection->convertErrorToMessage($@);
        SPL::CodeGen::exitln("ERROR: $msg");
    }
    
    # Note: existence of access parameter in the operator is guarenteed by operator.xml
    my $quotedAccessName = $model->getParameterByName('access')->getValueAt(0)->getSPLExpression();
    @splitted = split(/"/, $quotedAccessName);
    if (@splitted > 2) {       
        SPL::CodeGen::exitln("Value of parameter \'access\' has unexpected format: $quotedAccessName");
    }
    my $accessName= $splitted[1];
    
    # Get access and verify
    my $access;
    if ($connDocName) {
        eval {
            $access = Access->new($accessTag, $accessName, $connDocName);
        };
    }
    else {
        eval {
            $access = Access->new($accessTag, $accessName);
        };
    }
    if ($@) {
        my $msg = Connection->convertErrorToMessage($@);
        SPL::CodeGen::exitln("ERROR: $msg");
    }

    if (! $access->usesConnection($connName)) {
        SPL::CodeGen::exitln("ERROR: Connection specification $connName is not the name of a uses_connection element of access specification $accessName");
    }
    
    return ($conn, $access);
}

# Check the validity of the access-specific parameters from the access specification vs. the operator
# parameters.  Return an (ordered) array of parameter types and values to bind to the markers in the
# access.
# The third parameter is a reference to an array of operator parameter names that are checked by the
# restriction model (operator.xml).
sub resolveParms {
    my ($model, $access, $checkedParmNames) = @_;
    
    my $accessParmValues = [];
    my $accessName = $access->getName();
    my $operClass = $model->getContext()->getClass();

    for (my $i = 0; $i < $model->getNumberOfParameters(); ++$i) {
        my $operatorParm = $model->getParameterAt($i);
        my $operatorParmName = $operatorParm->getName();
        
        # Skip parameters that are specified in operator.xml.
        my $found = 0;
        foreach my $name (@$checkedParmNames) {
            if ($name eq $operatorParmName) {
                $found = 1;
                last;
            }
        }
        if ($found) {
            next;
        }
        
        # Check whether a parameter with this name has been specified in the access specification.
        my $accessParmIndex = $access->getParameterIndexByName($operatorParmName);
        if ($accessParmIndex == -1) {
            SPL::CodeGen::exitln("\'$operatorParmName\': Invalid parameter specified for \'$operClass\' operator, access specification \'$accessName\'");
        }
        
        # Check if we've already seen a parameter with this name.
        if (defined $$accessParmValues[$accessParmIndex]) {
            my $firstIndex = $$accessParmValues[$accessParmIndex]->{_index};
            SPL::CodeGen::exitln("\'$operatorParmName\': Parmeter for access specification \'$accessName\' previously defined (at index $firstIndex)");
        }
        
        # Check that the operator parameter has the correct number of values.
        # Note that currently all query parameters must have exactly one value.
        my $cardinality = $access->getParameterCardinalityAt($accessParmIndex);
        my $numValues = $operatorParm->getNumberOfValues();
        if ($cardinality == -1 && $numValues == 0) {
            SPL::CodeGen::exitln("\'$operatorParmName\': Parameter for access specification \'$accessName\' supplied to \'$operClass\' operator expects at least one value");
            
        }
        elsif ($cardinality >= 0 && $cardinality != $numValues) {
            SPL::CodeGen::exitln("\'$operatorParmName\': Parameter for access specification \'$accessName\' supplied to \'$operClass\' operator expects $cardinality value" . (($cardinality != 1) ? "s" : ""));
            
        }
        
        # Check that the operator parameter has the correct data type.
        my $actualParmType = $operatorParm->getValueAt(0)->getSPLType();
        my $formalParmType = $access->getParameterTypeAt($accessParmIndex);
        if ($actualParmType ne $formalParmType) {
            SPL::CodeGen::exitln("\'$operClass\' operator expects type base \'$formalParmType\' (not \'$actualParmType\') for value of parameter \'$operatorParmName\' from access specification \'$accessName\'");
        }
        
        # The access specification contains parameter value checks (within a range or in a list), but
        # we can only check them for constant parameter values.  Unfortunately, we can't (yet) tell
        # whether an operator parameter value is a constant.
        
        # Passed all the checks - save this actual parameter's type and value in the array.
        $$accessParmValues[$accessParmIndex] = {
                                                _name  => $operatorParmName,
                                                _type  => $operatorParm->getValueAt(0)->getSPLType(),
                                                _value => $operatorParm->getValueAt(0)->getCppExpression(),
                                                _index => $i                                # used in duplicate parameter error message
                                               };
    }
    
    # Check that we have values for all the "formal" parameters specified in the access specification.  There
    # are two boundary cases that we have to allow:
    # 1) A parameter with the same name may appear multiple times in the access spec, but it can appear only
    #    once in the actual operator.  Look for a parameter of the same name earlier in the array and use its
    #    value.
    # 2) If the access spec has a default value for this parameter, use it if no actual parameter was specified.
    for (my $i = 0; $i < $access->getNumberOfParameters(); ++$i) {
        if (exists $$accessParmValues[$i]) {
            next;
        }
        my $ithParmName = $access->getParameterNameAt($i);
        my $found = 0;
        for (my $j = 0; $j < $i; ++$j) {
            if ($ithParmName eq $access->getParameterNameAt($j)) {
                $$accessParmValues[$i] = {
                                          _name  => $$accessParmValues[$j]->{_name},
                                          _type  => $$accessParmValues[$j]->{_type},
                                          _value => $$accessParmValues[$j]->{_value},
                                          _index => $$accessParmValues[$j]->{_index}
                                         };
                $found = 1;
                last;
            }
        }
        if ($found) {
            next;
        }
        if (defined $access->getParameterDefaultAt($i)) {
            $$accessParmValues[$i] = {
                                      _name  => $ithParmName,
                                      _type  => $access->getParameterTypeAt($i),
                                      _value => $access->getParameterDefaultAt($i),
                                      _index => -1
                                     };
            next;
        }
        SPL::CodeGen::exitln("\'$operClass\' operator expects mandatory parameter \'$ithParmName\' for access specification \'$accessName\'");
        
        SPL::CodeGen::println("2: Parameter = " .
                              $$accessParmValues[$i]{_name} . " " .
                              $$accessParmValues[$i]{_type} . " " .
                              $$accessParmValues[$i]{_value} . " " .
                              $$accessParmValues[$i]{_index});
                              

    }
    return $accessParmValues;
}

# This subroutine checks the following restrictions on the input stream, output stream, and Native
# (query result set) schemas for enrich operators.
# 1) No input stream attribute can have the same name as an Native schema attribute, since there is no
#    way for a developer to disambiguate such references.
# 2) Each output stream attribute that does not have an assignment must have the same name and type as
#    an Native schema attribute.
# Returns an array of records of Native schema attributes that will be assigned to output stream
# attributes.
sub restrictEnrichSchemaAttributes {
    my ($access, $istream, $ostream, $operatorClass) = @_;
    
    my %istreamAndQueryNames;              # temporary hash for checking for duplicate attribute names
    foreach my $istreamAttr (@{$istream->getAttributes()}) {
        $istreamAndQueryNames{$istreamAttr->getName()} = -1;
    }
    for (my $i = 0; $i < $access->getNumberOfNativeSchemaAttributes(); ++$i) {
        my $extschemaName = $access->getNativeSchemaAttributeNameAt($i);
        if (exists $istreamAndQueryNames{$extschemaName}) {
            SPL::CodeGen::exitln("\'$operatorClass\' operator has an attribute named \'$extschemaName\' " .
                                 "in both its input stream and in the native schema for access specification \'" .
                                 $access->getName() . "\'");
        }
        else {
            $istreamAndQueryNames{$extschemaName} = $i;
        }
    }
    
    my $ostreamAttrsFromQuery = [];
    for (my $i = 0, my $j = 0; $i < $ostream->getNumberOfAttributes(); ++$i) {
        my $outputAttr = $ostream->getAttributeAt($i);
        if (!$outputAttr->hasAssignment()) {
            if (exists $istreamAndQueryNames{$outputAttr->getName()}) {
                # This must also be the name of an native schema attribute, since there would have been an
                # automatically generated assignment if it were the name of an input stream attribute.
                my $extSchemaAttrIndex = $istreamAndQueryNames{$outputAttr->getName()};
                if ($XMStypeTable{$outputAttr->getSPLType()} ne $access->getNativeSchemaAttributeTypeAt($extSchemaAttrIndex)) {
                    SPL::CodeGen::exitln("\'$operatorClass\' operator: type mismatch between output stream attribute \'" .
                                         $outputAttr->getName() . "\' and the native schema attribute of the same name " .
                                         "in access specification \'" . $access->getName() . "\'");
                }
                @$ostreamAttrsFromQuery[$j] = {_name          => $outputAttr->getName(),
                                               _type          => $outputAttr->getSPLType(),
                                               _length        => $access->getNativeSchemaAttributeLengthAt($extSchemaAttrIndex),
                                               _queryColIndex => $extSchemaAttrIndex + 1};
                ++$j;
            }
            else {
                # No code generator assignment, and no matching native schema attribute - error.
                SPL::CodeGen::exitln("\'$operatorClass\' operator: no matching attribute name found in the " .
                                     "input stream or in the native schema of access specification \'" .
                                     $access->getName() . "\' for the output attribute \'" . $outputAttr->getName() . "\'");
            }
        }
    }
    
    return $ostreamAttrsFromQuery;
}

# This subroutines checks the following restriction on the output stream and native (query result
# set) schemas for sink operators.
#   Each output stream attribute that does not have an assignment must have the same name and type as
#   an native schema attribute.
# Returns an array of records of native schema attributes that will be assigned to output stream
# attributes.
sub restrictSourceSchemaAttributes {
    my ($access, $ostream, $operatorClass) = @_;
    
    my %extSchemaNames;              # temporary hash for looking up native schema attribute names
    for (my $i = 0; $i < $access->getNumberOfNativeSchemaAttributes();  ++$i) {
        $extSchemaNames{$access->getNativeSchemaAttributeNameAt($i)} = $i;
    }
    my $ostreamAttrsFromQuery = [];
    for (my $i = 0, my $j = 0; $i < $ostream->getNumberOfAttributes(); ++$i) {
        my $outStreamAttr = $ostream->getAttributeAt($i);
        ### Work around bug in Attribute::hasAssignment, which always returns true.
        ###my $outAttrAssignment = $outputAttr->getAssignment();
        ###if (defined($outAttrAssignment) && !ref($outAttrAssignment)) {
        if (!$outStreamAttr->hasAssignment()) {
            my $outStreamAttrName = $outStreamAttr->getName();
            if (exists $extSchemaNames{$outStreamAttrName}) {
                my $extSchemaAttrIndex = $extSchemaNames{$outStreamAttrName};
                my $outStreamAttrType = $outStreamAttr->getSPLType();
                if ($XMStypeTable{$outStreamAttrType} ne $access->getNativeSchemaAttributeTypeAt($extSchemaAttrIndex)) {
                    SPL::CodeGen::exitln("\'$operatorClass\' operator: type mismatch between output stream attribute " .
                                         "\'$outStreamAttrName\' and the native schema attribute of the same name " .
                                         "in access specification \'" . $access->getName() . "\'");
                }
                else {
                    @$ostreamAttrsFromQuery[$j] = {
                                                   _name          => $outStreamAttrName,
                                                   _type          => $outStreamAttrType,
                                                   _length        => $access->getNativeSchemaAttributeLengthAt($extSchemaAttrIndex),
                                                   _queryColIndex => $extSchemaAttrIndex + 1};
                    ++$j;
                }
            }
            else {
                # No code generator assignment, and no matching native schema attribute - error.
                SPL::CodeGen::exitln("\'$operatorClass\' operator: no matching attribute name found in the " .
                                     "native schema of access specification \'" . $access->getName() .
                                     "\' for the output attribute \'$outStreamAttrName\'");
            }
        }        
    }

    return $ostreamAttrsFromQuery;
}

1;
