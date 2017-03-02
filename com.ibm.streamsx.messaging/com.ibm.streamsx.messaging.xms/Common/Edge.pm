package Edge;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################                             
                            

use strict;
use warnings;
use SPL::CodeGen;

require MessagingResource;


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
            SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_HAS_UNEXPECTED_FORMAT('connectionDocument', $quotedConnDoc));
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
    	SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_HAS_UNEXPECTED_FORMAT('connection', $quotedConnName));
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
    	SPL::CodeGen::exitln(MessagingResource::MSGTK_ERROR_WITH_MESSAGE($msg));
    }
    
    # Note: existence of access parameter in the operator is guarenteed by operator.xml
    my $quotedAccessName = $model->getParameterByName('access')->getValueAt(0)->getSPLExpression();
    @splitted = split(/"/, $quotedAccessName);
    if (@splitted > 2) {       
    	SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_HAS_UNEXPECTED_FORMAT('access', $quotedAccessName));
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
        SPL::CodeGen::exitln(MessagingResource::MSGTK_ERROR_WITH_MESSAGE($msg));
    }

    if (! $access->usesConnection($connName)) {
    	SPL::CodeGen::exitln(MessagingResource::MSGTK_CONNECTION_SPEC_NOT_NAME_OF_USESCONNECTION_IN_ACCESS_SPEC($connName, $accessName));
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
        	SPL::CodeGen::exitln(MessagingResource::MSGTK_INVALID_PARAM_SPECIFIED_FOR_OP_ACCESS_SPEC($operatorParmName, $operClass, $accessName));
        }
        
        # Check if we've already seen a parameter with this name.
        if (defined $$accessParmValues[$accessParmIndex]) {
            my $firstIndex = $$accessParmValues[$accessParmIndex]->{_index};
            SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_FOR_ACCESS_SPEC_PREVIOUSLY_DEFINED($operatorParmName, $accessName, $firstIndex));
        }
        
        # Check that the operator parameter has the correct number of values.
        # Note that currently all query parameters must have exactly one value.
        my $cardinality = $access->getParameterCardinalityAt($accessParmIndex);
        my $numValues = $operatorParm->getNumberOfValues();
        if ($cardinality == -1 && $numValues == 0) {
        	SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_FOR_ACCESS_SPEC_SUPPLIED_TO_OP_EXPECTS_MIN_ONE_VALUE($operatorParmName, $accessName, $operClass));
        }
        elsif ($cardinality >= 0 && $cardinality != $numValues) {
        	SPL::CodeGen::exitln(MessagingResource::MSGTK_PARAM_FOR_ACCESS_SPEC_SUPPLIED_TO_OP_EXPECTS_CARDINALITY_VALUES($operatorParmName, $accessName, $operClass, $cardinality));
        }
        
        # Check that the operator parameter has the correct data type.
        my $actualParmType = $operatorParm->getValueAt(0)->getSPLType();
        my $formalParmType = $access->getParameterTypeAt($accessParmIndex);
        if ($actualParmType ne $formalParmType) {
        	SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_EXPECTS_TYPE_BASE_FOR_PARAM_VALUE_FROM_ACCESS_SPEC($operClass, $formalParmType, $actualParmType, $operatorParmName, $accessName));
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
        SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_EXPECTS_MANDATORY_PARAM_FOR_ACCESS_SPEC($operClass, $ithParmName, $accessName));
        
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
        	SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_HAS_ATTRIB_IN_INPUT_STREAM_AND_NATIVE_SCHEMA_FOR_ACCESS_SPEC($operatorClass, $extschemaName, $access->getName()));
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
                	SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_HAS_TYPE_MISMATCH_BETWEEN_OUTPUT_STREAM_ATTRIB_AND_SAME_NAME_NATIVE_SCHEMA_ATTRIB($operatorClass, $outputAttr->getName(), $access->getName()));
                }
                @$ostreamAttrsFromQuery[$j] = {_name          => $outputAttr->getName(),
                                               _type          => $outputAttr->getSPLType(),
                                               _length        => $access->getNativeSchemaAttributeLengthAt($extSchemaAttrIndex),
                                               _queryColIndex => $extSchemaAttrIndex + 1};
                ++$j;
            }
            else {
                # No code generator assignment, and no matching native schema attribute - error.
                SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_HAS_NO_MATCHING_ATTRIB_IN_INPUT_STREAM_OR_NATIVE_SCHEMA_OF_ACCESS_SPEC($operatorClass, $access->getName(), $outputAttr->getName()));
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
                	SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_HAS_TYPE_MISMATCH_BETWEEN_OUTPUT_STREAM_ATTRIB_AND_SAME_NAME_NATIVE_SCHEMA_ATTRIB($operatorClass, $outStreamAttrName, $access->getName()));
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
                SPL::CodeGen::exitln(MessagingResource::MSGTK_OP_HAS_NO_MATCHING_ATTRIB_IN_NATIVE_SCHEMA_OF_ACCESS_SPEC($operatorClass, $access->getName(), $outStreamAttrName));
            }
        }        
    }

    return $ostreamAttrsFromQuery;
}

1;
