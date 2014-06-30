package XMSSourceCommon;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################                                 

use File::Basename;

sub verify($) 
{
	
	my ($model) = @_;


	#Need to know about the reconnection policy
	my $reconnectionPolicy = $model->getParameterByName("reconnectionPolicy");
	my $reconnectionBound = $model->getParameterByName("reconnectionBound");
	my $period = $model->getParameterByName("period");
	

	#reconnectionBound parameter can only appear if reconnectionPolicy is  BoundedRetry
	if (defined $reconnectionBound) {
		if (!defined $reconnectionPolicy || $reconnectionPolicy->getValueAt(0)->getSPLExpression() ne "BoundedRetry") {
			SPL::CodeGen::exitln("Operator \'XMSSource\': reconnectionBound parameter can only appear if reconnectionPolicy is  \'BoundedRetry\' ");
		}
		#reconnectionBound cannot be negative
		if ($reconnectionBound->getValueAt(0)->getSPLExpression() <0)
		{
			SPL::CodeGen::exitln("Operator \'XMSSource\': reconnectionBound cannot be negative ");    
		}
	}

	#period parameter can only appear if reconnectionPolicy is defined
	if (defined $period && !defined $reconnectionPolicy ) {        
		SPL::CodeGen::exitln("Operator \'XMSSource\': period parameter can only appear if reconnectionPolicy is specified ");    
	}
	
	

	my $connTag = 'XMS';  
	my $accessTag = 'destination'; 
	my ($conn, $access) = Edge::connectionSetup($model, $connTag, $accessTag);
 	my $msgType = $access->getAttributeByName('message_class');
 	my $nparm = $access->getNumberOfNativeSchemaAttributes();

	#native_schema should not be specified when message class is empty.
 	if (($msgType eq 'empty') && ($nparm > 0)){
	      SPL::CodeGen::exitln("Operator \'XMSSource\': unable to use message class \'" . $msgType .
		"\' in access specification " . $access->getName() .
		" if native schema attributes have been supplied\n");
	}

	
	#For ERROR PORT OUTPUT
	my $operatorErrorPort = undef;


	# Error ports are optional so even if one is allowed it may not be used.
	if ($model->getNumberOfOutputPorts() == 2) {
		$operatorErrorPort = $model->getOutputPortAt(1);
	}

	if (defined $operatorErrorPort) {
			my $errorAttribute =($operatorErrorPort)->getAttributeAt(0);
			my $errorAttributeType = $errorAttribute->getSPLType();		
			if ($errorAttributeType ne 'rstring') {
				SPL::CodeGen::exitln("Operator \'XMSSource\': The error ourput port can only contain an rstring.\n");
			}
	}

	
	my $parmlist = [];
	my %schemaAttrs = {}; 
	my $outStream = $model->getOutputPortAt(0);
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
	for ( my $i=0; $i < $nparm; $i++ )
	{
		my $parm = {};
		$$parm{_name} = $access->getNativeSchemaAttributeNameAt($i);
		$$parm{_type} = $access->getNativeSchemaAttributeTypeAt($i);
		$$parm{_length} = $access->getNativeSchemaAttributeLengthAt($i);
		# Check that the user hasn't specified a length value for types other than String or ByteLists
		if (($$parm{_type} ne "String") && ($$parm{_type} ne "Bytes") && ($$parm{_length} ne -1 ))
		{
			SPL::CodeGen::exitln("Operator \'XMSSource\': native schema attribute \'$$parm{_name}\' defined in access specification " . $access->getName() . 
			" must not have a length specification\n");
		} 


		# If the schema attribute exists in the output stream we need to check that the schema type matches the output type
		my $outAttr =  $outStream->getAttributeByName ( $$parm{_name} );
		if ( defined $outAttr )
		{
			#Verify if the attribute in native schema has a matching attribute in the input stream and if they have the same type     
			if (($outAttr->getSPLType() ne "int8") && ($outAttr->getSPLType() ne "uint8") && ($outAttr->getSPLType() ne "int16") && ($outAttr->getSPLType() ne "uint16") && ($outAttr->getSPLType() ne "int32") && ($outAttr->getSPLType() ne "uint32") && ($outAttr->getSPLType() ne "int64") && ($outAttr->getSPLType() ne "float32") && ($outAttr->getSPLType() ne "float64") && ($outAttr->getSPLType() ne "boolean") && ($outAttr->getSPLType() ne "blob") && ($outAttr->getSPLType() ne "rstring")){
				SPL::CodeGen::exitln("Operator \'XMSSource\': Stream schema attribute \'" .$$parm{_name} . "\'is not a supported data type\n");
			}     

			#check its type
			if ($XMStypeTable{$outAttr->getSPLType()} ne $$parm{_type})
			{
				SPL::CodeGen::exitln("Operator \'XMSSource\': native schema attribute \'" . $$parm{_name} .
				"\' in access specification " . $access->getName() .
				" has type \'" . $$parm{_type} .
				"\' which does not match the attribute in the output stream schema\n");
			}  
			
			if (defined $schemaAttrs{$$parm{_name} })
			{
				SPL::CodeGen::exitln("Operator \'XMSSource\': native schema attribute \'" . $$parm{_name} .
				"\' defined twice in access specification " . $access->getName() . "\n");
			}  
			$schemaAttrs { $$parm{_name} } = $$parm{_type};  
		}
		push @$parmlist, $parm;
	}


	for (my $i = 0; $i < $outStream->getNumberOfAttributes(); $i++)
	{
       	my $outputAttr = $outStream->getAttributeAt($i);
		if (($outputAttr->hasAssignment() == 0) && (!defined $schemaAttrs {$outputAttr->getName()}) )
		{
			SPL::CodeGen::exitln("Operator \'XMSSource\': output stream attribute \'" . $$parm{_name} .
			" has no matching attribute in the native schema " .
			"\' in access specification " . $access->getName() ."\n");
		} 
	}

	
	if ($msgType eq 'bytes'){
		for ( my $i=0; $i < @$parmlist; $i++ )
		{
			my $parm = @$parmlist[$i];
			my $type = $$parm{_type};
			my $dpsName = $$parm{_name};
			my $length = $$parm{_length};

			# TODO: Consider removing the check for length != -9999 in a future release
			if (  (($type eq 'String') || ($type eq 'Bytes')) && ($length<0 && $length!=-2 && $length!=-4 && $length!=-8 && $length!=-9999) ){	
				
				if ( ($i eq $nparm-1) && ($length eq -1) )
				{
					next; # do not throw an error if the last attribute has no length (i.e. length == -1)
				} else {
					SPL::CodeGen::exitln("Operator \'XMSSource\': native schema attribute \'" . $$parm{_name} .  
					"\' defined in access specification \'" . $access->getName() . 
					"\' must have a valid length specification\n");	
				}
			}
		}
	}


	# Raise a compile time error if the user has specified a negative length
	if ($msgType eq 'map' || $msgType eq 'stream'){
		for ( my $i=0; $i < @$parmlist; $i++ )
		{
			my $parm = @$parmlist[$i];
			my $length = $$parm{_length};
			if ($length<-1){	
				SPL::CodeGen::exitln("Operator \'XMSSource\': native schema attribute \'" . $$parm{_name} .  
				"\' defined in access specification \'" . $access->getName() . 
				"\' must have a valid length specification\n");
			}
		}
	}

}
	
1;	