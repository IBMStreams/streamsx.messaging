package XMSSinkCommon;
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
	my $periodVal=0.0; 

	#reconnectionBound parameter can only appear if reconnectionPolicy is  BoundedRetry
	if (defined $reconnectionBound) {
		if (!defined $reconnectionPolicy || $reconnectionPolicy->getValueAt(0)->getSPLExpression() ne "BoundedRetry") {
			SPL::CodeGen::exitln("Operator \'XMSSink\': reconnectionBound parameter can only appear if reconnectionPolicy is  \'BoundedRetry\' ");
		}
		#reconnectionBound cannot be negative
		if ($reconnectionBound ->getValueAt(0)->getSPLExpression() <0)
		{
			SPL::CodeGen::exitln("Operator \'XMSSink\': reconnectionBound cannot be negative ");    
		}
	}

	#period parameter can only appear if reconnectionPolicy is defined
	if (defined $period && !defined $reconnectionPolicy ) {        
		SPL::CodeGen::exitln("Operator \'XMSSink\': period parameter can only appear if reconnectionPolicy is specified ");    
	}
	
	


	my $connTag = 'XMS';  
	my $accessTag = 'destination'; 
	my ($conn, $access) = Edge::connectionSetup($model, $connTag, $accessTag);
 	my $msgType = $access->getAttributeByName('message_class');
 	my $nparm = $access->getNumberOfNativeSchemaAttributes();

	#native_schema should not be specified when message class is empty.
 	if (($msgType eq 'empty') && ($nparm > 0)){
	      SPL::CodeGen::exitln("Operator \'XMSSink\': unable to use message class \'" . $msgType .
		"\' in access specification " . $access->getName() .
		" if native schema attributes have been supplied\n");
	}

	
	#For ERROR PORT OUTPUT
	my $operatorErrorPort = undef;
	my $inStream = $model->getInputPortAt(0);


	# Error ports are optional so even if one is allowed it may not be used.
	if ($model->getNumberOfOutputPorts() == 1) {
		$operatorErrorPort = $model->getOutputPortAt(0);
	}

	if (defined $operatorErrorPort) {
		foreach my $errorAttribute (@{($operatorErrorPort)->getAttributes()}) {
			my $errorAttributeType = $errorAttribute->getSPLType();
			
			# Input Tuple
			if (SPL::CodeGen::Type::isTuple($errorAttributeType)) {
				if($inStream->getSPLTupleType() ne $errorAttributeType){
					SPL::CodeGen::exitln("Operator \'XMSSink\': The tuple attributes defined in the error stream do not match the tuple which is received\n");
				}
			}
		}
	}


	my $modelroot = $model->getContext()->getOperatorDirectory();
	unshift @INC, dirname($modelroot) . "/Common";	
	require Connection;
	require Access;
	require Edge;


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

	#Retrieve the attributes from the incoming tuple and create a list of these after some checks.
	my $parmlist = [];
	for ( my $i=0; $i < $nparm; $i++ ){
    		my $parm = {};
		$$parm{_name} = $access->getNativeSchemaAttributeNameAt($i);
		$$parm{_type} = $access->getNativeSchemaAttributeTypeAt($i);
		$$parm{_length} = $access->getNativeSchemaAttributeLengthAt($i);
    				
		# Check that the user hasn't specified a length value for types other than String or ByteLists
	    	if(($$parm{_type} ne "String") && ($$parm{_type} ne "Bytes") && ($$parm{_length} ne -1)){
      			SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute \'" . $$parm{_name} .
			"\' in access specification " . $access->getName() .
			" must not have a length specification \n");
		}
        
		#Verify if the attribute in native schema has a matching attribute in the input stream and if they have the same type     
		my $inAttr =  $inStream->getAttributeByName ( $$parm{_name} );    
		if ( defined $inAttr){
			if (($inAttr->getSPLType() ne "int8") && ($inAttr->getSPLType() ne "uint8") && ($inAttr->getSPLType() ne "int16") && ($inAttr->getSPLType() ne "uint16") && ($inAttr->getSPLType() ne "int32") && ($inAttr->getSPLType() ne "uint32") && ($inAttr->getSPLType() ne "int64") && ($inAttr->getSPLType() ne "float32") && ($inAttr->getSPLType() ne "float64") && ($inAttr->getSPLType() ne "boolean") && ($inAttr->getSPLType() ne "blob") && ($inAttr->getSPLType() ne "rstring")){
				SPL::CodeGen::exitln("Operator \'XMSSink\': Stream schema attribute \'" .$$parm{_name} . "\'is not a supported data type\n");
			}     

			if ($XMStypeTable{$inAttr->getSPLType()} ne $$parm{_type}){
				SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute \'" . $$parm{_name} .
				"\' in access specification " . $access->getName() .
				" has type \'" . $$parm{_type} .
				"\' which does not match the attribute in the input stream schema\n");
			}     
		}
      		else{
			SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute \'" . $$parm{_name} .
			"\' in access specification " . $access->getName() .
			" has no matching attribute in the input stream schema\n");
		}
		push @$parmlist, $parm;
	}
	

	if ( $msgType eq 'map' || $msgType eq 'stream' || $msgType eq 'xml')
	{
		foreach my $attribute (@$parmlist) {   
           		my $length = $$attribute{_length};
        		my $name = $$attribute{_name};

	        	#negative length is not allowed for message class map, stream and xml.
			if($length < -1){
				SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute $name in access specification " . $access->getName() . " should not have a negative length\n");
			}
		}
	}

	
	if ( $msgType eq 'bytes')
	{
		foreach my $attribute (@$parmlist) {   
           		my $length = $$attribute{_length};
			my $name = $$attribute{_name};

			#negative length is not allowed for message class bytes other than -2 -4 -8
			if($length <-1 && $length !=-2 && $length!=-4 && $length!=-8){
				SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute $name in access specification " . $access->getName() . " cannot have any other negative values than -2,-4 and -8 \n");
			}
		}
	}


	if ( $msgType eq 'wbe' || msgType eq 'wbe22')
	{
		foreach my $attribute (@$parmlist) {   
           		my $type = $$attribute{_type};
			my $name = $$attribute{_name};

			#bytes data type is not allowed for message class wbe and wbe22
			if($type eq 'Bytes'){
				SPL::CodeGen::exitln("Operator \'XMSSink\': native schema attribute $name in access specification " . $access->getName() . " cannot have Bytes data for this message class \n");
			}
		}
	}

}
	
1;