package XMSSinkCommon;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################  
use File::Basename;



sub verify($) 
{
	my ($model) = @_;

	my $modelroot = $model->getContext()->getOperatorDirectory();
	unshift @INC, dirname($modelroot) . "/../impl/nl/include";
	require MessagingResource;


	#Need to know about the reconnection policy
	my $reconnectionPolicy = $model->getParameterByName("reconnectionPolicy");
	my $reconnectionBound = $model->getParameterByName("reconnectionBound");
	my $period = $model->getParameterByName("period");
	my $periodVal=0.0; 

	#reconnectionBound parameter can only appear if reconnectionPolicy is  BoundedRetry
	if (defined $reconnectionBound) {
		if (!defined $reconnectionPolicy || $reconnectionPolicy->getValueAt(0)->getSPLExpression() ne "BoundedRetry") {
			SPL::CodeGen::exitln(MessagingResource::MSGTK_RECONNECTIONBOUND_ONLY_IF_RECONNECTIONPOLICY_IS_BOUNDEDRETRY("XMSSink"));
		}
		#reconnectionBound cannot be negative
		if ($reconnectionBound ->getValueAt(0)->getSPLExpression() <0)
		{
			SPL::CodeGen::exitln(MessagingResource::MSGTK_RECONNECTIONBOUND_CANNOT_BE_NEGATIVE("XMSSink"));
		}
	}

	#period parameter can only appear if reconnectionPolicy is defined
	if (defined $period && !defined $reconnectionPolicy ) {
		SPL::CodeGen::exitln(MessagingResource::MSGTK_PERIOD_PARAM_ONLY_IF_RECONNECTIONPOLICY_IS_SPECIFIED("XMSSink"));
	}
	
	


	my $connTag = 'XMS';  
	my $accessTag = 'destination'; 
	my ($conn, $access) = Edge::connectionSetup($model, $connTag, $accessTag);
 	my $msgType = $access->getAttributeByName('message_class');
 	my $nparm = $access->getNumberOfNativeSchemaAttributes();

	#native_schema should not be specified when message class is empty.
 	if (($msgType eq 'empty') && ($nparm > 0)){
		SPL::CodeGen::exitln(MessagingResource::MSGTK_UNABLE_TO_USE_MSG_CLASS_IN_ACCESS_SPEC_IF_NATIVE_SCHEMA_ATTRIBS_SUPPLIED("XMSSink", $msgType, $access->getName()));
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
					SPL::CodeGen::exitln(MessagingResource::MSGTK_TUPLE_ATTRIBS_IN_ERROR_STREAM_DONT_MATCH_RECEIVED_TUPLE("XMSSink"));
				}
			}
		}
	}


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
	    		SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_MUST_NOT_LENGTH_SPEC("XMSSink", $$parm{_name}, $access->getName()));
		}
        
		#Verify if the attribute in native schema has a matching attribute in the input stream and if they have the same type     
		my $inAttr =  $inStream->getAttributeByName ( $$parm{_name} );    
		if ( defined $inAttr){
			if (($inAttr->getSPLType() ne "int8") && ($inAttr->getSPLType() ne "uint8") && ($inAttr->getSPLType() ne "int16") && ($inAttr->getSPLType() ne "uint16") && ($inAttr->getSPLType() ne "int32") && ($inAttr->getSPLType() ne "uint32") && ($inAttr->getSPLType() ne "int64") && ($inAttr->getSPLType() ne "float32") && ($inAttr->getSPLType() ne "float64") && ($inAttr->getSPLType() ne "boolean") && ($inAttr->getSPLType() ne "blob") && ($inAttr->getSPLType() ne "rstring")){
				SPL::CodeGen::exitln(MessagingResource::MSGTK_STREAM_SCHEMA_ATTRIB_IS_NOT_SUPPORTED_DATA_TYPE("XMSSink", $$parm{_name}));
			}     

			if ($XMStypeTable{$inAttr->getSPLType()} ne $$parm{_type}){
				SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_HAS_TYPE_WHICH_DOSNT_MATCH_ATTRIB_IN_INPUT_STREAM_SCHEMA("XMSSink", $$parm{_name}, $access->getName(), $$parm{_type}));
			}     
		}
      		else{
      		SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_HAS_NO_MATCHING_ATTRIB_IN_INPUT_STREAM_SCHEMA("XMSSink", $$parm{_name}, $access->getName()));
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
				SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_SHOULD_NOT_HAVE_NEGATIVE_LENGTH("XMSSink", $name, $access->getName()));
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
				SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_CANNOT_HAVE_OTHER_NEGATIVE_VALUES_THAN("XMSSink", $name, $access->getName()));
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
				SPL::CodeGen::exitln(MessagingResource::MSGTK_NATIVE_SCHEMA_ATTRIB_IN_ACCESS_SPEC_CANNOT_HAVE_BYTES_DATA_FOR_THIS_MSG_CLASS("XMSSink", $name, $access->getName()));
			}
		}
	}

}
	
1;