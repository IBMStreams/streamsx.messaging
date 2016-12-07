package Access;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################                          
               

# Library for parsing the connection XML document.

use XML::Simple;
use Data::Dumper;
use File::Basename;
use Cwd qw(realpath abs_path getcwd);
use Encode;

require MessagingResource;

sub new {
  unless(@_ == 3 || @_ == 4) {
    die MessagingResource::MSGTK_INCORRECT_NUMBER_OF_ARGS_FOR_CONNECTION();
    return 0;
  }
  my $class = shift @_;
  my $type = shift @_;
  my $name = shift @_;
  my $document = '';
  $document = shift if @_;
  if ($document eq '') { $document = './etc/connections.xml'; }
  else {
    my @doclist = glob $document;
    $document = @doclist[0];
  }

  # Check document file
  if (!(-e $document)) {
    die MessagingResource::MSGTK_CONNECTION_XML_DOC_DOES_NOT_EXIST($document);
  }
  if (!(-s $document)) {
    die MessagingResource::MSGTK_CONNECTION_XML_DOC_EMPTY($document);
  }
  if (!(-r $document)) {
    die MessagingResource::MSGTK_CONNECTION_XML_DOC_NOT_READABLE_CHECK_PERMISSIONS($document);
  }
  if (-d $document) {
    die MessagingResource::MSGTK_DOC_IS_DIR_NOT_XML_DOC($document);
  }
  if (!(-T $document)) {
  	die MessagingResource::MSGTK_CONNECTION_XML_DOC_IS_NOT_TEXT_FILE($document);
  }

  # Validate the XML
  my $moduledir = realpath(dirname(__FILE__));
  my $xsd = "${moduledir}/connection.xsd";
  my $xmllint = "xmllint --noout --schema ${xsd} ${document} 2>/dev/null";
  # Convert unicode back to native character representation
  my $charmap = `/usr/bin/locale charmap`;
  system(Encode::encode($charmap, $xmllint));
  my $res = $? >> 8;
  if ($res != 0) {
  	die MessagingResource::MSGTK_CONNECTION_XML_DOC_DOES_NOT_VALIDATE($document, $xsd, $xmllint);
  }

  # Parse the access XML and grab the accesses element
  my $accesses;
  eval {
    $accesses = XMLin($document,
                     keyattr => { access_specification => 'name',
                                  operator_usage => 'type',
                                  uses_connection => 'connection',
                                  allowed => 'value',
                                  parameter => [],
                                  attribute => []},
                     forcearray => ['parameter','attribute','access_specification','uses_connection']);
  };
  if ($@) {
    my @msgfrags = split(/ at /,$@);
    my $msg = @msgfrags[0];
    $midpt = length($msg) / 2;

    # Fix "Is a directoryIs a directory"
    # Because of the -d check done earlier, it is hoped we never get this, but just in case (e.g., race condition
    # with a file being moved); also, this check should not cause any harm
    if ($msg eq "Is a directoryIs a directory") {	# TODO: Is the duplicated string correct???
      die MessagingResource::MSGTK_DOC_IS_DIR_NOT_XML_DOC($document);
    }
    
	die MessagingResource::MSGTK_CONNECTION_XML_FILE_PROBLEM($msg);
  }

# Process the access specifications
  my $accspecs = $accesses->{access_specifications};
  my $accspec = $accspecs->{access_specification}->{$name};

  $refType = ref($accspec);
  unless (defined($refType) && $refType ne '') {
  	die MessagingResource::MSGTK_MISSING_ACCESS_SPEC_IN_FILE($name, $document);
  }

  $refType = ref($accspec->{$type});
  unless (defined($refType) && $refType ne '') {
  	die MessagingResource::MSGTK_MISSING_TAG_FROM_ACCESS_SPEC_IN_FILE($type, $name, $document);
  }

  my $self = {
    _name => $name,
    _type => $type,
    _attributes => $accspec->{$type},
    _parameters => $accspec->{parameters},
    _extschema => $accspec->{native_schema},
    _uses => $accspec->{uses_connection}
  };

  bless ($self, $class);
  return $self;
}

# Returns a string containing the name of the access specification, as specified to the constructor.
sub getName {
  my ($self) = @_;
  return $self->{_name};
}

# Returns a boolean representing whether an access has a specific attribute
# Input:  the name of the attribute to check for existence
sub hasAttributeName {
  my($self, $aname) = @_;
  if (exists $self->{_attributes}->{$aname}) {
    return 1;
  } else {
    return 0;
  }
}

# Returns the value of a specific attribute of the access
# Input:  the name of the attribute whose value is being requested
sub getAttributeByName {
    my($self, $aname) = @_;
    return $self->{_attributes}->{$aname};
}

# Returns an array of parameters
sub getParameters {
  my ($self) = @_;
  return @{$self->{_parameters}->{parameter}};
}

# Returns the number of (output) parameters for an access.
sub getNumberOfParameters {
  my($self) = @_;
  if (exists $self->{_parameters}->{parameter}) {
    my @parameters = @{$self->{_parameters}->{parameter}};
    return $#parameters+1;
  } else { return 0; }
}

# Returns the parameter index of a given parameter name
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterIndexByName {
  my ($self, $name) = @_;
  my @parameters = @{$self->{_parameters}->{parameter}};
  for my $index (0..$#parameters) {
    if ($parameters[$index]->{name} eq $name) { return $index; }
  }
  return -1;
}

# Returns the name of a parameter for an access.
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterNameAt {
  my ($self, $index) = @_;
  return $self->{_parameters}->{parameter}->[$index]->{name};
}

# Returns the type of a parameter for an access.
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterTypeAt {
  my ($self, $index) = @_;
  return $self->{_parameters}->{parameter}->[$index]->{type};
}

# Returns the default of a parameter for an access.
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterDefaultAt {
  my ($self, $index) = @_;
  return $self->{_parameters}->{parameter}->[$index]->{default};
}

# Returns the cardinality of a parameter for an access. If it does not exist, the default of 1 is returned
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterCardinalityAt {
  my ($self, $index) = @_;
  if (exists $self->{_parameters}->{parameter}->[$index]->{cardinality}) {
    return $self->{_parameters}->{parameter}->[$index]->{cardinality};
  } else {
    return 1;
  }
}

# Returns the length of a parameter for an access.
# Input:  the positional order of the parameter in the collection of parameters
sub getParameterLengthAt {
  my ($self, $index) = @_;
  if (exists $self->{_parameters}->{parameter}->[$index]->{length}) {
    return $self->{_parameters}->{parameter}->[$index]->{length};
  } else { return -1; }
}

# Returns the number of Native schema attributes for an access.
sub getNumberOfNativeSchemaAttributes {
  my($self) = @_;
  my @schemaattrs = @{$self->{_extschema}->{attribute}};
  return $#schemaattrs+1;
}

# Returns the name of an attribute for an access Native schema.
# Input:  the positional order of the parameter in the collection of parameters
sub getNativeSchemaAttributeNameAt {
  my ($self, $index) = @_;
  return $self->{_extschema}->{attribute}->[$index]->{name};
}

# Returns the type of an attribute for an access schema.
# Input:  the positional order of the parameter in the collection of parameters
sub getNativeSchemaAttributeTypeAt {
  my ($self, $index) = @_;
  return $self->{_extschema}->{attribute}->[$index]->{type};
}

# Returns the length of an attribute for an access schema.
# Input:  the positional order of the parameter in the collection of parameters
sub getNativeSchemaAttributeLengthAt {
  my ($self, $index) = @_;
  if (exists $self->{_extschema}->{attribute}->[$index]->{length}) {
    return $self->{_extschema}->{attribute}->[$index]->{length};
  } else { return -1; }
}

# Returns the length of an attribute for an access schema.
# Input:  the positional order of the parameter in the collection of parameters
sub getNativeSchemaAttributeKeyAt {
  my ($self, $index) = @_;
  if (exists $self->{_extschema}->{attribute}->[$index]->{key}) {
    return $self->{_extschema}->{attribute}->[$index]->{key};
  } else { return 0; }
}

sub addNativeSchemaAttribute {
  my $self = shift @_;
  my $name = shift @_;
  my $type = shift @_;
  my $length = '';
  $length = shift if @_;
#print Dumper($self->{_extschema});

  my $numAttrs = $self->getNumberOfNativeSchemaAttributes();
  my $attrRef = {};
  $attrRef->{"name"} = $name;
  $attrRef->{"type"} = $type;
  if ($length ne '') { $attrRef->{"length"} = $length; }

  $self->{_extschema}->{attribute}->[$numAttrs] = $attrRef;

#print Dumper($self->{_extschema});
}

# Returns a boolean representing whether an access has specified that it is meant
# to work with a specific connection specification.
# Input: the name of the connection specification
sub usesConnection {
  my ($self, $connname) = @_;
  if (exists $self->{_uses}->{$connname}) {
    return 1;
  } else {
    return 0;
  }
}

# Returns a boolean representing whether a specific value for a specific access
# parameter passes that parameter's check.
# Input: the parameter number, the value to check, and (optionally) whether a
# value violating the check should have an error message printed.
sub checkStaticParameterAt {
  unless(@_ == 3 || @_ == 4) {
  	die MessagingResource::MSGTK_INCORRECT_NUMBER_OF_ARGS_PASSED_TO_ACCESS_CHECKSTATICPARAMAT();
    return 0;
  }
  my $self = shift @_;
  my $index = shift @_;
  my $checkval = shift @_;
  my $quietmode = 0;
  $quietmode = shift if @_;

  if (not (exists $self->{_parameters}->{parameter}->[$index])) {
  	die MessagingResource::MSGTK_INVALID_PARAM_CHECK($index, $self->{_name});
  }
  if (not (exists $self->{_parameters}->{parameter}->[$index]->{check})) { return 1; }
  my $check = $self->{_parameters}->{parameter}->[$index]->{check};

  my $checktype = $check->{type};
  if ($checktype eq 'list') {
    if (exists $check->{allowed}->{$checkval}) {
      return 1;
    } else {
      if (!$quietmode) {
        my @values = keys %{$check->{allowed}};
        die MessagingResource::MSGTK_INVALID_CONST_VALUE_FOR_PARAM_OF_ACCESS($checkval, $self->{_parameters}->{parameter}->[$index]->{name}, $self->{_name}, @values);
      }
      return 0;
    }
  } elsif ($checktype eq 'range') {
    if ($checkval > $check->{max}) {
      if (!$quietmode) {
        die MessagingResource::MSGTK_VALUE_FOR_PARAM_OF_ACCESS_IS_OVER_MAX($checkval, $self->{_parameters}->{parameter}->[$index]->{name}, $self->{_name}, $check->{max});
      }
      return 0;
    }
    if ($checkval < $check->{min}) {
      if (!$quietmode) {
        die MessagingResource::MSGTK_VALUE_FOR_PARAM_OF_ACCESS_IS_UNDER_MIN($checkval, $self->{_parameters}->{parameter}->[$index]->{name}, $self->{_name}, $check->{min});
      }
      return 0;
    }
    return 1;  # we're within the range if we reach here
  } else {
    die MessagingResource::MSGTK_UNSUPPORTED_CHECK_TYPE_FOR_PARAM_OF_ACCESS($self->{_parameters}->{parameter}->[$index]->{name}, $self->{_name});
  }
  return 0; # Should not have reached this line
}

# Returns a run-time C++ boolean expression representing whether a value for a specific access
# parameter passes that parameter's check.
# Input: the parameter number and the value to check (as a C++ expression in its proper type)
sub checkRuntimeParameterAt {
  my ($self, $index, $checkval) = @_;

  if (not (exists $self->{_parameters}->{parameter}->[$index])) {
  	die MessagingResource::MSGTK_INVALID_PARAM_CHECK_GREATER_THAN_NUMBER_OF_PARAMS_FOR_ACCESS($index, $self->{_name});
  }

  if (not (exists $self->{_parameters}->{parameter}->[$index]->{check})) { return "1"; }
  my $check = $self->{_parameters}->{parameter}->[$index]->{check};

  my $code = '(';

  my $checktype = $check->{type};
  my $datatype = $self->{_parameters}->{parameter}->[$index]->{type};
  if ($checktype eq 'list') {
    my @values = keys %{$check->{allowed}};
    my $first = 1;
    foreach my $validvalue (@values) {
      if ($first == 0) { $code .= " || "; }
      else { $first = 0; }
      if ($datatype eq 'rstring') {
        $code .= "strcmp($checkval, \"$validvalue\") == 0";
      }
      else {
        $code .= "($checkval) == $validvalue";
      }
    }
  } elsif ($checktype eq 'range') {
    my $bound;
    if (exists $check->{min}) {
      $bound = $check->{min};
      if ($datatype eq 'rstring') {
        $code .= "strcmp($checkval, \"$bound\") >= 0";
      } else {
        $code .= "($checkval) >= $bound";
      }
      if (exists $check->{max}) {
        $code .= " && ";
      }
    }
    if (exists $check->{max}) {
      $bound = $check->{max};
      if ($datatype eq 'rstring') {
        $code .= "strcmp($checkval, \"$bound\") <= 0";
      } else {
        $code .= "($checkval) <= $bound";
      }
    }
  } else {
  	die MessagingResource::MSGTK_UNSUPPORTED_CHECK_TYPE_FOR_PARAM_OF_ACCESS($self->{_parameters}->{parameter}->[$index]->{name}, $self->{_name});
  }

  return $code . ')';
}

1;