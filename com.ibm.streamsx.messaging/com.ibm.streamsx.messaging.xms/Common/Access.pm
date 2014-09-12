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

sub new {
  unless(@_ == 3 || @_ == 4) {
    die "Incorrect number of arguments passed to Connection->new().  Required are type and name values.  Optional is document.***";
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
    die "Connection XML document '$document' does not exist***";
  }
  if (!(-s $document)) {
    die "Connection XML document '$document' is empty***";
  }
  if (!(-r $document)) {
    die "Connection XML document '$document' is not readable. Please check permissions***";
  }
  if (-d $document) {
    die "'$document' is a directory, not a connection XML document***";
  }
  if (!(-T $document)) {
    die "Connection XML document '$document' is not a text file***";
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
    die "Connection XML document '$document' does not validate with schema '$xsd' using the system command '$xmllint'***";
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
    if ($msg eq "Is a directoryIs a directory") {
      die "$document is a directory, not a connection xml document***";
    }

    die "Connection XML file problem. $msg***";
  }

# Process the access specifications
  my $accspecs = $accesses->{access_specifications};
  my $accspec = $accspecs->{access_specification}->{$name};

  $refType = ref($accspec);
  unless (defined($refType) && $refType ne '') {
    die "Missing access_specification named '$name' in file '$document'***";
  }

  $refType = ref($accspec->{$type});
  unless (defined($refType) && $refType ne '') {
    die "Missing '$type' tag from access_specification '$name' in file '$document'***";
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
    die "Incorrect number of arguments passed to Access->checkStaticParameterAt().  Required are index and check values.  Optional is quiet mode.***";
    return 0;
  }
  my $self = shift @_;
  my $index = shift @_;
  my $checkval = shift @_;
  my $quietmode = 0;
  $quietmode = shift if @_;

  if (not (exists $self->{_parameters}->{parameter}->[$index])) {
    die "Invalid parameter check.  '$index' is greater than the number of parameters for the access named '$self->{_name}'***";
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
        die "Invalid constant value '$checkval' for parameter '$self->{_parameters}->{parameter}->[$index]->{name}' of access named '$self->{_name}'.  Valid values are: @values***";
      }
      return 0;
    }
  } elsif ($checktype eq 'range') {
    if ($checkval > $check->{max}) {
      if (!$quietmode) {
        die "Value of '$checkval' for parameter '$self->{_parameters}->{parameter}->[$index]->{name}' of access named '$self->{_name}' is over the maximum allowed of '$check->{max}'***";
      }
      return 0;
    }
    if ($checkval < $check->{min}) {
      if (!$quietmode) {
        die "Value of '$checkval' for parameter '$self->{_parameters}->{parameter}->[$index]->{name}' of access named '$self->{_name}' is under the minimum allowed of '$check->{min}'***";
      }
      return 0;
    }
    return 1;  # we're within the range if we reach here
  } else {
    die "Unsupported check type specified for parameter '$self->{_parameters}->{parameter}->[$index]->{name}' of access named '$self->{_name}'***";
  }
  return 0; # Should not have reached this line
}

# Returns a run-time C++ boolean expression representing whether a value for a specific access
# parameter passes that parameter's check.
# Input: the parameter number and the value to check (as a C++ expression in its proper type)
sub checkRuntimeParameterAt {
  my ($self, $index, $checkval) = @_;

  if (not (exists $self->{_parameters}->{parameter}->[$index])) {
    die "Invalid parameter check.  '$index' is greater than the number of parameters for the access named '$self->{_name}'***";
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
    die "Unsupported check type specified for parameter '$self->{_parameters}->{parameter}->[$index]->{name}' of access named '$self->{_name}'***";
  }

  return $code . ')';
}

1;