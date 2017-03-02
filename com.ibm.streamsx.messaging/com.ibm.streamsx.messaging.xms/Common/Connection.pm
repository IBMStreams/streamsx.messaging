package Connection;
#######################################################################
# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                                    
#######################################################################                                 
                           

use XML::Simple;
use Data::Dumper;
use File::Basename;
use Cwd qw(realpath abs_path getcwd);
use Encode;

require MessagingResource;


my $path_separator = "/";

sub new {
  unless(@_ == 3 || @_ == 4) {
  	die MessagingResource::MSGTK_INCORRECT_NUMBER_OF_ARGS_PASSED_TO_CONNECTION_NEW();
    return 0;
  }
  my $class = shift @_;
  my $type = shift @_;
  my $name = shift @_;
  my $document = '';
  $document = shift if @_;

  if ($document eq '') { $document = './etc/connections.xml'; 
				}
  else {
    my @doclist = glob $document;
    $document = @doclist[0];
  }

  # Check document file
  if (!(-e $document)) {
	my $dir1 = getcwd;

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
  my $xsd = $moduledir . $path_separator . "connection.xsd";
  my $xmllint = "xmllint --noout --schema ${xsd} ${document} 2>/dev/null";
  # Convert unicode back to native character representation
  my $charmap = `/usr/bin/locale charmap`;
  system(Encode::encode($charmap, $xmllint));
  my $res = $? >> 8;
  if ($res != 0) {
    die MessagingResource::MSGTK_CONNECTION_XML_DOC_DOES_NOT_VALIDATE($document, $xsd, $xmllint);
  }

  # Parse the connection XML and grab the connections element
  my $connections;
  eval {
    $connections = XMLin($document,
                         keyattr => { connection_specification => 'name',
                                      installation => 'name',
                                      include => [],
                                      define => [],
                                      lib => [] },
                         forcearray => ['include','define','lib','connection_specification','access_specification','installation']);
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

  # Process the connection specifications
  my $connspecs = $connections->{connection_specifications};
  my $connspec = $connspecs->{connection_specification}->{$name};

  $refType = ref($connspec);
  unless (defined($refType) && $refType ne '') {
  	die MessagingResource::MSGTK_MISSING_CONNECTION_SPEC_IN_FILE($name, $document);
  }

  $refType = ref($connspec->{$type});
  unless (defined($refType) && $refType ne '') {
  	die MessagingResource::MSGTK_MISSING_ELEMENT_FROM_CONNECTION_SPEC_IN_FILE($type, $name, $document);
  }

#  my $installname = $connspec->{"installation"};
#  unless (defined($refType) && $refType ne '') {
#    die "Missing installation attribute for connection_specification '$name' in file '$document'";
#  }
#
#  my $install = $connections->{installations}->{installation}->{$installname};
#  unless (defined($refType) && $refType ne '') {
#    die "Missing installation element with name '$installname' used by connection_specification '$name' in file '$document'";
#  }

  my $self = {
    _name => $name,
    _type => $type,
#    _install => $install,
#    _libpath => '',
#    _incpath => '',
    _attributes => $connspec->{$type}
  };

#  if (exists $install->{"incpath"}) {
#    $self->{_incpath} = $install->{"incpath"};
#  }

#  if (exists $install->{"libpath"}) {
#    $self->{_libpath} = $install->{"libpath"};
#  }

  bless ($self, $class);
  return $self;
}

# Returns a boolean representing whether an connection has a specific attribute
# Input:  the name of the attribute to check for existence
sub hasAttributeName {
  my ($self, $aname) = @_;
  if (exists $self->{_attributes}->{$aname}) {
    return 1;
  } else {
    return 0;
  }
}

# Returns the value of a specific attribute of the connection
# Input:  the name of the attribute whose value is being requested
sub getAttributeByName {
    my ($self, $aname) = @_;
    return $self->{_attributes}->{$aname};
}

# Returns the include path specified for the connection
# sub getIncpath {
#   my ($self) = @_;
#   return $self->{_incpath};
# }

# Returns the library path specified for the connection
# sub getLibpath {
#   my ($self) = @_;
#   return $self->{_libpath};
# }

# Returns the number of libs for the connection's installation
# sub getNumberOfLibs {
#   my($self) = @_;
#   if (exists $self->{_install}->{lib}) {
#     my @libs = @{$self->{_install}->{lib}};
#     return $#libs+1;
#   } else { return 0; }
# }

# Returns the name of a lib for the connection's installation
# Input:  the positional order of the lib in the collection of lib
# sub getLibAt {
#   my ($self, $index) = @_;
#   return $self->{_install}->{lib}->[$index]->{name};
# }

# Returns the number of includes for the connection's installation
#sub getNumberOfIncludes {
#  my($self) = @_;
#  if (exists $self->{_install}->{include}) {
#    my @includes = @{$self->{_install}->{include}};
#    return $#includes+1;
#  } else { return 0; }
#}

# Returns the name of an include for the connection's installation
# Input:  the positional order of the define in the collection of includes
#sub getIncludeAt {
#  my ($self, $index) = @_;
#  my $file = $self->{_install}->{include}->[$index]->{name};
#  my $base = basename($file);
#  if ($file eq $base) {
#    return $file;
#  } else {
#    my @list = glob $file;
#    $file = @list[0];
#    return $file;
#  }
#}

# Returns the number of defines for the connection's installation
#sub getNumberOfDefines {
#  my($self) = @_;
#  if (exists $self->{_install}->{define}) {
#    my @defines = @{$self->{_install}->{define}};
#    return $#defines+1;
#  } else { return 0; }
#}

# Returns the name of a define for the connection's installation
# Input:  the positional order of the define in the collection of defines
#sub getDefineNameAt {
#  my ($self, $index) = @_;
#  return $self->{_install}->{define}->[$index]->{name};
#}

# Returns the value of a define for the connection's installation (or the empty string if the define doesn't have a value)
# Input:  the positional order of the value in the collection of defines
#sub getDefineValueAt {
#  my ($self, $index) = @_;
#  if (exists $self->{_install}->{define}->[$index]->{value}) {
#    return $self->{_install}->{define}->[$index]->{value};
#  } else {
#    return '';
#  }
#}

sub convertErrorToMessage {
  my ($self, $error) = @_;
  return (split(/\*+/, $error))[0];
}

1;
