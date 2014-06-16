#!/usr/bin/perl

use strict;
use Getopt::Long;


my $debug = 0;
my $dryrun = 0;
my $doPush;
my $doCommit=1; 
my $message = "Update documents";
my $help;
my $useTemp;
my $deleteTemp = 1;
my $makeDoc = 1;
my %samples;

GetOptions('push!'=>\$doPush,
	   'commit!'=>\$doCommit,
	   'dryrun' => \$dryrun,
	   'message|m=s' => \$message,
	   'makedoc!' => \$makeDoc,
           'help|h|?'=>\$help,
	   'deleteTemp!' => \$deleteTemp,
           'createTempRepository' => \$useTemp);
if ($dryrun) {
    $doPush = 0;
    $doCommit = 0;
}

if ($doPush && !$doCommit) {
    print STDERR "If --push is specified, then commit must be enabled\n";
} 

if ($help) {
    print STDOUT "makeDocs.pl runs spl-make-doc in the current repository for all toolkits and samples,\n";
    print STDOUT "and then commits the doc updates to a second document repository (one set to gh-pages).\n";
    print STDOUT "Usage: makeDocs.pl [--commit|--nocommit] [--push|--nopush] [--message <msg>] <gh-pagesrepos>\n";
    exit 0;
}
if (scalar @ARGV != 1 && !$useTemp) {
    print STDERR "Usage: makeDocs.pl <pagesrepo>\n";
    exit 1;
}
if ($useTemp && $deleteTemp) {
    $doCommit or die "Must do a commit if making a temporary repository";
    $doPush or die "Must do a push if using a temporary repository";
}
my $pagesLocation = $ARGV[0];
die "$pagesLocation not a valid directory" unless $useTemp || (-e $pagesLocation && -d $pagesLocation);

sub updateIndexHtml() {

    my $infile = "$pagesLocation/index.html";
    my $outfile = "$pagesLocation/index.html.tmp";
    open(IN,"<$pagesLocation/index.html") or die "Could not open $infile";
    open(OUT,">$pagesLocation/index.html.tmp") or die "Could not open $outfile";
    while(<IN>) {
	if (/<!--\s*BEGIN SAMPLE LIST\s*-->/) {
	    print OUT $_;
	    my $foundEnd = 0;
	    print OUT "<ul>\n";
	    for my $app (keys %samples) {
		print OUT "<li><a href=\"$samples{$app}\">$app</a>\n";
	    }
	    print OUT "</ul>\n";
	    while(<IN>) {
		if (/<!--\s*END SAMPLE LIST\s*-->/) {
		    $foundEnd = 1;
		    print OUT $_;
		    last;
		}
	    }
	    die "Did not find END SAMPLE LIST" if !$foundEnd;
	}
	else {
	    print OUT $_;
	}
    }

    close OUT;
    close IN;
    system("cp $outfile $infile");
    system("cd $pagesLocation; git add index.html");
}

sub lookForApp($$) {
    my ($dir,$exclude) = @_;
    $debug && print "lookForApp($dir,$exclude)\n";
    my @files = `ls $dir`;
    my $changes = 0;
    for my $f (@files) {
	chomp $f;
	my $fullName = "$dir/$f";
	$debug && print "Trying file $fullName\n";
	next if (defined $exclude && $dir =~ /$exclude/);
	next unless (-d "$fullName");
	if (-e "$fullName/doc/spldoc") {
	    if ($fullName =~ /samples/) {
		$debug && print "$fullName is a sample\n";
		$samples{$f}="$fullName/doc/spldoc/html/index.html";
	    }
	    $debug && print "directory $fullName appears to have a documentation\n";
	    if ($dryrun) {
		print "Would copy $fullName/doc to $pagesLocation\n";
		print "cp -r $fullName/doc $pagesLocation/$fullName\n";
		print "cd $pagesLocation; git add -A $fullName/doc\n";
	    }
	    else {
		system("mkdir -p $pagesLocation/$fullName");
		system("cp -r $fullName/doc $pagesLocation/$fullName");
		system("cd $pagesLocation; git add -A $fullName/doc");
	    }
	    $? >> 8 == 0 or die "Problem adding.";
	}
	else {
	    $debug && print "no docs found in $fullName\n";
	    lookForApp("$fullName");
	}
    }
}

sub main() {
    if ($makeDoc) {
	system("ant spldoc");
    }
    $? >> 8 == 0 or die "Could not build spl doc";
    # Make sure the branch is checked out in location given on the command
  
    if ($useTemp) {
	my $line = `git remote show origin | grep Fetch`;
	$line =~ /Fetch URL:\s+(.*)$/;
	my $url = $1;
        $url =~ /github.com[:\/]\w+\/(.+)\.git$/;
	my $repoName = $1;
	
	$pagesLocation = "/tmp/$repoName";
	die "Cannot create repository since $pagesLocation already exists" if (-e $pagesLocation);
	system("cd /tmp; git clone $url");
    }

    system("cd $pagesLocation; git checkout gh-pages");
    $? >> 8 == 0 or die "Cannot set branch to gh-pages in $pagesLocation";
    # handle the toolkit; need a better way of specifying which toolkit.
    lookForApp(".","test");
    updateIndexHtml();
    my $changes = `cd $pagesLocation; git status -s | grep -v "^?" | wc -l`;
    chomp $changes;
    print "$changes files changed\n";
    if ($changes > 0) {
	if ($dryrun) {
	    print "DryRun: $changes files changed\n";
	}
	elsif ($changes >0 && $doCommit) {
	    system("cd $pagesLocation; git commit -a -m \"$message\"");
	    if ($doPush) {
		system("cd $pagesLocation; git push origin gh-pages");
	    }
	}
    }
    if ($useTemp && $deleteTemp) {
	if ($pagesLocation =~ /tmp/) {
	    system("rm -rf $pagesLocation");
	}
	else {
	    die "Unexpected temporary repository structure; not deleting $pagesLocation";
	}
    }
}

main();
