#!/usr/bin/perl
#
# Convert the normal candidates lists (which list via FBID) to lists that
# list via the Lucene indices.
#
# Also, convert the original surface strings into filename links.
#

use DBI;
use Storable;

# Read in the index conversion data
#
$hashfile  = "indices.hash";
$indexfile = "indices.txt";

if (-e $hashfile) {
    print "retrieved freebaseID -> document index hash from storage";
    %freebaseid_to_docindex = %{ retrieve($hashfile) };
} else {
    print "recalculating freebaseID -> document index hash";

    open (F1, $indexfile);

    while ($line = <F1>) {
	chomp $line;
	@parts = split(/\t/,$line);
	$freebase_id = shift @parts;
	$doc_index = shift @parts;

	if ($hit{$freebase_id}) {
	    push @top_senses, $doc_index;
	}
	$freebaseid_to_docindex{$freebase_id} = $doc_index;
    }
    close (F1);

    store (\%freebaseid_to_docindex, $hashfile);
}

# Iterate through all the top5s*.data files, and convert each of them
#
$datafiles_list = `ls -1 top5s*.data`;
@datafiles = split(/\n/,$datafiles_list);

foreach $datafile (@datafiles) {
    print $datafile."\n";

    @f1lines = ();

    open (F1, $datafile);  
    while ($line = <F1>) {
	push @f1lines, $line;
    }
    close (F1);

    push @f1lines, "entity\tlast line";  # Add this to the end to trigger the writeout for the last entity
    $entity = "not set yet";

    @current_set = ();
    %id_to_rank = {};
    %id_to_inlinks = {};
    %id_to_names = {};
    $exact_used = 0;

    open (FX, ">".$datafile.".out");

    foreach $line (@f1lines) {
	chomp $line;
	@parts = split(/\t/,$line);
	$type = shift @parts;

	if ($type eq "entity") {
	    if ($entity ne "not set yet") {
		print FX "entity\t$entity\n";
		@current_set = sort{ $id_to_inlinks{$b} <=> $id_to_inlinks{$a} } @current_set;

		# If there are 6 unique elements and #6 is the exact match, then replace it with #5
		if (0+@current_set eq 6) {
		    if ($id_to_rank{$current_set[5]} eq "exact") {
			$elt = pop @current_set;
			$tmp = pop @current_set;
			push @current_set, $elt;
		    }
		}

		if (0+@current_set eq 0) {
		    print FX "id\tnone\n";
		} else {
		    print FX "id";

		    for ($i=0; $i<5; $i++) {
			if (0+@current_set > 0) {
			    $nextid = shift @current_set;
			    $rank = $id_to_rank{$nextid};
			    $inlinks = $id_to_inlinks{$nextid};
			    $name = $id_to_names{$nextid};

			    $docindex = $freebaseid_to_docindex{$nextid};

			    print FX "\t".$nextid."|".$docindex;
			}
		    }
		    print FX "\n";
		}
	    }

	    $entity = shift @parts;
	    @current_set = ();
	    %id_to_rank = {};
	    %id_to_inlinks = {};
	    %id_to_names = {};
	    $exact_used = 0;
	} elsif ($type eq "hit") {
	    $rank = shift @parts;
	    $fbid = shift @parts;
	    $inlinks = shift @parts;
	    $name = shift @parts;

	    if ($rank eq "exact") {
		$id_to_rank{$fbid} = $rank;
	    }

	    if (!$id_to_inlinks{$fbid}) {
		push @current_set, $fbid;
		$id_to_inlinks{$fbid} = $inlinks;
		$id_to_names{$fbid} = $name;
	    }
	}
    }

    close (FX);
}
