#!/usr/local/bin/perl
#
# Collect the results back together, apply the scoring function, and write out the
# best match on each line.
#

open (F1, "top5s.2a2.data");

while ($line = <F1>) {
    chomp $line;
    @parts = split(/\t/,$line);
    if ($parts[0] eq "hit") {
	$fbid = $parts[2];
	$inlinks = $parts[3];
	$title = $parts[4];

	$fbid_to_inlinks{$fbid} = $inlinks;
	$fbid_to_title{$fbid} = $title;

	# calculate string match level between $arg and $title
	if ($arg eq $title) {
	    $sml{$arg}{$fbid} = 5;
	} else {
	    $inexact = 0;

	    if ($title =~ /(.*) \(.*\)/) {
		if ($1 eq $arg) {
		    $inexact = 1;
		    $sml{$arg}{$fbid} = 4;
		}
	    }
	    if ($inexact eq 0) {
		@entity_parts = split(/\ /,$title);
		@arg_parts = split(/\ /,$arg);
		$word_diff = 0+@entity_parts-@arg_parts;
		$string_match_level = 4-$word_diff;
		if ($string_match_level < 1) { $string_match_level = 1; }
		$sml{$arg}{$fbid} = $string_match_level;
	    }
	}
	#print "$arg to $title has a match level of: ".$sml{$arg}{$fbid}."\n";

    } elsif ($parts[0] eq "entity") {
	$arg = $parts[1];
    }
}
close (F1);

open (F1, "batch-output-scores");

while ($line = <F1>) {
    chomp $line;
    @parts = split(/\t/,$line);
    $term = $parts[0];
    $file = $parts[1];
    $match = $parts[2];   # the Freebase ID
    $value = $parts[3];   # context-match value

    $score = log($fbid_to_inlinks{$match}) * $value * $sml{$term}{$match};

    if ($best{$file}) {
	if ($score > $best_score{$file}) {
	    $best{$file} = $match;
	    $best_score{$file} = $score;
	}
    } else {
	$best{$file} = $match;
	$best_score{$file} = $score;
    }
}

close (F1);

open (F1, "./occam.data");
open (FX, ">occam.matched");

while ($line = <F1>) {
    chomp $line;
    @parts = split(/\t/,$line);
    $file = $parts[0];
    
    print FX $line;
    print FX "\t";
    if ($best{$file}) {
	print FX $best{$file}."\t".$fbid_to_title{$best{$file}}."\t".$best_score{$file}."\n";
    } else {
	print FX "none\tnone\tnone\n";
    }
}

close (FX);
close (F1);
