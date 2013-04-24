#!/usr/local/bin/perl
#
# This modification does word-matching instead of substring. So, "Thomas Evans"
# would match up with "Thomas B. Evans, Jr." This procedure is also many times
# faster.
#
# Could be even faster if we just ignore words like "the" (50,000 matches) and
# "of" (100,000 matches), but I think we're averaging over 100 judgements per
# second now already.
#
# If the word we're matching against begins with the word "the" then strip that
# word and call it an instance (rather than an equals).
#

open (F1, "./occam.data");

# Split and parse into a map "argseen" that counts
# the number of times an argument is seen.
# also keep track of some probably-unnecessary counters of num spaces etc.

while ($line = <F1>) {
    chomp $line;
    @parts = split(/\t/,$line);
    $sent_idx = $parts[0];
    $arg1 = $parts[1];

    # For now we'll just assume here that the ReVerb arg1s are good
    # enough, and don't have to be further processed.

    if ($argseen{$arg1}) {
	$repeat++;
    } else {
	$new++;
	$argseen{$arg1}++;
	push @allargs, $arg1;

	if ($arg1 =~ /\ /) {
	    $space++;
	} else {
	    $nospace++;
	}
    }
}

close (F1);

print "Repeat arg1 strings: $repeat\n";
print "New arg1 strings: $new\n";
print "Total arg1 strings: ".($repeat+$new)."\n";
print "With spaces: $space\n";
print "No spaces: $nospace\n";

print "Caching the entity list, and storing surface forms for exact matching.\n";

open (F1, "output.fbid-prominence.sorted");

while ($line = <F1>) {
    $k++;

    if ($k % 100000 == 0) {
        print "$k lines processed.\n";
    }

    @parts = split(/\t/,$line);
    $fbid = shift @parts;
    $fbid = substr($fbid, rindex($fbid, "/")+1);
    $inlinks = shift @parts;
    $title = shift @parts;
    
    @allwords = split(/\,|\ /,$title);  # split the words along commas and spaces

    %used_already = {};

    foreach $word (@allwords) {
	if ($used_already{$word}) {
	    # If a line repeats a word several times, only add it the first time
	    #print "Already added $word from $title.\n";	    
	} else {
	    if (!$seen{$word}) {  # start a new entry
		$seen{$word} = 1;
		$cached{$word} = $fbid;
		$new_word++;
	    } else {  # augment an existing entry		
		$seen{$word}++;
		$cached{$word} = $cached{$word}."\t".$fbid;
	    }
	    $used_already{$word}++;
	}
    }

    $fbid_to_line{$fbid} = "hit\tword\t$fbid\t$inlinks\t$title\n";

    # store any exact matches (minus parens)
    if ($title =~ /(.*) \(.*\)/) {
	$tmptitle = $1;
	if ($titlemap{$tmptitle}) {
	} else {
	    $titlemap{$tmptitle} = "hit\texact\t$fbid\t$inlinks\t$title\n";
	}
    }
    if ($titlemap{$title}) {
    } else {
	$titlemap{$title} = "hit\texact\t$fbid\t$inlinks\t$title\n";
    }
}

close (F1);

print "Total words from entity list: $new_word\n";

$senses = 5;  # Admit up to 5 senses per word

$time_before_er = time();

$k = 0;

open (FX, ">top5s.2a2.data");

foreach $entity_in_english (@allargs) {
    $k++;
    if ($k%500 == 0) {
	$elapsed = time() - $time_before_er;
	print "K: $k at time $elapsed.\n";
    }

    print FX "entity\t".$entity_in_english."\n";

    $exactmatched = 0;

    # If there's an exact match, then include it.
    if ($titlemap{$entity_in_english}) {
	print FX $titlemap{$entity_in_english};
	$exactmatched = 1;
    }

    # If the word begins with "the" then shift that piece off
    @e_i_e_parts = split(/\ /,$entity_in_english);
    $firstword = shift @e_i_e_parts;
    if ((lc $firstword) eq "the") {
	$newfirst = shift @e_i_e_parts;
	$newfirst = (uc (substr($newfirst,0,1))).(substr($newfirst,1));
	unshift @e_i_e_parts, $newfirst;
	$entity_in_english = join(" ",@e_i_e_parts);
	$the_used++;
    }

    # If there's an exact match, then include it.
    if ($exactmatched eq 0) {
	if ($titlemap{$entity_in_english}) {
	    print FX $titlemap{$entity_in_english};
	}
    }

    if (!($entity_in_english =~ /\ /)) {
	# no spaces, so just read off the answers directly

	$cachedresult = $cached{$entity_in_english};
	@cachedparts = split(/\t/,$cachedresult);

	$hitcount = 0;
	search1: foreach $cachedpart (@cachedparts) {
	    print FX $fbid_to_line{$cachedpart};

	    $hitcount++;
	    if ($hitcount >= $senses) {
		last search1;
	    }	    
	}
    } else {
	# spaces, so find and intersect the appropriate lists to find the top-k
	# obtain the lists for each word

	@components = split(/\,|\ /,$entity_in_english);  # split the words along commas and spaces

	@lists = ();
	$fail = "";
	foreach $component (@components) {
	    if ($cached{$component}) {
		push @lists, $cached{$component};
	    } else {
		# try standardizing the caps
		$newcap = (uc (substr($component,0,1))).(lc (substr($component,1)));
		if ($cached{$newcap}) {
		    push @lists, $cached{$newcap};
		} else {
		    $newcap = lc $newcap;
		    if ($cached{$newcap}) {
			push @lists, $cached{$newcap};
		    } else {
			$fail .= "(no matches on \"$component\")";
		    }
		}
	    }
	}

	if ($fail ne "") {
	    print FX $fail."\n";
	} else {
	    %list_memberships = {};
	    @candidates = ();
	    $necessary = 0;

	    foreach $list (@lists) {
		$necessary++;
		@listparts = split(/\t/,$list);

		foreach $listpart (@listparts) {
		    if (!$list_memberships{$listpart}) {
			push @candidates, $listpart;
		    }
		    $list_memberships{$listpart}++;
		}
	    }

	    $hitcount = 0;
	    $passed = 0;
	    $failed = 0;

	    search2: foreach $candidate (@candidates) {
		if ($list_memberships{$candidate} == $necessary) {
		    #print FX "Adding $candidate at ".$list_memberships{$candidate}."\n";
		    print FX $fbid_to_line{$candidate};

		    $hitcount++;
		    if ($hitcount >= $senses) {
			last search2;
		    }
		    $passed++;
		} else {
		    $failed++;
		}
	    }
	    if ($passed + $failed > 100000) {
		print "  Passed $passed Failed $failed for $entity_in_english.\n";
	    }
	}
	#chomp $line;
	#@parts = split(/\t/,$line);
	#$fbid = shift @parts;
	#$fbid = substr($fbid, rindex($fbid, "/")+1);
	#$inlinks = shift @parts;
	#$title = shift @parts;
	
	#print FX "hit\t$hitcount\t$fbid\t$inlinks\t$title\n";
    }
    
    close (F1);
}

close (FX);

$time_after_er = time();

print "\"The\" words filtered: ".$the_used."\n";
