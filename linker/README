Entity linking
==============

Entity linking is the task of mapping a string, like "William Clinton," to the appropriate Freebase entity. Sometimes, we want the Wikipedia page instead of the Freebase entity.

This document was taken from the wiki at: https://dada.cs.washington.edu/knowitall/wiki/index.php/Entity_linking

Theory
------

Our entity linking system consists of two steps: candidate finding, and context matching.
In the candidate finding step, we match the string to a Wikipedia article by looking up the string in a resource called Crosswikis. In previous work, we found candidates through string matching, selecting all Wikipedia articles whose titles are a superset of the string we want to link (not exactly, see thesis link below).

In the context matching step, we take some given context sentences, and compute a tf-idf similarity with the candidate Wikipedia articles. When we link Open IE tuples, for example, the context sentences are the instances of that tuple.

Each candidate link gets a score, which is candidate_score * context_score * log(num_inlinks).
For more details, see chapter 4 of Tom Lin's thesis. His work describes the string matching candidate finder, not the Crosswikis-based one.

How to use the entity linker
----------------------------

The entity linking code is in the openie-linker subproject of openie-backend. The entity-linking repository provides a sample of how to use it. ReverbEntityLinkingExperiment, for example, adds in relation links for ReVerb extraction groups. It's similar to ScoobiEntityLinker in openie-backend, but is not written to run on Hadoop.

The entity linker requires the following supporting files.

1. browser-freebase/
2. entitylinking/
3. crosswikis/crosswikis/

These directories belong in some directory, called the "supporting data directory." If you are on rv-n02 through rv-n10, the supporting data directory is /scratch/, /scratch2/, /scratch3/, or /scratch4/.

If you would like to run the entity linker on your own machine, download all the supporting data (9 GB compressed / 37 GB uncompressed) from rv-n02:/scratch6/supporting_data.tgz. Also inside supporting_data.tgz is also a file with 100 ReVerb extractions you can test with. You will also need to start the Derby network server on your machine.

To run ReverbEntityLinkingExperiment, follow these steps:

1. Clone openie-backend and run sbt publish-local
2. Clone entity-linking
3. Run sbt 'run-main edu.washington.cs.knowitall.entity.experiment.ReverbEntityLinkingExperiment /path/to/input/openie_100.txt /path/to/supporting_data/ /path/to/output/openie_100_out.txt'
