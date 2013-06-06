package edu.knowitall.browser.entity;

// batch_match.java
//
// syntax: java batch_match
//
// Runs context match for a series of entities and candidates.
//
// expected input files:
//   batch-data - all the data to match, this file is made from:
//       ./occam_sorted.data - all the tuples we want to disambiguate
//       ./top5s_wordmatch.data.out - the cached
//   ./sources/ - all the source data files
//
// output files:
//   matching_output_scores_all.txt

// matching_input_entities.txt - a text file containing the indices being considered
// matching_input_context.txt - a text file pointing to the source documents to classify into the entities

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.lucene.store.FSDirectory;

public class batch_match {

    private IndexReader reader;

    private Searcher searcher;

    private MoreLikeThis mlt;

    public batch_match(File basePath) throws CorruptIndexException, IOException {
        File index = Constants.batchMatchPath(basePath);
        System.err.println("Using " + index + " as my index.");

        reader = IndexReader.open(FSDirectory.open(index), true);

        searcher = new IndexSearcher(reader);

        mlt = new MoreLikeThis(reader); // create a
        // MoreLikeThis
        // using
        // current
        // index
    }

    public void go() throws IOException {
        // set up the search index

        Vector<String> all_arg1 = new Vector<String>();
        // load in the map of entity to source sentences and entity to
        // freebase hits.
        Hashtable<String, String> entity_to_sources = new Hashtable<String, String>();
        Hashtable<String, String> entity_to_topk = new Hashtable<String, String>();

        BufferedReader all_tuples = new BufferedReader(new FileReader(new File(
                "batch-data")));
        String text = null;
        while ((text = all_tuples.readLine()) != null) {
            // First, split the line across tabs
            StringTokenizer st = new StringTokenizer(text, "\t");
            String entity = st.nextToken();
            String sources = st.nextToken();
            if (!st.hasMoreTokens())
                continue;
            String topk = st.nextToken();

            all_arg1.add(entity);
            entity_to_sources.put(entity, sources);
            entity_to_topk.put(entity, topk);

            // System.out.println("For " + entity + ": " + sources + " -> "
            // + topk);
        }
        all_tuples.close();

        BufferedWriter outputScores = new BufferedWriter(new FileWriter(
                "batch-output-scores"));

        long startTime = System.currentTimeMillis();

        // iterate through all the arguments
        //
        for (int a = 0; a < all_arg1.size(); a++) {
            // for (int a=0; a<1500; a++) {
            String arg1 = all_arg1.get(a);
            String sources = entity_to_sources.get(arg1);
            String topk = entity_to_topk.get(arg1);

            if ((a > 0) && (a % 250 == 0)) {
                long currentTime = System.currentTimeMillis();
                long millisElapsed = currentTime - startTime;
                long argumentAverage = millisElapsed / (long) a;

                System.out.println("Finished: " + a + " in "
                        + (millisElapsed / 1000) + " seconds, so "
                        + argumentAverage + " ms/arg");
            }

            // System.out.println(arg1 + " topk is " + topk);

            processSingleArg(outputScores, arg1, sources, topk);
        }

        outputScores.close();

    }

    private void processSingleArg(BufferedWriter outputScores, String arg,
            String sources, String topk) throws IOException,
            CorruptIndexException {
        // load the entities that we're mapping to
        Vector<Integer> likelyDocs = new Vector<Integer>();

        if (!topk.equals("none")) {
            StringTokenizer st = new StringTokenizer(topk, ",");
            while (st.hasMoreElements()) {
                String fbid_and_index = st.nextToken();

                // System.out.println("  fbid_and_index is " +
                // fbid_and_index);
                StringTokenizer st2 = new StringTokenizer(fbid_and_index, "|");

                String fbid = st2.nextToken(); // note: save the fbid to
                                               // write out later

                String idx = "no match";
                if (st2.hasMoreElements()) {
                    idx = st2.nextToken();
                    // System.out.println("  " + fbid + " = " + idx);
                    likelyDocs.add(new Integer(idx));
                }
            }
        }

        if (likelyDocs.size() == 0) {
            // no candidates to add. put in the null result, and go to
            // next entity
        } else {
            // load the text files that we're matching against

            Vector<String> inputContext = new Vector<String>();
            StringTokenizer st = new StringTokenizer(sources, "|");
            Hashtable<String, String> sourceFileToId = new Hashtable<String, String>();

            while (st.hasMoreElements()) {
                String nextElt = st.nextToken();
                String char1 = nextElt.substring(0, 1);
                String char2 = nextElt.substring(1, 2);
                String char3 = nextElt.substring(2, 3);

                String sourcefile = "/scratch/usr/tlin/at_scale/" + char1 + "/"
                        + char2 + "/" + char3 + "/sources/" + nextElt + ".data";
                // System.out.println("  sf: " + sourcefile);
                sourceFileToId.put(sourcefile, nextElt);
                inputContext.add(sourcefile);
            }

            // do the actual matching...
            //

            for (int doc_index = 0; doc_index < inputContext.size(); doc_index++) {
                String icFilename = inputContext.get(doc_index);
                // System.out.println("Opening file: " + icFilename);

                File queryFile = new java.io.File(inputContext.get(doc_index));
                FileReader fis;
                try {
                    fis = new FileReader(queryFile);
                } catch (FileNotFoundException fne) {
                    fne.printStackTrace();
                    continue;
                }

                List<Pair<String, Double>> results = processSingleArgWithInputStream(
                        arg, likelyDocs, fis);
                String sourceId = sourceFileToId.get(icFilename);
                for (Pair<String, Double> result : results) {
                    outputScores.write(arg + "\t" + sourceId + "\t"
                            + result.one + "\t" + result.two + "\n");
                }

                fis.close();
            }
        }
    }

    public List<Pair<String, Double>> processSingleArgWithInputStream(
            String arg, Vector<Integer> likelyDocs, Reader frdr)
            throws IOException {

        // System.err.println(String.format("processSingleArgWithInputStream(%s, %s, %s)",
        // arg, likelyDocs, frdr));

        List<Pair<String, Double>> returnList = new LinkedList<Pair<String, Double>>();

        Query query;

        try {
            query = mlt.like(frdr);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error on arg: " + arg);
            return Collections.emptyList();
        }

        // Query query = mlt.like(queryFile);
        // System.out.println("Query on file " + doc_index +
        // " is: " + query);
        // outputScores.write("file\t" +
        // inputContext.get(doc_index) + "\t" + query + "\n");

        Filter enumFilter = new EnumFilter(likelyDocs); // my
                                                        // enumerated-filter

        TopScoreDocCollector collector = TopScoreDocCollector.create(10, false);
        searcher.search(query, enumFilter, collector);
        // searcher.search(query, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        int numTotalHits = hits.length;

        for (int i = 0; i < numTotalHits; i++) {
            String docPath = (reader.document(hits[i].doc)).get("path");
            double scoring = Math.floor((1000 * hits[i].score) + 0.5) / 1000;

            String match = docPath.substring(docPath.lastIndexOf("/") + 1,
                    docPath.lastIndexOf("."));
            // System.out.println("    score = " + scoring +
            // " for " + match);
            // String sourceId = sourceFileToId.get(icFilename);
            returnList.add(new Pair<String, Double>(match, scoring));

        }
        return returnList;
    }

    public Iterable<Pair<String, Double>> processSingleArgWithSources(
            String arg, Iterable<Integer> likelyDocs, Iterable<String> sources)
            throws IOException {

        StringBuilder sbuf = new StringBuilder();
        boolean first = true;
        for (String src : sources) {
            if (!first)
                sbuf.append("\n");
            first = false;
            sbuf.append(src);
        }

        Vector<Integer> likelyDocsVector = new Vector<Integer>();
        for (Integer i : likelyDocs) {
            likelyDocsVector.add(i);
        }

        StringReader srcReader = new StringReader(sbuf.toString());

        return processSingleArgWithInputStream(arg, likelyDocsVector, srcReader);
    }

    public static void main(String[] args) throws CorruptIndexException,
            IOException {
        File basePath = new File(args[0]);
        new batch_match(Constants.batchMatchPath(basePath)).go();
    }
}
