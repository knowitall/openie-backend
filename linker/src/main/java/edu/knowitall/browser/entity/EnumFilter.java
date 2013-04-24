package edu.knowitall.browser.entity;
import java.util.Collections;
import java.util.Vector;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

class EnumFilter extends Filter {

    Vector<Integer> likelyDocs;

    public EnumFilter(Vector<Integer> likelyDocs_input) {
	// Copy the vector by value
	likelyDocs = new Vector<Integer>();
	for (int i=0; i<likelyDocs_input.size(); i++) {
	    likelyDocs.add(likelyDocs_input.get(i));
	}

	// The ID iterator needs to be sorted
	Collections.sort(likelyDocs);
    }

    public DocIdSet getDocIdSet(IndexReader reader) {
	DocIdSet dis = new EnumDocIdSet(likelyDocs);

	return dis;
    }
}