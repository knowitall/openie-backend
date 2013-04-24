package edu.knowitall.browser.entity;
import java.io.IOException;
import java.util.Vector;

import org.apache.lucene.search.DocIdSetIterator;

class EnumDocIdSetIterator extends DocIdSetIterator {

    Vector<Integer> ofInterest = new Vector<Integer>();
    int position = -1;

    public EnumDocIdSetIterator(Vector<Integer> likelyDocs) {
	ofInterest = likelyDocs;
    }

    // return -1 if nextDoc() or advance(int) were not called yet.
    // return NO_MORE_DOCS if the iterator has exhausted.
    // else return the doc ID it is currently on.

    public int docID() {
	if (position == -1) {
	    return -1;
	} else if (position >= ofInterest.size()) {
	    return NO_MORE_DOCS;
	} else {
	    return ((Integer)ofInterest.get(position)).intValue();
	}
    }

    // advances to the next document in the set, and returns the doc it
    // is currently on, or NO_MORE_DOCS if there are no more docs in the set.

    public int nextDoc() throws IOException {
	position++;
	return docID();
    }

    public int advance(int target) throws IOException {
	int doc;
	while ((doc = nextDoc()) < target) {
	}
	return doc;
    }
}