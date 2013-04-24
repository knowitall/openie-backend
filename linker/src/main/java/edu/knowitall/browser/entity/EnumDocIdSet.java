package edu.knowitall.browser.entity;
import java.util.Vector;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

class EnumDocIdSet extends DocIdSet {

    Vector<Integer> likelyDocs;

    public EnumDocIdSet(Vector<Integer> likelyDocs) {
	this.likelyDocs = likelyDocs;
    }

    public boolean isCacheable() {
	return true;
    }

    public DocIdSetIterator iterator() {
	return new EnumDocIdSetIterator(likelyDocs);
    }
}