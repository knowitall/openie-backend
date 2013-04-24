package edu.knowitall.browser.entity;

import java.io.Serializable;

public class Pair<T, S> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public T one;
	public S two;
	
	public Pair(T s1, S s2) {
		one = s1;
		two = s2;
	}
	
	@Override
	public String toString() {
		return "Pair["+one+", "+two+"]";
	}
	
	public static <A,B> Pair<A, B> from(A a, B b) { return new Pair<A, B>(a, b); }
}
