package edu.knowitall.browser.entity;

import java.util.List;

public class EntityLink {

	public final Entity entity;
	public final double score;
	public final int inlinks;

	public EntityLink(String name, String fbid, double score, int inlinks) {
		this.entity = new Entity(name, fbid);
		this.score = score;
		this.inlinks = inlinks;
	}

	public void attachTypes(List<String> typeStrings) {
		entity.attachTypes(typeStrings);
	}

	public List<String> retrieveTypes() {
		return entity.retrieveTypes();
	}
}