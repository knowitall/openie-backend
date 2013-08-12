package edu.knowitall.browser.entity;

import java.util.List;

public class EntityLink {

	public final Entity entity;
	
	public final double candidateScore;
	
	public final double docSimScore;
	
	public final int inlinks;
	
	public final double combinedScore;

	public EntityLink(String name, String fbid, double candidateScore, int inlinks, double docSimScore) {
		this.entity = new Entity(name, fbid);
		this.candidateScore = candidateScore;
		this.inlinks = inlinks;
		this.docSimScore = docSimScore;
		this.combinedScore = candidateScore * Math.log(inlinks) * docSimScore;
	}

	public void attachTypes(List<String> typeStrings) {
		entity.attachTypes(typeStrings);
	}

	public List<String> retrieveTypes() {
		return entity.retrieveTypes();
	}
}