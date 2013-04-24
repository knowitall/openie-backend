package edu.knowitall.browser.entity;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Function;

public class Entity {

	public final String name;
	public final String fbid;

	private List<String> typeStrings;
	private boolean typesAttached = false;

	public Entity(String name, String fbid) {
		this.name = name;
		this.fbid = fbid;
		this.typeStrings = new LinkedList<String>();
	}

	public void attachTypes(List<String> typeStrings) {
		typesAttached = true;
		this.typeStrings = Collections.unmodifiableList(typeStrings);
	}

	public List<String> retrieveTypes() {
		if (!typesAttached)
			throw new IllegalStateException("Error: types were never attached.");
		else
			return this.typeStrings;
	}
	
	public String serializeToString() {
		String escName = name.replaceAll(":", "_COLON_");
		String escFbid = fbid.replaceAll(":", "_COLON_");
		return String.format("%s:%s", escName, escFbid);
	}
	
	public static Entity deserializeFromString(String str) {
		String[] split = str.split(":");
		String name = split[0].replaceAll("_COLON_", ":");
		String fbid = split[0].replaceAll("_COLON_", ":");
		return new Entity(name, fbid);
	}
	
	public static Function<Entity, String> serializeFunction = new Function<Entity, String>() {
		public String apply(Entity entity) {
			return entity.serializeToString();
		}
	};
	
	public static Function<String, Entity> deserializeFunction = new Function<String, Entity>() {
		public Entity apply(String string) {
			return deserializeFromString(string);
		}
	};
}
