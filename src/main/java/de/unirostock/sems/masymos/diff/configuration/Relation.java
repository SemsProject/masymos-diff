package de.unirostock.sems.masymos.diff.configuration;

import org.neo4j.graphdb.RelationshipType;

public class Relation extends de.unirostock.sems.masymos.configuration.Relation {
	
	/**
	 * Relationship to describe diffs between two Documents
	 * 
	 */
	public static enum DiffRelTypes implements RelationshipType {
		HAS_DIFF,
		HAS_DIFF_ENTRY,
		
		IS_SOURCE,
		IS_DESTINATION,
		
		DIFF_TRIGGERED_BY
	}
	
}
