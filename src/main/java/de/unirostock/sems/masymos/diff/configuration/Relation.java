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
		
		DIFF_DELETED,
		DIFF_INSERTED,
		DIFF_UPDATE_OLD,
		DIFF_UPDATE_NEW,
		DIFF_MOVE_OLD,
		DIFF_MOVE_NEW
		
	}
	
}
