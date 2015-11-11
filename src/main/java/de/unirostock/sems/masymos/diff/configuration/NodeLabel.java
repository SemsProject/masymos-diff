package de.unirostock.sems.masymos.diff.configuration;

import org.neo4j.graphdb.Label;

public class NodeLabel extends de.unirostock.sems.masymos.configuration.NodeLabel {

	public static enum DiffTypes implements Label {

		//DIFF specific
		DIFF,
		DIFF_NODE,
		DIFF_DELETE,
		DIFF_INSERT,
		DIFF_UPDATE,
		DIFF_MOVE

	}

}