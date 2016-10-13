package de.unirostock.sems.masymos.diff.traverse;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import de.unirostock.sems.masymos.configuration.NodeLabel;
import de.unirostock.sems.masymos.configuration.Relation.DocumentRelTypes;

public class DBModelTraverser extends de.unirostock.sems.masymos.database.traverse.ModelTraverser {

	/**
	 * gets the model node from a document node
	 * 
	 * @param documentNode
	 * @return
	 */
	public static Node getModelFromDocument(Node documentNode) {
		
		if( documentNode == null )
			return null;
		
		Node modelNode = null;
		if( documentNode.hasLabel(NodeLabel.Types.DOCUMENT) ) {
			Relationship relation = documentNode.getSingleRelationship( DocumentRelTypes.HAS_MODEL, Direction.OUTGOING );
			if( relation == null )
				System.out.println( "no model under document " + documentNode.getId() );
			modelNode = relation != null ? relation.getEndNode() : null;
		}
		else if( documentNode.hasLabel(NodeLabel.Types.MODEL) )
			return documentNode;
		
		if( modelNode != null && modelNode.hasLabel(NodeLabel.Types.MODEL) )
			return modelNode;
		else
			return null;
	}
	
}
