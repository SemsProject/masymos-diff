package de.unirostock.sems.masymos.diff;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.configuration.NodeLabel;
import de.unirostock.sems.masymos.diff.configuration.Property;
import de.unirostock.sems.masymos.diff.configuration.Relation;
import de.unirostock.sems.masymos.diff.exception.ModelAccessException;
import de.unirostock.sems.masymos.diff.traverse.DBModelTraverser;

public abstract class DiffUtils {
	
	protected static Manager manager = Manager.instance();
	protected static GraphDatabaseService graphDB = Manager.instance().getDatabase();
	
	public static String getModelType( Node modelNode ) {
		
		if( modelNode == null )
			return null;
		
		if( modelNode.hasLabel(NodeLabel.Types.MODEL) == false )
			modelNode = DBModelTraverser.getModelFromDocument(modelNode);
		
		// check again, if result from traversal is null
		if( modelNode == null )
			return null;
		
		if( modelNode.hasLabel(NodeLabel.Types.CELLML_MODEL) )
			return Property.ModelType.CELLML;
		else if( modelNode.hasLabel(NodeLabel.Types.SBML_MODEL) )
			return Property.ModelType.SBML;
		
		return null;
	}
	
	public static String downloadDocumentToString( Node documentNode ) throws ModelAccessException {
		
		URL url = null;
		try {
			url = getUrlFromDocument(documentNode);
			
			return IOUtils.toString(url, "UTF-8");
		} catch (IOException e) {
			throw new ModelAccessException("Cannot access " + url, e);
		}
		
	}
	
	public static URL getUrlFromDocument( Node documentNode ) throws ModelAccessException {
		
		String url = null;
		if( documentNode.hasProperty(Property.General.XMLDOC) )
			url = (String) documentNode.getProperty(Property.General.XMLDOC);
		else if( documentNode.hasProperty(Property.General.URI) )
			url = (String) documentNode.getProperty(Property.General.URI);
		
		try {
			return new URL(url);
		} catch (MalformedURLException e) {
			throw new ModelAccessException("Invalid url stored in document node" + url, e);
		}
	}
	
	/**
	 * running through all model part oriented sub-nodes of a model node an stores
	 * their xml-id in a map, along with the node object.
	 * 
	 * @param modelNode
	 * @return
	 */
	public static Map<String, Node> traverseModelParts( Node modelNode ) {
		
		if( modelNode.hasLabel(NodeLabel.Types.MODEL) == false )
			throw new IllegalArgumentException("The entry node is not a modelNode");
		
		Map<String, Node> partList = new HashMap<String, Node>();
		
		try ( Transaction tx = manager.createNewTransaction() ) {
			
			// get ID of the model itself
			insertIntoPartList(modelNode, partList);
			
			// traverse all CellMl components of this model
			if( modelNode.hasLabel(NodeLabel.Types.CELLML_MODEL) ) {
				Iterable<Relationship> cellMlComponents = modelNode.getRelationships(Direction.OUTGOING, Relation.CellmlRelTypes.HAS_COMPONENT, Relation.CellmlRelTypes.HAS_VARIABLE, Relation.CellmlRelTypes.HAS_REACTION);
				for( Relationship componentRelation : cellMlComponents ) {
					Node componentNode = componentRelation.getEndNode();
					String componentId = insertIntoPartList(componentNode, partList);
					
					// iterate relations under a Component (Reactions, Variables)
					Iterable<Relationship> cellMlSubComponents = componentNode.getRelationships(Direction.OUTGOING, Relation.CellmlRelTypes.HAS_REACTION, Relation.CellmlRelTypes.HAS_VARIABLE);
					for( Relationship subComponentRelation : cellMlSubComponents ) {
						Node subComponentNode = subComponentRelation.getEndNode();
						insertIntoPartList(subComponentNode, componentId, partList);
					}
				}
			}
			
			// traverse all SBML parts
			if( modelNode.hasLabel(NodeLabel.Types.SBML_MODEL) ) {
				Iterable<Relationship> sbmlParts = modelNode.getRelationships(Direction.OUTGOING, Relation.SbmlRelTypes.HAS_COMPARTMENT,
						Relation.SbmlRelTypes.HAS_EVENT, Relation.SbmlRelTypes.HAS_FUNCTION, Relation.SbmlRelTypes.HAS_MODIFIER,
						Relation.SbmlRelTypes.HAS_PARAMETER, Relation.SbmlRelTypes.HAS_PRODUCT, Relation.SbmlRelTypes.HAS_REACTANT,
						Relation.SbmlRelTypes.HAS_REACTION, Relation.SbmlRelTypes.HAS_RULE, Relation.SbmlRelTypes.HAS_SPECIES );
				for( Relationship partRelation : sbmlParts ) {
					Node partNode = partRelation.getEndNode();
					insertIntoPartList(partNode, partList);
				}
			}
			
			tx.success();
		}
		
		return partList;
	}
	
	private static String insertIntoPartList( Node node, Map<String, Node> partList ) {
		return insertIntoPartList(node, null, partList);
	}
	
	private static String insertIntoPartList( Node node, String prefix, Map<String, Node> partList ) {
		
		String id = null;
		if( node.hasProperty(Property.General.ID) )
			id = (String) node.getProperty( Property.General.ID );
		else if( node.hasProperty(Property.General.NAME) )
			id = (String) node.getProperty( Property.General.NAME );
		
		// add prefix
		if( prefix != null )
			id = prefix + ":" + id;
		
		if( id != null && id.isEmpty() == false ) {
			if( partList.containsKey(id) == false )
				partList.put( id, node );
			else
				System.err.println("Prevented id overwrite in part list for " + id);
		}
		
		return id;
	}
	
}
