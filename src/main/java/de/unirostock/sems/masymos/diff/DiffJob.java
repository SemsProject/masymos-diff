package de.unirostock.sems.masymos.diff;

import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;

import org.jdom2.Attribute;
import org.jdom2.Element;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unirostock.sems.bives.api.Diff;
import de.unirostock.sems.bives.cellml.algorithm.CellMLValidator;
import de.unirostock.sems.bives.cellml.api.CellMLDiff;
import de.unirostock.sems.bives.cellml.parser.CellMLDocument;
import de.unirostock.sems.bives.ds.ModelDocument;
import de.unirostock.sems.bives.sbml.algorithm.SBMLValidator;
import de.unirostock.sems.bives.sbml.api.SBMLDiff;
import de.unirostock.sems.bives.sbml.parser.SBMLDocument;
import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.configuration.NodeLabel;
import de.unirostock.sems.masymos.diff.configuration.PatchType;
import de.unirostock.sems.masymos.diff.configuration.Property;
import de.unirostock.sems.masymos.diff.configuration.Relation;
import de.unirostock.sems.masymos.diff.data.XmlId;
import de.unirostock.sems.masymos.diff.exception.DiffException;
import de.unirostock.sems.masymos.diff.exception.DiffProcessingException;
import de.unirostock.sems.masymos.diff.exception.ModelAccessException;
import de.unirostock.sems.masymos.diff.exception.ModelTypeException;
import de.unirostock.sems.masymos.diff.traverse.DBModelTraverser;
import de.unirostock.sems.xmlutils.ds.DocumentNode;
import de.unirostock.sems.xmlutils.ds.TreeNode;

/**
 * Runnable Job generating a diff between 2 model versions
 * 
 * @author martin
 *
 */
public class DiffJob implements Runnable {

	/**
	 * version of the generator routine.
	 * Should be increased, with BiVeS update!
	 */
	public static final int GENERATOR_VERSION = 2;
	
	private static Logger log = LoggerFactory.getLogger(DiffJob.class);
	protected static GraphDatabaseService graphDB = Manager.instance().getDatabase();
	protected static Manager manager = Manager.instance();

	protected Node documentNodeA = null;
	protected Node documentNodeB = null;

	protected Node modelNodeA = null;
	protected Node modelNodeB = null;

	protected ModelDocument modelA = null;
	protected ModelDocument modelB = null;

	protected Node diffNode = null;
	protected Diff diff = null;
	
	/**
	 * creates a new Diff Job. no intensive calculation included.
	 * @param nodeA
	 * @param nodeB
	 */
	public DiffJob(Node nodeA, Node nodeB) {

		if( nodeA == null || nodeB == null )
			throw new IllegalArgumentException("both nodes are not allowed to be null.");
		
		this.documentNodeA = nodeA;
		this.documentNodeB = nodeB;
		
	}

	/**
	 * starts the diff calculation. Should run as separate thread, called through 
	 */
	@Override
	public void run() {
		
		try (Transaction tx = manager.createNewTransaction()) {
			
			log.debug("Traversing document and model nodes");
			
			log.trace( "Document A labels: {}", documentNodeA.getLabels() );
			log.trace( "Document B labels: {}", documentNodeB.getLabels() );
			
			// traverse to Document nodes
//			documentNodeA = DBModelTraverser.getDocumentFromModel(documentNodeA);
//			documentNodeB = DBModelTraverser.getDocumentFromModel(documentNodeB);
			
			if( documentNodeA == null || documentNodeB == null )
				throw new IllegalArgumentException("One or both document nodes are null");
			
			// traverse to Model nodes (one below document)
			modelNodeA = DBModelTraverser.getModelFromDocument(documentNodeA);
			modelNodeB = DBModelTraverser.getModelFromDocument(documentNodeB);
			
			if( log.isInfoEnabled() )
				log.info( "comparing model {} and model {}", 
						documentNodeA.getProperty(Property.General.XMLDOC, null),
						documentNodeB.getProperty(Property.General.XMLDOC, null) );
			
			log.debug("Creating diff node");
			
			createDiffNode();
			// link the diff, to the models/documents
			linkDiffNode();
			
			log.debug("generating diff");
			generateDiff();

			// do the actual work
			log.debug("inserting diff into graph db");
			insertDiff();
			
			// commit stuff to the DB
			log.debug("committing transaction");
			tx.success();
			log.info("committed transaction");
		}
		catch (DiffException e) {
			// exception during diff stuff
//			log.info( "Exception while comparing model {} and model {}", 
//					documentNodeA.getProperty(Property.General.XMLDOC, null),
//					documentNodeB.getProperty(Property.General.XMLDOC, null) );
			log.error("Creating Diff failed", e);
		}
		catch (Exception e) {
//			log.info( "Exception while comparing model {} and model {}", 
//					documentNodeA.getProperty(Property.General.XMLDOC, null),
//					documentNodeB.getProperty(Property.General.XMLDOC, null) );
			log.error("Unexpected exception during diff generation", e);
		}

	}
	
	private Node createDiffNode() {

		Node diffNode = null;

		diffNode = graphDB.createNode();
		diffNode.addLabel( NodeLabel.DiffTypes.DIFF );
		// set properties
		diffNode.setProperty(Property.DiffNode.GENERATED, new Date().getTime());
		diffNode.setProperty(Property.DiffNode.GENERATOR_VERSION, GENERATOR_VERSION);

		this.diffNode = diffNode;

		return diffNode;
	}

	private void linkDiffNode() {

		// create relations
		Relationship relA = documentNodeA.createRelationshipTo(diffNode, Relation.DiffRelTypes.HAS_DIFF);
		Relationship relB = diffNode.createRelationshipTo(documentNodeB, Relation.DiffRelTypes.HAS_DIFF);

		// set properties for relation
		relA.setProperty(Property.DiffNode.DIFF_PART, "A");
		relB.setProperty(Property.DiffNode.DIFF_PART, "B");

	}

	private Diff generateDiff() throws ModelTypeException, ModelAccessException, DiffProcessingException {

		Diff diff = null;
		String typeA = DiffUtils.getModelType(modelNodeA);
		String typeB = DiffUtils.getModelType(modelNodeB);

		if( typeA == null || typeB == null || typeA.equals(typeB) == false )
			throw new ModelTypeException( MessageFormat.format("Cannot compare a model of type {0}, with a model of type {1}", typeA, typeB) );

		// get model URLs
		URL modelUrlA = DiffUtils.getUrlFromDocument( documentNodeA );
		URL modelUrlB = DiffUtils.getUrlFromDocument( documentNodeB );
		
		// null check
		if( modelUrlA == null || modelUrlB == null )
			throw new ModelAccessException("At least one of the URLs is null");

		if( typeA.equals(Property.ModelType.CELLML) ) {
			// parse models
			modelA = parseCellMlDocument(modelUrlA);
			modelB = parseCellMlDocument(modelUrlB);

			// compare CellML models
			diff = new CellMLDiff( (CellMLDocument) modelA, (CellMLDocument) modelB );
		} else if( typeA.equals(Property.ModelType.SBML) ) {
			// parse models
			modelA = parseSbmlDocument(modelUrlA);
			modelB = parseSbmlDocument(modelUrlB);

			// compare sbml models
			diff = new SBMLDiff((SBMLDocument) modelA, (SBMLDocument) modelB );
		}

		if( diff == null )
			throw new DiffProcessingException( MessageFormat.format("Cannot generate diff for {0} and {1}!", modelUrlA, modelUrlB) );

		// generating the actual diff
		try {
			diff.mapTrees();
		} catch (Exception e) {
			throw new DiffProcessingException( MessageFormat.format("Exception while generating diff for {0} and {1}!", modelUrlA, modelUrlB), e);
		}

		this.diff = diff;
		return diff;
	}

	private void insertDiff() {

		// get all parts with their xml-id from the models (to speed things up)
		log.debug("building part list");
		Map<String, Node> partListA = DiffUtils.traverseModelParts(modelNodeA);
		Map<String, Node> partListB = DiffUtils.traverseModelParts(modelNodeB);
		
		log.info("part list size A:{}, B:{}", partListA.size(), partListB.size());
		
		// add same stats
		diffNode.setProperty( Property.DiffNode.NUM_DELETES, diff.getPatch().getNumDeletes() );
		diffNode.setProperty( Property.DiffNode.NUM_INSERTS, diff.getPatch().getNumInserts() );
		diffNode.setProperty( Property.DiffNode.NUM_UPDATES, diff.getPatch().getNumUpdates() );
		diffNode.setProperty( Property.DiffNode.NUM_MOVES, diff.getPatch().getNumMoves() );
		diffNode.setProperty( Property.DiffNode.NUM_NODE_CHANGES, diff.getPatch().getNumNodeChanges() );
		diffNode.setProperty( Property.DiffNode.NUM_TEXT_CHANGES, diff.getPatch().getNumTextChanges() );
		
		if( log.isInfoEnabled() )
			log.info( "bives diff stats: DELETES:{}, INSERTS:{}, UPDATES:{}, MOVES:{}, NODE_CHANGES:{}, TEXT_CHANGES:{}",
						diff.getPatch().getNumDeletes(), diff.getPatch().getNumInserts(), diff.getPatch().getNumUpdates(),
						diff.getPatch().getNumMoves(), diff.getPatch().getNumNodeChanges(), diff.getPatch().getNumTextChanges() );
		
		int numDeletes = 0, numInserts = 0, numUpdates = 0, numMoves = 0;
		
		// iterate over deletes
		for( Element entry : diff.getPatch().getDeletes().getChildren() ) {
			if( addPatch(PatchType.DELETE, entry, partListA, partListB) )
				numDeletes++;
		}

		// iterate over inserts
		for( Element entry : diff.getPatch().getInserts().getChildren() ) {
			if( addPatch(PatchType.INSERT, entry, partListA, partListB) )
				numInserts++;
		}
		
		// iterate over updates
		for( Element entry : diff.getPatch().getUpdates().getChildren() ) {
			if( addPatch(PatchType.UPDATE, entry, partListA, partListB) )
				numUpdates++;
		}

		// iterate over moves
		for( Element entry : diff.getPatch().getMoves().getChildren() ) {
			if( addPatch(PatchType.MOVE, entry, partListA, partListB) )
				numMoves++;
		}
		
		log.debug("diff node stat:   DELETES:{}, INSERTS:{}, UPDATES:{}, MOVES:{}", numDeletes, numInserts, numUpdates, numMoves);

	}
	
	protected boolean addPatch( PatchType type, Element entry, Map<String, Node> partListA, Map<String, Node> partListB ) {
		
		DocumentNode oldXmlNode = null;
		DocumentNode newXmlNode = null;
		
		XmlId oldId = null;
		XmlId newId = null;
		
		String oldXPath = entry.getAttributeValue("oldPath");
		if( oldXPath != null ) {
			oldXmlNode = getDocumentNodeFromXPath(modelA, oldXPath);
			oldId = getIdFromXmlNode( oldXmlNode, partListA );
		}
		
		String newXPath = entry.getAttributeValue("newPath");
		if( newXPath != null ) {
			newXmlNode = getDocumentNodeFromXPath(modelB, newXPath);
			newId = getIdFromXmlNode( newXmlNode, partListB );
		}
		
		if( (partListA.containsKey(oldId.getId()) || oldXPath == null) && (partListB.containsKey(oldId.getId()) || newXPath == null) ) {
			Node patchNode = addPatchNode( type,
					oldXPath != null ? partListA.get(oldId.getId()) : null,
					newXPath != null ? partListB.get(newId.getId()) : null,
					oldId, newId, entry
				);
			return patchNode != null;
		}
		else {
			log.warn("Didn't found corresponding {} graph node with id A:{} and B:{} for A:{} and B:{}", type, newId, oldId, oldXPath, newXPath);
			return false;
		}
		
	}

	protected Node addPatchNode( PatchType type, Node oldNode, Node newNode, XmlId oldId, XmlId newId, Element entry ) {
		
		// label definition
		Label nodeLabel = null;
		RelationshipType relationTypeA = null;
		RelationshipType relationTypeB = null;
		
		// distinguish node and label names
		switch (type) {
			case DELETE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_DELETE;
				relationTypeA = Relation.DiffRelTypes.DIFF_DELETED;
				break;
				
			case INSERT:
				nodeLabel = NodeLabel.DiffTypes.DIFF_INSERT;
				relationTypeB = Relation.DiffRelTypes.DIFF_INSERTED;
				break;
				
			case MOVE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_MOVE;
				relationTypeA = Relation.DiffRelTypes.DIFF_MOVE_OLD;
				relationTypeB = Relation.DiffRelTypes.DIFF_MOVE_NEW;
				break;
				
			case UPDATE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_UPDATE;
				relationTypeA = Relation.DiffRelTypes.DIFF_UPDATE_OLD;
				relationTypeB = Relation.DiffRelTypes.DIFF_UPDATE_NEW;
				break;
				
			default:
				throw new IllegalArgumentException("Unknown patch type was given: " + type.toString());
		}
		
		// create node, add labels and xml attributes
		Node patchNode = graphDB.createNode();
		patchNode.addLabel( nodeLabel );
		patchNode.addLabel( NodeLabel.DiffTypes.DIFF_NODE );
		
		// other xml attributes
		addXmlAttributesToNode(patchNode, entry);
		
		// wire the triggeredBy relation
		addTriggeredByRelation(patchNode, entry);
		
		// wire the node
		diffNode.createRelationshipTo( patchNode, Relation.DiffRelTypes.HAS_DIFF_ENTRY );
		boolean inherit = false;
		
		if( relationTypeA != null && oldNode != null ) {
			Relationship relationA = patchNode.createRelationshipTo( oldNode, relationTypeA );
			
			relationA.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() );
			relationA.setProperty( Property.DiffNode.INHERIT_LEVEL, oldId.getInheritLevel() );
			inherit = oldId.isInherit() ? true : inherit;
		}
		
		if( relationTypeB != null && newNode != null ) {
			Relationship relationB = patchNode.createRelationshipTo( newNode, relationTypeB );
			
			relationB.setProperty( Property.DiffNode.INHERIT, newId.isInherit() );
			relationB.setProperty( Property.DiffNode.INHERIT_LEVEL, newId.getInheritLevel() );
			inherit = newId.isInherit() ? true : inherit;
		}
		
		// inherit attribute stuff
		patchNode.setProperty( Property.DiffNode.INHERIT, inherit );
		
		return patchNode;
	}
	
	// ----------------------------------------
	
	private CellMLDocument parseCellMlDocument( URL document ) throws ModelAccessException {

		CellMLValidator validator = new CellMLValidator();
		if( validator.validate(document) == false )
			throw new ModelAccessException("Model is invalid: ", validator.getError());

		return validator.getDocument();
	}

	private SBMLDocument parseSbmlDocument( URL document ) throws ModelAccessException {

		SBMLValidator validator = new SBMLValidator();
		if( validator.validate(document) == false )
			throw new ModelAccessException("Model is invalid: ", validator.getError());

		return validator.getDocument();
	}
	
	protected DocumentNode getDocumentNodeFromXPath( ModelDocument model, String xPath ) {

		// parse XPath
		TreeNode tree = model.getTreeDocument().getNodeByPath(xPath);
		
		if( tree == null )
			return null;
		
		// check if DocumentNode
		if( tree instanceof DocumentNode )
			return (DocumentNode) tree;
		else {
			return tree.getParent();
		}

	}
	
	protected XmlId getIdFromXmlNode( DocumentNode node, Map<String, Node> partList ) {
		
		// null check
		if( node == null )
			return new XmlId(null, false);
		
		// get either the id or the name (CellML)
		String prefix = null;
		String id = node.getId();
		if( id == null || id.isEmpty() )
			id = node.getAttributeValue("name");
		
		// if node is a (CellML) variable, add prefix
		if( node.getTagName().equals("variable") ) {
			XmlId prefixId = getIdFromXmlNode( node.getParent(), partList );
			prefix = prefixId != null ? prefixId.getId() : null;
		}
		
		// if current node has no id nor name or the id/name is not in the part list -> try the parent
		if( id == null || id.isEmpty() || partList.containsKey(id) == false ) {
			// returns null, if parent is null (e.g. not existing)
			XmlId xmlId = getIdFromXmlNode( node.getParent(), partList );
			if( xmlId != null) {
				xmlId.increaseInheritLevel();
				return xmlId;
			}
			else
				return new XmlId(null, false);
		}
		else
			return new XmlId( prefix != null ? prefix + ":" + id : id, false );
	}

	protected void addXmlAttributesToNode( Node patchNode, Element entry ) {

		for( Attribute attr : entry.getAttributes() ) {
			patchNode.setProperty( Property.DiffNode.XML_ATTR_PREFIX + attr.getName(), attr.getValue() );
		}

	}
	
	protected void addTriggeredByRelation( Node patchNode, Element entry ) {
		
		String triggerdBy = entry.getAttributeValue("triggeredBy");
		if( triggerdBy == null )
			return;
		
		Node targetNode = DiffUtils.getPatchNodeByBivesId(diffNode, triggerdBy);
		if( targetNode == null ) {
			log.warn("Could not find patch node with bives.id={} for triggeredBy relation", triggerdBy);
			return;
		}
		
		patchNode.createRelationshipTo(targetNode, Relation.DiffRelTypes.DIFF_TRIGGERED_BY);
	}

	// ----------------------------------------

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((documentNodeA == null) ? 0 : documentNodeA.hashCode());
		result = prime * result + ((documentNodeB == null) ? 0 : documentNodeB.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DiffJob other = (DiffJob) obj;
		if (documentNodeA == null) {
			if (other.documentNodeA != null)
				return false;
		} else if (!documentNodeA.equals(other.documentNodeA))
			return false;
		if (documentNodeB == null) {
			if (other.documentNodeB != null)
				return false;
		} else if (!documentNodeB.equals(other.documentNodeB))
			return false;
		return true;
	}

}
