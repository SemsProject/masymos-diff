package de.unirostock.sems.masymos.diff;

import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;

import org.jdom2.Attribute;
import org.jdom2.Element;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
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
import de.unirostock.sems.masymos.diff.configuration.NodeLabel;
import de.unirostock.sems.masymos.diff.configuration.Property;
import de.unirostock.sems.masymos.diff.configuration.Relation;
import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.traverse.DBModelTraverser;
import de.unirostock.sems.masymos.diff.data.XmlId;
import de.unirostock.sems.masymos.diff.exception.DiffException;
import de.unirostock.sems.masymos.diff.exception.DiffProcessingException;
import de.unirostock.sems.masymos.diff.exception.ModelAccessException;
import de.unirostock.sems.masymos.diff.exception.ModelTypeException;
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
	public static final int GENERATOR_VERSION = 1;
	
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
			
			// traverse to Document nodes
			documentNodeA = DBModelTraverser.getDocumentFromModel(documentNodeA);
			documentNodeB = DBModelTraverser.getDocumentFromModel(documentNodeB);
			
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
			String oldXPath = entry.getAttributeValue("oldPath");
			DocumentNode oldXmlNode = getDocumentNodeFromXPath(modelA, oldXPath);

			XmlId oldId = getIdFromXmlNode( oldXmlNode, partListA );

			if( partListA.containsKey(oldId.getId()) ) {
				addDeleteNode( partListA.get(oldId.getId()), oldId, entry );
				numDeletes++;
			}
			else
				log.warn("Didn't found corresponding graph node with id {} for {} in model A", oldId, oldXPath);
		}

		// iterate over inserts
		for( Element entry : diff.getPatch().getInserts().getChildren() ) {
			String newXPath = entry.getAttributeValue("newPath");
			DocumentNode newXmlNode = getDocumentNodeFromXPath(modelB, newXPath);

			XmlId newId = getIdFromXmlNode( newXmlNode, partListB );

			if( partListB.containsKey(newId.getId()) ) {
				addInsertNode( partListB.get(newId.getId()), newId, entry );
				numInserts++;
			}
			else
				log.warn("Didn't found corresponding graph node with id {} for {} in model B", newId, newXPath);

		}
		
		// iterate over updates
		for( Element entry : diff.getPatch().getUpdates().getChildren() ) {
			String oldXPath = entry.getAttributeValue("oldPath");
			String newXPath = entry.getAttributeValue("newPath");

			DocumentNode oldXmlNode = getDocumentNodeFromXPath(modelA, oldXPath);
			DocumentNode newXmlNode = getDocumentNodeFromXPath(modelB, newXPath);

			XmlId oldId = getIdFromXmlNode( oldXmlNode, partListA );
			XmlId newId = getIdFromXmlNode( newXmlNode, partListB );

			if( partListA.containsKey(oldId.getId()) && partListB.containsKey(newId.getId()) ) {
				addUpdateNode( partListA.get(oldId.getId()), partListB.get(newId.getId()), oldId, newId, entry );
				numUpdates++;
			}
			else
				log.warn("Didn't found corresponding graph node with id A:{} and B:{} for A:{} and B:{}", newId, oldId, oldXPath, newXPath);
		}

		// iterate over updates
		for( Element entry : diff.getPatch().getMoves().getChildren() ) {
			String oldXPath = entry.getAttributeValue("oldPath");
			String newXPath = entry.getAttributeValue("newPath");

			DocumentNode oldXmlNode = getDocumentNodeFromXPath(modelA, oldXPath);
			DocumentNode newXmlNode = getDocumentNodeFromXPath(modelB, newXPath);

			XmlId oldId = getIdFromXmlNode( oldXmlNode, partListA );
			XmlId newId = getIdFromXmlNode( newXmlNode, partListB );

			if( partListA.containsKey(oldId.getId()) && partListB.containsKey(newId.getId()) ) {
				addMoveNode( partListA.get(oldId.getId()), partListB.get(newId.getId()), oldId, newId, entry );
				numMoves++;
			}
			else
				log.warn("Didn't found corresponding graph node with id A:{} and B:{} for A:{} and B:{}", newId, oldId, oldXPath, newXPath);
		}
		
		log.debug("diff node stat:   DELETES:{}, INSERTS:{}, UPDATES:{}, MOVES:{}", numDeletes, numInserts, numUpdates, numMoves);

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

	protected Node addDeleteNode( Node oldNode, XmlId oldId, Element entry ) {
		
		// create node, add labels and xml attributes
		Node deleteNode = graphDB.createNode();
		deleteNode.addLabel( NodeLabel.DiffTypes.DIFF_DELETE );
		deleteNode.addLabel( NodeLabel.DiffTypes.DIFF_NODE );
		addXmlAttributesToNode(deleteNode, entry);
		
		// wire the node
		Relationship relationA = deleteNode.createRelationshipTo( oldNode, Relation.DiffRelTypes.DIFF_DELETED );
		diffNode.createRelationshipTo( deleteNode, Relation.DiffRelTypes.HAS_DIFF_ENTRY );
		
		// inherit attribute stuff
		deleteNode.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() );
		
		relationA.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() );
		relationA.setProperty( Property.DiffNode.INHERIT_LEVEL, oldId.getInheritLevel() );
		
		return deleteNode;
	}

	protected Node addInsertNode( Node newNode, XmlId newId, Element entry ) {

		// create node, add labels and xml attributes
		Node insertNode = graphDB.createNode();
		insertNode.addLabel( NodeLabel.DiffTypes.DIFF_INSERT );
		insertNode.addLabel( NodeLabel.DiffTypes.DIFF_NODE );
		addXmlAttributesToNode(insertNode, entry);
		
		// wire the node
		Relationship relationB = insertNode.createRelationshipTo( newNode, Relation.DiffRelTypes.DIFF_INSERTED );
		diffNode.createRelationshipTo( insertNode, Relation.DiffRelTypes.HAS_DIFF_ENTRY );

		// inherit attribute stuff
		insertNode.setProperty( Property.DiffNode.INHERIT, newId.isInherit() );
		
		relationB.setProperty( Property.DiffNode.INHERIT, newId.isInherit() );
		relationB.setProperty( Property.DiffNode.INHERIT_LEVEL, newId.getInheritLevel() );
		
		return insertNode;
	}

	protected Node addUpdateNode( Node oldNode, Node newNode, XmlId oldId, XmlId newId, Element entry ) {

		// create node, add labels and xml attributes
		Node updateNode = graphDB.createNode();
		updateNode.addLabel( NodeLabel.DiffTypes.DIFF_UPDATE );
		updateNode.addLabel( NodeLabel.DiffTypes.DIFF_NODE );
		addXmlAttributesToNode(updateNode, entry);
		
		// wire the node
		Relationship relationA = updateNode.createRelationshipTo( oldNode, Relation.DiffRelTypes.DIFF_UPDATE_OLD );
		Relationship relationB = updateNode.createRelationshipTo( newNode, Relation.DiffRelTypes.DIFF_UPDATE_NEW );
		diffNode.createRelationshipTo( updateNode, Relation.DiffRelTypes.HAS_DIFF_ENTRY );
		
		// inherit attribute stuff
		updateNode.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() || newId.isInherit() );
		
		relationA.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() );
		relationA.setProperty( Property.DiffNode.INHERIT_LEVEL, oldId.getInheritLevel() );
		
		relationB.setProperty( Property.DiffNode.INHERIT, newId.isInherit() );
		relationB.setProperty( Property.DiffNode.INHERIT_LEVEL, newId.getInheritLevel() );

		return updateNode;
	}

	protected Node addMoveNode( Node oldNode, Node newNode, XmlId oldId, XmlId newId, Element entry ) {
		
		// create node, add labels and xml attributes
		Node moveNode = graphDB.createNode();
		moveNode.addLabel( NodeLabel.DiffTypes.DIFF_MOVE );
		moveNode.addLabel( NodeLabel.DiffTypes.DIFF_NODE );
		addXmlAttributesToNode(moveNode, entry);
		
		// wire the node
		Relationship relationA = moveNode.createRelationshipTo( oldNode, Relation.DiffRelTypes.DIFF_MOVE_OLD );
		Relationship relationB = moveNode.createRelationshipTo( newNode, Relation.DiffRelTypes.DIFF_MOVE_NEW );
		diffNode.createRelationshipTo( moveNode, Relation.DiffRelTypes.HAS_DIFF_ENTRY );
		
		// inherit attribute stuff
		moveNode.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() || newId.isInherit() );
		
		relationA.setProperty( Property.DiffNode.INHERIT, oldId.isInherit() );
		relationA.setProperty( Property.DiffNode.INHERIT_LEVEL, oldId.getInheritLevel() );
		
		relationB.setProperty( Property.DiffNode.INHERIT, newId.isInherit() );
		relationB.setProperty( Property.DiffNode.INHERIT_LEVEL, newId.getInheritLevel() );

		
		return moveNode;
	}

	protected void addXmlAttributesToNode( Node node, Element entry ) {

		for( Attribute attr : entry.getAttributes() ) {
			node.setProperty( Property.DiffNode.XML_ATTR_PREFIX + attr.getName(), attr.getValue() );
		}

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
