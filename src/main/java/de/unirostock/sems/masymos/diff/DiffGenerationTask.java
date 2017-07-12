package de.unirostock.sems.masymos.diff;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.UniqueFactory.UniqueEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unirostock.sems.bives.api.Diff;
import de.unirostock.sems.bives.cellml.algorithm.CellMLValidator;
import de.unirostock.sems.bives.cellml.api.CellMLDiff;
import de.unirostock.sems.bives.cellml.parser.CellMLDocument;
import de.unirostock.sems.bives.ds.ModelDocument;
import de.unirostock.sems.bives.ds.Patch;
import de.unirostock.sems.bives.sbml.algorithm.SBMLValidator;
import de.unirostock.sems.bives.sbml.api.SBMLDiff;
import de.unirostock.sems.bives.sbml.parser.SBMLDocument;
import de.unirostock.sems.comodi.ChangeFactory;
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
import de.unirostock.sems.masymos.diff.thread.Priority;
import de.unirostock.sems.masymos.diff.traverse.DBModelTraverser;
import de.unirostock.sems.masymos.util.OntologyFactory;
import de.unirostock.sems.xmlutils.ds.DocumentNode;
import de.unirostock.sems.xmlutils.ds.TreeNode;

/**
 * Runnable Job generating a diff between 2 model versions
 * 
 * @author martin
 *
 */
public class DiffGenerationTask implements Runnable, Priority {

	/**
	 * version of the generator routine.
	 * Should be increased, with BiVeS update!
	 */
	public static final int GENERATOR_VERSION = 2;
	public static final String COMODI_ONTOLOGY = "ComodiOntology";
	public static final int PRIORITY = 1;
	
	private static Logger log = LoggerFactory.getLogger(DiffGenerationTask.class);
	protected static GraphDatabaseService graphDB = Manager.instance().getDatabase();
	protected static Manager manager = Manager.instance();

	protected Node documentNodeSource = null;
	protected Node documentNodeDestination = null;

	protected Node modelNodeSource = null;
	protected Node modelNodeDestination = null;

	protected ModelDocument modelSource = null;
	protected ModelDocument modelDestination = null;

	protected Node diffNode = null;
	protected Diff diff = null;
	
	protected Map<String, Node> patchNodeMap = new HashMap<String, Node>();
	
	/**
	 * creates a new Diff Job. no intensive calculation included.
	 * @param nodeA
	 * @param nodeB
	 */
	public DiffGenerationTask(Node nodeA, Node nodeB) {

		if( nodeA == null || nodeB == null )
			throw new IllegalArgumentException("both nodes are not allowed to be null.");
		
		this.documentNodeSource = nodeA;
		this.documentNodeDestination = nodeB;
		
	}
	
	/**
	 * returns the default priority for all DiffJobs (1)
	 */
	@Override
	public int getPriority() {
		return PRIORITY;
	}

	/**
	 * starts the diff calculation. Should run as separate thread, called through 
	 */
	@Override
	public void run() {
		
		try (Transaction tx = graphDB.beginTx()) {
			
			log.debug("Traversing document and model nodes");
			
			log.trace( "Document A labels: {}", documentNodeSource.getLabels() );
			log.trace( "Document B labels: {}", documentNodeDestination.getLabels() );
			
			// traverse to Document nodes
//			documentNodeA = DBModelTraverser.getDocumentFromModel(documentNodeA);
//			documentNodeB = DBModelTraverser.getDocumentFromModel(documentNodeB);
			
			if( documentNodeSource == null || documentNodeDestination == null )
				throw new IllegalArgumentException("One or both document nodes are null");
			
			// traverse to Model nodes (one below document)
			modelNodeSource = DBModelTraverser.getModelFromDocument(documentNodeSource);
			modelNodeDestination = DBModelTraverser.getModelFromDocument(documentNodeDestination);
			
			if( log.isInfoEnabled() )
				log.info( "comparing model {} and model {}", 
						documentNodeSource.getProperty(Property.General.XMLDOC, null),
						documentNodeDestination.getProperty(Property.General.XMLDOC, null) );
			
			log.debug("Creating diff node");
			
			createDiffNode();
			// link the diff, to the models/documents
			linkDiffNode();
			
			log.debug("generating diff");
			generateDiff();

			// do the actual work
			log.debug("inserting diff into graph db");
			insertDiff();
			
			// annotate with rdf
			log.debug("annotating with rdf");
			insertRDF( diff.getPatch().getAnnotations() );
			
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
		Relationship relA = documentNodeSource.createRelationshipTo(diffNode, Relation.DiffRelTypes.HAS_DIFF);
		Relationship relB = documentNodeDestination.createRelationshipTo(diffNode, Relation.DiffRelTypes.HAS_DIFF);

		// set properties for relation
		relA.setProperty(Property.DiffNode.DIFF_PART, "source");
		relB.setProperty(Property.DiffNode.DIFF_PART, "destination");

	}

	private Diff generateDiff() throws ModelTypeException, ModelAccessException, DiffProcessingException {

		Diff diff = null;
		String typeA = DiffUtils.getModelType(modelNodeSource);
		String typeB = DiffUtils.getModelType(modelNodeDestination);

		if( typeA == null || typeB == null || typeA.equals(typeB) == false )
			throw new ModelTypeException( MessageFormat.format("Cannot compare a model of type {0}, with a model of type {1}", typeA, typeB) );

		// get model URLs
		URL modelUrlA = DiffUtils.getUrlFromDocument( documentNodeSource );
		URL modelUrlB = DiffUtils.getUrlFromDocument( documentNodeDestination );
		
		// null check
		if( modelUrlA == null || modelUrlB == null )
			throw new ModelAccessException("At least one of the URLs is null");

		if( typeA.equals(Property.ModelType.CELLML) ) {
			// parse models
			modelSource = parseCellMlDocument(modelUrlA);
			modelDestination = parseCellMlDocument(modelUrlB);

			// compare CellML models
			diff = new CellMLDiff( (CellMLDocument) modelSource, (CellMLDocument) modelDestination );
		} else if( typeA.equals(Property.ModelType.SBML) ) {
			// parse models
			modelSource = parseSbmlDocument(modelUrlA);
			modelDestination = parseSbmlDocument(modelUrlB);

			// compare sbml models
			diff = new SBMLDiff((SBMLDocument) modelSource, (SBMLDocument) modelDestination );
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
		Map<String, Node> partListA = DiffUtils.traverseModelParts(modelNodeSource);
		Map<String, Node> partListB = DiffUtils.traverseModelParts(modelNodeDestination);
		
		log.info("part list size A:{}, B:{}", partListA.size(), partListB.size());
		
		// compute patch
		log.debug("compute patch");
		Patch patch = diff.getPatch();
		
		// add same stats
		diffNode.setProperty( Property.DiffNode.NUM_DELETES, patch.getNumDeletes() );
		diffNode.setProperty( Property.DiffNode.NUM_INSERTS, patch.getNumInserts() );
		diffNode.setProperty( Property.DiffNode.NUM_UPDATES, patch.getNumUpdates() );
		diffNode.setProperty( Property.DiffNode.NUM_MOVES, patch.getNumMoves() );
		diffNode.setProperty( Property.DiffNode.NUM_NODE_CHANGES, patch.getNumNodeChanges() );
		diffNode.setProperty( Property.DiffNode.NUM_TEXT_CHANGES, patch.getNumTextChanges() );
		
		if( log.isInfoEnabled() )
			log.info( "bives diff stats: DELETES:{}, INSERTS:{}, UPDATES:{}, MOVES:{}, NODE_CHANGES:{}, TEXT_CHANGES:{}",
						patch.getNumDeletes(), patch.getNumInserts(), patch.getNumUpdates(),
						patch.getNumMoves(), patch.getNumNodeChanges(), patch.getNumTextChanges() );
		
		int numDeletes = 0, numInserts = 0, numUpdates = 0, numMoves = 0;
		
		// iterate over deletes
		for( Element entry : patch.getDeletes().getChildren() ) {
			if( addPatch(PatchType.DELETE, entry, partListA, partListB) )
				numDeletes++;
		}

		// iterate over inserts
		for( Element entry : patch.getInserts().getChildren() ) {
			if( addPatch(PatchType.INSERT, entry, partListA, partListB) )
				numInserts++;
		}
		
		// iterate over updates
		for( Element entry : patch.getUpdates().getChildren() ) {
			if( addPatch(PatchType.UPDATE, entry, partListA, partListB) )
				numUpdates++;
		}

		// iterate over moves
		for( Element entry : patch.getMoves().getChildren() ) {
			if( addPatch(PatchType.MOVE, entry, partListA, partListB) )
				numMoves++;
		}
		
		log.debug("diff node stat:   DELETES:{}, INSERTS:{}, UPDATES:{}, MOVES:{}", numDeletes, numInserts, numUpdates, numMoves);

	}
	
	protected boolean addPatch( PatchType type, Element entry, Map<String, Node> partListSource, Map<String, Node> partListDestination ) {
		
		DocumentNode sourceXmlNode = null;
		DocumentNode destinationXmlNode = null;
		
		XmlId sourceId = null;
		XmlId destinationId = null;
		
		String sourceXPath = entry.getAttributeValue("oldPath");
		if( sourceXPath != null ) {
			sourceXmlNode = getDocumentNodeFromXPath(modelSource, sourceXPath);
			sourceId = getIdFromXmlNode( sourceXmlNode, partListSource );
		}
		
		String destinationXPath = entry.getAttributeValue("newPath");
		if( destinationXPath != null ) {
			destinationXmlNode = getDocumentNodeFromXPath(modelDestination, destinationXPath);
			destinationId = getIdFromXmlNode( destinationXmlNode, partListDestination );
		}
		
		if(		(sourceXPath == null || partListSource.containsKey(sourceId.getId()) ) &&
				(destinationXPath == null || partListDestination.containsKey(destinationId.getId()) ) &&
				!(sourceXPath == null && destinationXPath == null) ) {
			
			Node patchNode = addPatchNode( type,
					sourceXPath != null ? partListSource.get(sourceId.getId()) : null,
					destinationXPath != null ? partListDestination.get(destinationId.getId()) : null,
					sourceId, destinationId, entry
				);
			return patchNode != null;
		}
		else {
			log.warn("Didn't found corresponding {} graph node with id A:{} and B:{} for A:{} and B:{}", type, destinationId, sourceId, sourceXPath, destinationXPath);
			return false;
		}
		
	}

	protected Node addPatchNode( PatchType type, Node sourceNode, Node destinationNode, XmlId sourceId, XmlId destinationId, Element entry ) {
		
		// label definition
		Label nodeLabel = null;
		RelationshipType sourceRelationType = null;
		RelationshipType destinationRelationType = null;
		
		// distinguish node and label names
		switch (type) {
			case DELETE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_DELETE;
				sourceRelationType = Relation.DiffRelTypes.IS_SOURCE;
				break;
				
			case INSERT:
				nodeLabel = NodeLabel.DiffTypes.DIFF_INSERT;
				destinationRelationType = Relation.DiffRelTypes.IS_DESTINATION;
				break;
				
			case MOVE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_MOVE;
				sourceRelationType = Relation.DiffRelTypes.IS_SOURCE;
				destinationRelationType = Relation.DiffRelTypes.IS_DESTINATION;
				break;
				
			case UPDATE:
				nodeLabel = NodeLabel.DiffTypes.DIFF_UPDATE;
				sourceRelationType = Relation.DiffRelTypes.IS_SOURCE;
				destinationRelationType = Relation.DiffRelTypes.IS_DESTINATION;
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
		patchNode.createRelationshipTo( diffNode, Relation.DatabaseRelTypes.BELONGS_TO );
		boolean inherit = false;
		
		if( sourceRelationType != null && sourceNode != null ) {
			Relationship relationA = patchNode.createRelationshipTo( sourceNode, sourceRelationType );
			
			relationA.setProperty( Property.DiffNode.INHERIT, sourceId.isInherit() );
			relationA.setProperty( Property.DiffNode.INHERIT_LEVEL, sourceId.getInheritLevel() );
			inherit = sourceId.isInherit() ? true : inherit;
		}
		
		if( destinationRelationType != null && destinationNode != null ) {
			Relationship relationB = patchNode.createRelationshipTo( destinationNode, destinationRelationType );
			
			relationB.setProperty( Property.DiffNode.INHERIT, destinationId.isInherit() );
			relationB.setProperty( Property.DiffNode.INHERIT_LEVEL, destinationId.getInheritLevel() );
			inherit = destinationId.isInherit() ? true : inherit;
		}
		
		// inherit attribute stuff
		patchNode.setProperty( Property.DiffNode.INHERIT, inherit );
		
		// add patch node to map
		String bivesId = entry.getAttributeValue("id");
		if( bivesId != null && bivesId.isEmpty() == false )
			patchNodeMap.put( bivesId, patchNode );
		
		return patchNode;
	}
	
	private void insertRDF(ChangeFactory annotations) {
		
		// get RDF annotations from Bives
		Model rdfModel = annotations.getAnnotaions();
		
		ResIterator resIter = rdfModel.listSubjects();
		while( resIter.hasNext() ) {
			Resource resource = resIter.nextResource();
			StmtIterator propIter = resource.listProperties();
			while( propIter.hasNext() ) {
				Statement statement = propIter.nextStatement();
				org.apache.jena.rdf.model.Property prop = statement.getPredicate();
				RDFNode object = statement.getObject();
				
				Resource objectResource = null;
				if( object.isResource() == true && (objectResource = object.asResource()).getNameSpace().equals( ChangeFactory.COMODI_NS ) ) {
					log.debug("S:{} P:{} O:{}", resource.toString(), prop.getLocalName(), objectResource.toString());
					try {
						addComodiTerm( resource.getURI(), prop.getLocalName(), objectResource.getLocalName() );
					}
					catch(Exception e) {
						log.error(MessageFormat.format("Error while annotation DiffNode {0} with COMODI term {1}", resource, objectResource) );
					}
				}
				
			}
		}
		
	}
	
	private Relationship addComodiTerm(String bivesId, String relationType, String object) {
		
		// cleanUp name
		if( bivesId == null || bivesId.isEmpty() )
			return null;
		
		try {
			URI bivesIdUri = new URI(bivesId);
			bivesId = bivesIdUri.getFragment();
		} catch (URISyntaxException e) {
			log.debug("Bives id was no uri. Abort.", e);
			return null;
		}
		
		// get patch node from diff
		Node patchNode = patchNodeMap.get(bivesId); //DiffUtils.getPatchNodeByBivesId(diffNode, bivesId);
		if( patchNode == null ) {
			log.debug("no patch node found for bivesId {}", bivesId);
			return null;
		}
		
		// get ontology node from comodi
		Node comodiNode = null;
		UniqueEntity<Node> comodiNodeEntity = OntologyFactory.getFactory(COMODI_ONTOLOGY).getOrCreateWithOutcome("id", object);
		if( comodiNodeEntity != null )
			comodiNode = comodiNodeEntity.entity();
		else {
			log.warn("Comodi term {} not found in database", object);
			return null;
		}
			
		// wire relation
		Relationship relation = patchNode.createRelationshipTo( comodiNode, RelationshipType.withName(relationType) );
		return relation;
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
		if( node == null ) {
			log.trace("return XmlID:null, because no DocumentNode is also null");
			return new XmlId(null, false);
		}
		
		// get either the id or the name (CellML)
		String prefix = null;
		String id = node.getId();
		if( id == null || id.isEmpty() )
			id = node.getAttributeValue("name");
		
		// if node is a (CellML) variable, add prefix
		if( node.getTagName().equals("variable") ) {
			XmlId prefixId = getIdFromXmlNode( node.getParent(), partList );
			prefix = prefixId != null ? prefixId.getId() : null;
			log.trace("XmlId prefix {}, because CellMl variable", prefix);
		}
		
		// if current node has no id nor name or the id/name is not in the part list -> try the parent
		if( id == null || id.isEmpty() || partList.containsKey(id) == false ) {
			// returns null, if parent is null (e.g. not existing)
			log.trace("No id found, try to traserve upwards in XML");
			XmlId xmlId = getIdFromXmlNode( node.getParent(), partList );
			if( xmlId.isEmpty() == false) {
				log.trace("Found id on parent, increase inherit level");
				xmlId.increaseInheritLevel();
				return xmlId;
			}
			else {
				log.warn("Did not found id on parent, very unusual.");
				return new XmlId(null, false);
			}
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
		result = prime * result + ((documentNodeSource == null) ? 0 : documentNodeSource.hashCode());
		result = prime * result + ((documentNodeDestination == null) ? 0 : documentNodeDestination.hashCode());
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
		DiffGenerationTask other = (DiffGenerationTask) obj;
		if (documentNodeSource == null) {
			if (other.documentNodeSource != null)
				return false;
		} else if (!documentNodeSource.equals(other.documentNodeSource))
			return false;
		if (documentNodeDestination == null) {
			if (other.documentNodeDestination != null)
				return false;
		} else if (!documentNodeDestination.equals(other.documentNodeDestination))
			return false;
		return true;
	}

}
