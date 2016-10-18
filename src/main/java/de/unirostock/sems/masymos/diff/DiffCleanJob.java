package de.unirostock.sems.masymos.diff;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.neo4j.cypher.CypherException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.configuration.NodeLabel;
import de.unirostock.sems.masymos.diff.configuration.Relation;
import de.unirostock.sems.masymos.diff.thread.Priority;

public class DiffCleanJob implements Callable<Long>, Priority {

	public static final int PRIORITY = 20;
	private static Logger log = LoggerFactory.getLogger(DiffExecutor.class);

	protected static Manager manager = Manager.instance();
	protected static GraphDatabaseService graphDB = Manager.instance().getDatabase();
	private final RemovalMethod removalMethod;

	public static enum RemovalMethod {
		CYPHER,
		TRAVERSAL;
	}

	public DiffCleanJob(RemovalMethod removalMethod) {
		this.removalMethod = removalMethod;
	}

	@Override
	public int getPriority() {
		return PRIORITY;
	}

	@Override
	public Long call() throws Exception {
		if( this.removalMethod == RemovalMethod.CYPHER )
			return this.removeAllDiffsCypher();
		else
			return this.removeAllDiffs();
	}

	public long removeAllDiffs() {

		int elementCount = 0;
		long sum = 0;

		log.info("Start removing all existing diffs from database");

		// multiple iterations, to prevent heap overflow
		do {
			sum += elementCount;
			elementCount = 0;

			try ( Transaction tx = graphDB.beginTx() ) {

				// using a set, to avoid deleting an already deleted node
				Set<Node> deleteSet = new HashSet<Node>();

				log.debug("Gather nodes (Limited to 500)");
				// first gather all diff related nodes
				Result result = graphDB.execute("MATCH (d) WHERE (d:DIFF) OR (d:DIFF_NODE) RETURN d Limit 500");
				while( result.hasNext() ) {
					Map<String, Object> row = result.next();
					Node diff = (Node) row.get("d");

					if( diff != null ) {
						deleteSet.add(diff);

						if( diff.hasLabel(NodeLabel.DiffTypes.DIFF) ) {
							for( Relationship relation : diff.getRelationships( Relation.DiffRelTypes.HAS_DIFF_ENTRY ) ) {
								if( relation.getEndNode().hasLabel(NodeLabel.DiffTypes.DIFF_NODE) )
									deleteSet.add( relation.getEndNode() );
							}
						}
					}
				}
				result.close();

				log.debug("Start deleting {} nodes with relations", deleteSet.size());
				// do the actual delete
				for( Node node : deleteSet ) {
					for( Relationship relation : node.getRelationships() )
						relation.delete();
					node.delete();
					elementCount++;
				}

				tx.success();
				if( log.isDebugEnabled() )
					log.debug("Removed {} nodes", elementCount);
			}
		} while( elementCount > 0 );

		log.info("done removing existing diff nodes");
		return sum;
	}

	public long removeAllDiffsCypher() {

		final String diffNodeQuery = "MATCH (d:DIFF)-[r1:HAS_DIFF_ENTRY]->(n:DIFF_NODE)-[r2]->(mn) WITH r1,r2,n LIMIT 1000 DELETE r1,r2,n";
		final String diffQuery = "MATCH (:DOCUMENT)-[r1:HAS_DIFF]->(d:DIFF)-[r2:HAS_DIFF]->(:DOCUMENT) WITH r1,r2,d LIMIT 1000 DELETE r1,r2,d";
		long sum = 0;

		try ( Transaction tx = graphDB.beginTx() ) {
			log.info("Removing old nodes via cypher");

			// first delete all diff sub-nodes
			while(true) {
				Result result = graphDB.execute(diffNodeQuery);
				sum += result.getQueryStatistics().getNodesDeleted();
				// stop, if the query had no effect
				if( result.getQueryStatistics().getNodesDeleted() == 0 )
					break;
			}
			log.info("done removing sub-nodes");

			// then delete all diff nodes and relations between Documents
			while(true) {
				Result result = graphDB.execute(diffQuery);
				sum += result.getQueryStatistics().getNodesDeleted();
				// stop, if the query had no effect
				if( result.getQueryStatistics().getNodesDeleted() == 0 )
					break;
			}
			log.info("done removing diff nodes");

			tx.success();
		}
		catch (CypherException e) {
			log.error("Error while removing old diff entries. Using alternate method", e);
			removeAllDiffs();
		}
		
		return sum;
	}

}
