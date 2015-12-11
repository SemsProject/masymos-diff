package de.unirostock.sems.masymos.diff;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import de.unirostock.sems.masymos.diff.configuration.Property;
import de.unirostock.sems.masymos.diff.configuration.Relation;

public class DiffExecutor {
	
	private static Logger log = LoggerFactory.getLogger(DiffExecutor.class);
	protected static DiffExecutor instance = null;
	
	public static synchronized DiffExecutor instance() {
		
		if( instance == null )
			instance = new DiffExecutor();
		
		return instance;
	}
	
	public static void setThreadPoolSize(int size) {
		numThreads = size;
	}
	
	public static void setQueryLimit(int limit) {
		numQueryLimit = limit;
	}

	// ----------------------------------------
	
	protected GraphDatabaseService graphDB = null;
	protected Manager manager = null;
	protected ExecutorService executor = null;
	
	protected static int numThreads = 5;
	protected static int numQueryLimit = 100;
	
	private DiffExecutor() {
		
		if( manager == null )
			manager = Manager.instance();
		
		if( graphDB == null )
			graphDB = manager.getDatabase();
		
		buildThreadPoolExecutor();
	}
	
	private void buildThreadPoolExecutor() {
		
		// create executor
		if( executor == null || (executor.isShutdown() && executor.isTerminated()) ) {
			log.info("Created fixed ThreadPoolExecutor with {} threads", numThreads);
			executor = Executors.newFixedThreadPool(numThreads);
		}
		
	}
	
	public Set<DiffJob> getDocumentsWithoutDiff(int limit) {
		return getDocumentsWithoutDiff(limit, null);
	}
	
	public Set<DiffJob> getDocumentsWithoutDiff(int limit, Set<Integer> doneJobs, String typeFilter) {
		Set<DiffJob> resultJobList = new HashSet<DiffJob>();
		
		int discardCount = 0;
		int length = 0;
		Set<DiffJob> jobs = null;
		do {
			// get optimal query length
			length = limit - resultJobList.size();
			if( length > 0 && discardCount > 0 )		// not first iteration, so first couple entries are already done
				length = discardCount + (length * 2);	// length = amount of entries needed = amount of "bad" entries
														// skip the bad one and add the needed amount => *2
			
			// get the jobs
			jobs = getDocumentsWithoutDiff(length, typeFilter);
			// clean already done jobs
			for( DiffJob currentJob : jobs ) {
				if( doneJobs.contains(currentJob.hashCode()) == false )
					resultJobList.add(currentJob);
				else
					discardCount++;
				
				if( resultJobList.size() >= limit )
					break;
			}
		
		} while( resultJobList.size() < limit && jobs.size() == length );
		// do until the limit of jobs is reached, or there are no more new jobs in the database
		
		return resultJobList;
	}
	
	public Set<DiffJob> getDocumentsWithoutDiff(int limit, String typeFilter) {
		Set<DiffJob> jobs = new HashSet<DiffJob>();
		
		log.debug("Get document nodes without diff, limited to {}", limit);
		
		// search for model pairs without a diff
		Map<String, Object> parameter = new HashMap<String, Object>();
		parameter.put("limit", limit);
		
		// do filtering
		String query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:MODEL) and (b)-->(:MODEL)  Return a,b Limit {limit}";
		if( Property.ModelType.CELLML.equals(typeFilter) ) {
			query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:CELLML_MODEL) and (b)-->(:CELLML_MODEL) Return a,b Limit {limit}";
			log.debug("restrict to CELLML models");
		}
		else if( Property.ModelType.SBML.equals(typeFilter) ) {
			query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:SBML_MODEL) and (b)-->(:SBML_MODEL) Return a,b Limit {limit}";
			log.debug("restrict to SBML models");
		}
		
		try ( Transaction tx = manager.createNewTransaction() ) {
			log.debug("start graph db transaction");
			
			Result result = graphDB.execute(query, parameter);
			while( result.hasNext() ) {
				Map<String, Object> row = result.next();
				jobs.add( new DiffJob( (Node) row.get("a"), (Node) row.get("b")) );
			}
			
			// close stuff
			result.close();
			tx.success();
			log.debug("finished transaction with {} document node pairs", jobs.size());
		}
		
		return jobs;
	}
	
	public void execute() {
		execute(0);
	}
	
	public void execute(long doneJobsLimit) {
		
		// keeps all already processed jobs
		Set<Integer> doneJobs = new HashSet<Integer>();
		
		// just for safety (and multiple calls of execute)
		buildThreadPoolExecutor();
		
		while(true) {
			// get a job...
			// (... and a life)
			Set<DiffJob> jobs = getDocumentsWithoutDiff(numQueryLimit, doneJobs, null);
			
			// no jobs -> exit loop
			if( jobs.size() == 0 )
				break;
			// max jobs for a run reached -> exit loop
			else if( doneJobs.size() >= doneJobsLimit && doneJobsLimit > 0 )
				break;
			
			for( DiffJob currentJob : jobs ) {
				// submit to process
				executor.submit(currentJob);
				// add to done jobs
				doneJobs.add( currentJob.hashCode() );
			}
			
			log.debug("Done {} jobs in general, {} this turn.", doneJobs.size(), jobs.size());
		}
		
		// finished submitting jobs
		log.info("Finished Submitting {} jobs. Shutting down Executor", doneJobs.size());
		
		try {
			executor.shutdown();
			while( executor.awaitTermination(10, TimeUnit.SECONDS) == false )
				log.info("Wait for Shutdown of executor");
		} catch (InterruptedException e) {
			log.error("Interruption while waiting for executor termination", e);
		}
	}
	
	public void removeAllDiffs() {
		
		int elementCount = 0;
		
		log.info("Start removing all existing diffs from database");
		
		// multiple iterations, to prevent heap overflow
		do {
			elementCount = 0;
			
			try ( Transaction tx = manager.createNewTransaction() ) {
				
				// using a set, to avoid deleting an already deleted node
				Set<Node> deleteSet = new HashSet<Node>();

				log.debug("Gather nodes (Limited to 500)");
				// first gather all diff related nodes
				Result result = graphDB.execute("MATCH d WHERE d:DIFF OR d:DIFF_NODE RETURN d Limit 500");
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
		
	}
	
	public void removeAllDiffsCypher() {
		
		final String diffNodeQuery = "MATCH (d:DIFF)-[r1:HAS_DIFF_ENTRY]->(n:DIFF_NODE)-[r2]->(mn) WITH r1,r2,n LIMIT 1000 DELETE r1,r2,n";
		final String diffQuery = "MATCH (:DOCUMENT)-[r1:HAS_DIFF]->(d:DIFF)-[r2:HAS_DIFF]->(:DOCUMENT) WITH r1,r2,d LIMIT 1000 DELETE r1,r2,d";
		
		try ( Transaction tx = manager.createNewTransaction() ) {
			log.info("Removing old nodes via cypher");
			
			// first delete all diff sub-nodes
			while(true) {
				Result result = graphDB.execute(diffNodeQuery);
				// stop, if the query had no effect
				if( result.getQueryStatistics().getNodesDeleted() == 0 )
					break;
			}
			log.info("done removing sub-nodes");
			
			// then delete all diff nodes and relations between Documents
			while(true) {
				Result result = graphDB.execute(diffQuery);
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
	}
	
}
