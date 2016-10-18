package de.unirostock.sems.masymos.diff;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.configuration.Property;
import de.unirostock.sems.masymos.diff.thread.Priority;

public class DiffSubmitJob implements Callable<Long>, Priority {

	public final static int PRIORITY = 10;
	
	private static Logger log = LoggerFactory.getLogger(DiffSubmitJob.class);
	protected static Manager manager = Manager.instance();
	protected static GraphDatabaseService graphDB = Manager.instance().getDatabase();
	
	
	protected long doneJobsLimit = 100;
	protected long queryLimit = 500;
	protected final ExecutorService executor;
	
	public DiffSubmitJob(ExecutorService executor, long doneJobsLimit, long queryLimit) {
		this.doneJobsLimit = doneJobsLimit;
		this.queryLimit = queryLimit;
		this.executor = executor;
	}
	
	@Override
	public int getPriority() {
		return PRIORITY;
	}

	@Override
	public Long call() throws Exception {
	
		// let's submit some jobs!
		// keeps all already processed jobs
		Set<Integer> doneJobs = new HashSet<Integer>();
		
		while(true) {
			// get a job...
			// (... and a life)
			Set<DiffJob> jobs = getDocumentsWithoutDiff(queryLimit, doneJobs, null);
			
			// no jobs -> exit loop
			if( jobs.size() == 0 ) {
				log.info("No jobs left, stop submitting");
				break;
			}
			// max jobs for a run reached -> exit loop
			else if( doneJobs.size() >= doneJobsLimit && doneJobsLimit > 0 ) {
				log.info("Reached job submission limit, stopping for now");
				break;
			}
			
			for( DiffJob currentJob : jobs ) {
				// submit to process
				executor.submit(currentJob);
				// add to done jobs
				doneJobs.add( currentJob.hashCode() );
			}
			
			log.debug("Done {} jobs in general, {} this turn.", doneJobs.size(), jobs.size());
		}
		// finished submitting jobs
		log.info("Finished Submitting {} jobs.", doneJobs.size());
		return (long) doneJobs.size();
	}
	
	
	protected Set<DiffJob> getDocumentsWithoutDiff(long limit) {
		return getDocumentsWithoutDiff(limit, null);
	}
	
	protected Set<DiffJob> getDocumentsWithoutDiff(long limit, Set<Integer> doneJobs, String typeFilter) {
		Set<DiffJob> resultJobList = new HashSet<DiffJob>();
		
		long discardCount = 0;
		long length = 0;
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
	
	protected Set<DiffJob> getDocumentsWithoutDiff(long limit, String typeFilter) {
		Set<DiffJob> jobs = new HashSet<DiffJob>();
		
		log.debug("Get document nodes without diff, limited to {}", limit);
		
		// search for model pairs without a diff
		Map<String, Object> parameter = new HashMap<String, Object>();
		parameter.put("limit", limit);
		
		// do filtering
		String query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:MODEL) and (b)-->(:MODEL) Return a,b Limit {limit}";
		if( Property.ModelType.CELLML.equals(typeFilter) ) {
			query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:CELLML_MODEL) and (b)-->(:CELLML_MODEL) Return a,b Limit {limit}";
			log.debug("restrict to CELLML models");
		}
		else if( Property.ModelType.SBML.equals(typeFilter) ) {
			query = "Match (a:DOCUMENT)-[:HAS_SUCCESSOR]->(b:DOCUMENT) Where NOT (a)-->(:DIFF)-->(b) and (a)-->(:SBML_MODEL) and (b)-->(:SBML_MODEL) Return a,b Limit {limit}";
			log.debug("restrict to SBML models");
		}
		
		try ( Transaction tx = graphDB.beginTx() ) {
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

	
}
