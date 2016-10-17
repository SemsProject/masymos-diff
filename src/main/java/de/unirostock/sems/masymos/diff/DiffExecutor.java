package de.unirostock.sems.masymos.diff;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.unirostock.sems.masymos.database.Manager;
import de.unirostock.sems.masymos.diff.thread.PriorityExecutor;

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
	protected PriorityExecutor executor = null;

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
			executor = new PriorityExecutor(1, numThreads, 20, TimeUnit.SECONDS);
		}

	}

	public PriorityExecutor getExecutor() {
		return executor;
	}
	
	/**
	 * Submits a DiffSubmitJobs, which in turn submits the actual 
	 * DiffJobs.
	 * 
	 * @param wait if set to true, this method will wait, until the DiffSubmitJob is finished
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void generateDiffs(boolean wait) throws InterruptedException, ExecutionException {
		generateDiffs(0, wait);
	}
	
	/**
	 * Submits a DiffSubmitJobs, which in turn submits the actual 
	 * DiffJobs. 
	 * 
	 * @param doneJobsLimit Limit of jobs to submit. Set to 0 for unlimited
	 * @param wait if set to true, this method will wait, until the DiffSubmitJob is finished
	 * @throws InterruptedException, ExecutionException 
	 */
	public void generateDiffs(long doneJobsLimit, boolean wait) throws InterruptedException, ExecutionException {

		// just to make sure, the executor is loaded
		buildThreadPoolExecutor();

		// create Job and submit it
		DiffSubmitJob submitJob = new DiffSubmitJob(this.executor, doneJobsLimit, numQueryLimit);
		Future<Long> submitJobResult = this.executor.submit(submitJob);

		if( wait == true ) {
			// Waiting for Future to finish
			try {
				Long count = submitJobResult.get();
				log.info("Successfully submitted {} Jobs", count);
			} catch (InterruptedException | ExecutionException e) {
				log.error("Error while submitting DiffJobs", e);
				throw e;
			}
		}
	}
	
	/**
	 * Submits a DiffCleanJob, which removes all Diff related nodes from the database
	 * s
	 * @param removalMethod
	 * @param wait if set to true, this method will wait, until the DiffCleanJob is finished
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void cleanDiffs(DiffCleanJob.RemovalMethod removalMethod, boolean wait) throws InterruptedException, ExecutionException {
		
		// just to make sure, the executor is loaded
		buildThreadPoolExecutor();
		
		// create the clean job and submit it
		DiffCleanJob cleanJob = new DiffCleanJob(removalMethod);
		Future<Long> cleanJobResult = this.executor.submit(cleanJob);
		
		if( wait == true ) {
			// Wait until future is in the present
			try {
				Long count = cleanJobResult.get();
				log.info("Successfully removed {} nodes", count);
			} catch (InterruptedException | ExecutionException e) {
				log.error("Error while cleaning diffs", e);
				throw e;
			}
		}
	}

	/**
	 * initiates termination process for the underlying ThreadPoolExecutor
	 * and waits until all remaining task are done
	 */
	public void terminate() {
		log.info("initiate execution shutdown");
		this.executor.shutdown();

		while( true ) {
			try {
				if( this.executor.awaitTermination(5, TimeUnit.SECONDS) == true ) {
					log.info("Executor terminated");
					break;
				}
				else 
					log.info("Awaiting termination of executor...");
			} catch (InterruptedException e) {
				log.warn("InterruptException while awaiting executor termination", e);
			}
		}
	}

}
