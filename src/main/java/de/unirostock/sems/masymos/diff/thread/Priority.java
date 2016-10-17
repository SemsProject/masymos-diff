package de.unirostock.sems.masymos.diff.thread;

/**
 * used to indicate priority, to a possible PriorityQueue/-Executor
 * @author martin
 *
 */
public interface Priority {
	
	/**
	 * Returns the priority of this task
	 * @return priority as int. Higher numbers mean higher priority.
	 */
	public int getPriority();
}
