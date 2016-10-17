package de.unirostock.sems.masymos.diff.thread;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PriorityExecutor extends ThreadPoolExecutor {

	public final int minPriority = Integer.MIN_VALUE;
	public final int maxPriority = Integer.MAX_VALUE;
	
	/**
	 * constructs the BlockingQueue
	 * @param initialSize
	 * @return
	 */
	private static BlockingQueue<Runnable> constructQueue(int initialSize) {
		return new PriorityBlockingQueue<>(initialSize, new PriorityExecutor.PriorityComparator());
	}
	
	// constructor
	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, PriorityExecutor.constructQueue(corePoolSize), handler);
	}

	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, PriorityExecutor.constructQueue(corePoolSize), threadFactory, handler);
	}

	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, PriorityExecutor.constructQueue(corePoolSize), threadFactory);
	}

	public PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, PriorityExecutor.constructQueue(corePoolSize));
	}
	
	// overwritten internal methods/classes
	
	protected static class PriorityComparator implements Comparator<Runnable> {

		@Override
		public int compare(final Runnable left, final Runnable right) {
			final int priorityLeft = left instanceof Priority ? ((Priority) left).getPriority() : 0;
			final int priorityRight = right instanceof Priority ? ((Priority) right).getPriority() : 0;
			final long diff = priorityRight - priorityLeft;
			return diff == 0 ? 0 : diff < 0 ? -1 : 1;
		}
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
		if (callable instanceof Priority)
			return new PriorityTask<T>(((Priority) callable).getPriority(), callable);
		else
			return new PriorityTask<T>(0, callable);
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
		if (runnable instanceof Priority)
			return new PriorityTask<T>(((Priority) runnable).getPriority(), runnable, value);
		else
			return new PriorityTask<T>(0, runnable, value);
	}

	private static final class PriorityTask<T> extends FutureTask<T> implements Comparable<PriorityTask<T>> {
		private final int priority;

		public PriorityTask(final int priority, final Callable<T> tCallable) {
			super(tCallable);

			this.priority = priority;
		}

		public PriorityTask(final int priority, final Runnable runnable, final T result) {
			super(runnable, result);

			this.priority = priority;
		}

		@Override
		public int compareTo(final PriorityTask<T> o) {
			final long diff = o.priority - priority;
			return diff == 0 ? 0 : diff < 0 ? -1 : 1;
		}
	}

}
