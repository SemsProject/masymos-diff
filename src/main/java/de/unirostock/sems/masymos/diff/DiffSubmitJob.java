package de.unirostock.sems.masymos.diff;

import de.unirostock.sems.masymos.diff.thread.Priority;

public class DiffSubmitJob implements Runnable, Priority {

	public final static int PRIORITY = 10;
	
	@Override
	public int getPriority() {
		return PRIORITY;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
