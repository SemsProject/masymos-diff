package de.unirostock.sems.masymos.diff.configuration;

public class Property extends de.unirostock.sems.masymos.configuration.Property {
	
	public class DiffNode {
		/** time point of generation for the diff */
		public static final String GENERATED = "generated";
		/** version of the routine generating this diff. Not forced to align with BiVeS version, but should increase, when BiVeS gets updated */
		public static final String GENERATOR_VERSION = "generatorVersion";
		/** determines which side of the diff, the model was fed in. Either A or B */
		public static final String DIFF_PART = "diffPart";
		/** is set to true, if the annotated element is not directly affected by a change, but one of its children */
		public static final String INHERIT = "inherit";
		/** levels above the actual element, from where the id was derived */
		public static final String INHERIT_LEVEL = "inheritLevel";
		/** prefix for all xml attributes from bives diff */
		public static final String XML_ATTR_PREFIX = "bives.";
		
		public static final String NUM_INSERTS	= "numInserts";
		public static final String NUM_DELETES	= "numDeletes";
		public static final String NUM_UPDATES	= "numUpdates";
		public static final String NUM_MOVES	= "numMoves";
		public static final String NUM_NODE_CHANGES = "numNodeChanges";
		public static final String NUM_TEXT_CHANGES = "numTextChanges";
	}
}
