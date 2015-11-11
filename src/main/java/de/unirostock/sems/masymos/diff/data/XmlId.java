package de.unirostock.sems.masymos.diff.data;

import java.io.Serializable;

public class XmlId implements Serializable {
	
	private static final long serialVersionUID = 4915247842597834942L;
	
	/** the actual xml element id */
	protected String id = null;
	/** is set to true, if the id was derived from a parent object */
	protected boolean inherit = false;
	/** Number of levels above the actual element, from where is id was derived */
	protected long inheritLevel = 0;
	
	public XmlId() {}

	public XmlId(String id, boolean inherit) {
		super();
		this.id = id;
		this.inherit = inherit;
	}

	/**
	 * Retuns the XML element id
	 * 
	 * @return
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the actual XML element id
	 * 
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * Returns true, if the id is null
	 * 
	 * @return
	 */
	public boolean isNull() {
		return id == null;
	}
	
	/**
	 * Returns true, if the id null or the length is 0
	 * @return
	 */
	public boolean isEmpty() {
		return id == null || id.isEmpty();
	}

	/**
	 * Returns true, if the id was derived from a parent element
	 * @return
	 */
	public boolean isInherit() {
		return inherit;
	}

	/**
	 * Sets the inherit flag
	 * 
	 * @param inherit
	 */
	public void setInherit(boolean inherit) {
		this.inherit = inherit;
	}

	/**
	 * Retuns the number of levels above the actual element, from where is id was derived
	 * @return
	 */
	public long getInheritLevel() {
		return inheritLevel;
	}

	/**
	 * Sets the number of levels above the actual element, from where is id was derived
	 * @param inheritLevel
	 */
	public void setInheritLevel(long inheritLevel) {
		this.inheritLevel = inheritLevel;
	}
	
	/**
	 * increases the inherit level by 1 and sets inherit flag to true.
	 */
	public void increaseInheritLevel() {
		this.inheritLevel++;
		this.inherit = true;
	}

	@Override
	public int hashCode() {
		if( id != null )
			return id.hashCode();
		else
			return 0;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	@Override
	public String toString() {
		return id;
	}
	
}
