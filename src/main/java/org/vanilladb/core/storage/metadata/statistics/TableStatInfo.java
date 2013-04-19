package org.vanilladb.core.storage.metadata.statistics;

/**
 * Holds statistical information about a table.
 */
public class TableStatInfo {
	private long numBlks;
	private Histogram hist;

	TableStatInfo(long numBlks, Histogram hist) {
		this.numBlks = numBlks;
		this.hist = hist;
	}

	/**
	 * Returns the estimated number of blocks in the table.
	 * 
	 * @return the estimated number of blocks in the table
	 */
	public long blocksAccessed() {
		return numBlks;
	}

	/**
	 * Returns a histogram that approximates the join distribution of
	 * frequencies of field values in this table.
	 * 
	 * @return the histogram
	 */
	public Histogram histogram() {
		return hist;
	}
}