package org.vanilladb.core.query.algebra;

import org.vanilladb.core.storage.metadata.statistics.Bucket;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * An abstract {@link Plan} that may result in the reduction of number of record
 * and other statistics.
 */
public abstract class ReduceRecordsPlan implements Plan {

	/**
	 * Buckets of a field may be discarded during the cost estimation if its
	 * frequency is less than 1. As a result, the total frequencies of buckets
	 * may be diverse in different fields. This method synchronizes the total
	 * frequencies of different fields in the specified histogram.
	 * 
	 * @param hist
	 *            the histogram
	 * @return a histogram whose total frequencies in different fields are
	 *         synchronized
	 */
	public static Histogram syncHistogram(Histogram hist) {
		double maxRecs = 0.0;
		for (String fld : hist.fields()) {
			double numRecs = 0.0;
			for (Bucket bkt : hist.buckets(fld))
				numRecs += bkt.frequency();
			if (Double.compare(numRecs, maxRecs) > 0)
				maxRecs = numRecs;
		}
		Histogram syncHist = new Histogram(hist.fields());
		for (String fld : hist.fields()) {
			double numRecs = 0.0;
			for (Bucket bkt : hist.buckets(fld))
				numRecs += bkt.frequency();
			double extrapolation = maxRecs / numRecs;
			for (Bucket bkt : hist.buckets(fld))
				syncHist.addBucket(fld, new Bucket(bkt.valueRange(),
						extrapolation * bkt.frequency(), bkt.distinctValues(),
						bkt.valuePercentiles()));
		}
		return syncHist;
	}

}
