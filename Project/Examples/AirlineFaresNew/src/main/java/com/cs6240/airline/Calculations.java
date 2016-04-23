package com.cs6240.airline;

import java.util.Collections;
import java.util.List;

/*
 * This class consists of all the calculations invloved in the program
 * It includes mean, median and fast median methods.
 *  
 * @author: Shakti Patro , Kavyashree nagendrakumar
 */

public class Calculations {

	/*
	 * This method calculates mean for a airline for a month 
	 * Returns output in a map : (month airline, mean)
	 */
	public static Double calculateMean(List<Double> priceList){
		Double totalPrice = 0.0;
		for (Double price : priceList) {
			totalPrice += price;
		}
		Double avgPrice = totalPrice / priceList.size();
		return avgPrice;
	}
	
	/*
	 * This method calculates median for a airline for a month 
	 * Returns output in a map : (airline month , median)
	 */
	public static Double calculateMedian(List<Double> priceList){
		Double medianPrice = 0.0;
		Collections.sort(priceList);
		int middle = priceList.size()/2;
		if (priceList.size() % 2 == 1) {
			medianPrice = priceList.get(middle);
		} else {
			medianPrice = (priceList.get(middle - 1) + priceList.get(middle))/2;
		}
		return medianPrice;
	}
	
    //Calculate fast median
	/**
	 * Ref: http://blog.teamleadnet.com/2012/07/quick-select-algorithm-find-kth-element.html 
	 * The code has been reused from this site.
	 * 
	 * @param arr
	 * @param k
	 * @return
	 */
	public static double calculateFastMedian(List<Double> values) {
		int k=values.size()/2;
		if (values == null || values.size() <= k)
			throw new Error();
		int from = 0, to = values.size() - 1;
		// if from == to we reached the kth element
		while (from < to) {
			int r = from, w = to;
			double mid = values.get((r + w) / 2);
			// stop if the reader and writer meets
			while (r < w) {
				if (values.get(r) >= mid) { // put the large values at the end
					double tmp = values.get(w);
					values.set(w, values.get(r));
					values.set(r, tmp);
					w--;
				} else { // the value is smaller than the pivot, skip
					r++;
				}
			}
			// if we stepped up (r++) we need to step one down
			if (values.get(r) > mid)
				r--;
			// the r pointer is on the end of the first k elements
			if (k <= r) {
				to = r;
			} else {
				from = r + 1;
			}
		}
		return values.get(k);
	}
}
