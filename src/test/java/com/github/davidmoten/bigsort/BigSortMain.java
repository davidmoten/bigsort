package com.github.davidmoten.bigsort;

import rx.Observable;
import rx.schedulers.Schedulers;

public class BigSortMain {

	public static void main(String[] args) {
		// for profiling
		int n = 1000000000;
		System.out.println(n * Math.log(n));
		long t = System.currentTimeMillis();

		BigSort.sort(Observable.range(1, n).map(i -> n - i + 1), 100000, 10,
				Schedulers.computation()).subscribe();
		System.out.println(((System.currentTimeMillis() - t) / 1000.0) + "s");
	}
}
