package com.github.davidmoten.bigsort;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

public class BigSortStringTest {

	@Test
	public void testDemonstrateSortLinesFromFiles() {
		Observable<String> source = Observable.just("c", "b", "a");

		int maxToSortInMemoryPerThread = 3;
		int maxTempFiles = 2;
		Scheduler scheduler = Schedulers.computation();

		Observable<String> sorted = BigSort.sort(source,
				maxToSortInMemoryPerThread, maxTempFiles, scheduler);

		List<String> list = sorted
		// get the results as a list
				.toList()
				// block and return result
				.toBlocking().single();

		assertEquals(Arrays.asList("a", "b", "c"), list);
	}
}
