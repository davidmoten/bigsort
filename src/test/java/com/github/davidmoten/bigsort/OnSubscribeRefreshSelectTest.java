package com.github.davidmoten.bigsort;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import rx.Observable;

import com.github.davidmoten.rx.operators.OnSubscribeRefreshSelect;

public class OnSubscribeRefreshSelectTest {

	private Comparator<Integer> COMPARATOR = new Comparator<Integer>() {
		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	};

	@Test(timeout = 1000000)
	public void test() {
		@SuppressWarnings("unchecked")
		List<Observable<Integer>> list = Arrays.asList(Observable.just(1, 2),
				Observable.just(3).onBackpressureBuffer());
		Observable<Integer> o = Observable
				.create(new OnSubscribeRefreshSelect<Integer>(list, BigSort
						.minimum(COMPARATOR)));
		List<Integer> values = o.toList().toBlocking().single();
		assertEquals(Arrays.asList(1, 2, 3), values);
	}
}
