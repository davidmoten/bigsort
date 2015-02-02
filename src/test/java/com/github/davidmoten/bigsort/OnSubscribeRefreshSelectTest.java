package com.github.davidmoten.bigsort;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static rx.Observable.just;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import rx.Observable;

import com.github.davidmoten.rx.operators.OnSubscribeRefreshSelect;

public class OnSubscribeRefreshSelectTest {

	private static final Comparator<Integer> COMPARATOR = new Comparator<Integer>() {
		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	};

	@Test(timeout = 10000)
	public void test1() {
		checkEquals(asList(1, 2, 3), just(1, 2), just(3).onBackpressureBuffer());
	}

	@Test(timeout = 10000)
	public void test2() {
		checkEquals(asList(1, 2, 3, 3, 4, 4, 5, 6, 6, 7), just(1, 2, 5, 6),
				just(3, 4, 4, 7), just(3, 6));
	}

	@Test(timeout = 10000)
	public void test3() {
		checkEquals(asList(1), Observable.<Integer> empty(), just(1)
				.onBackpressureBlock());
	}

	@Test(timeout = 10000)
	public void test4() {
		checkEquals(asList(1, 2), Observable.<Integer> empty(), just(1, 2));
	}

	private static void checkEquals(List<Integer> expected,
			Observable<Integer>... input) {
		Observable<Integer> o = Observable
				.create(new OnSubscribeRefreshSelect<Integer>(Arrays
						.asList(input), BigSort.minimum(COMPARATOR)));
		List<Integer> values = o.toList().toBlocking().single();
		assertEquals(expected, values);

	}

}
