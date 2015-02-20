package com.github.davidmoten.bigsort;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static rx.Observable.just;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.rx.Functions;
import com.github.davidmoten.rx.operators.OnSubscribeRefreshSelect;

public class OnSubscribeRefreshSelectTest {

	private static final Comparator<Integer> COMPARATOR = new Comparator<Integer>() {
		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	};

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test1() {
		checkEquals(asList(1, 2, 3), just(1, 2), just(3).onBackpressureBuffer());
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test2() {
		checkEquals(asList(1, 2, 3, 3, 4, 4, 5, 6, 6, 7), just(1, 2, 5, 6),
				just(3, 4, 4, 7), just(3, 6));
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test3() {
		checkEquals(Collections.<Integer> emptyList(),
				Observable.<Integer> empty(), Observable.<Integer> empty());
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test4() {
		checkEquals(asList(1, 2), Observable.<Integer> empty(), just(1, 2));
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test5() {
		checkEquals(asList(1, 2), just(1).onBackpressureBuffer(), just(2)
				.onBackpressureBuffer());
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test6() {
		checkEquals(asList(1, 2, 3, 4), just(1, 2, 4), just(3)
				.onBackpressureBuffer());
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test7() {
		checkEquals(asList(1, 2, 4, 5, 7, 8, 9), just(1, 2, 4),
				Observable.<Integer> empty(), just(5, 7), just(8, 9));
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 1000)
	public void test8() {
		checkEquals(asList(1, 2, 4, 5, 7, 8, 9), just(1, 2, 4),
				Observable.<Integer> empty(), just(5, 7), just(8, 9));
	}

	@SuppressWarnings("unchecked")
	@Test(timeout = 100000000)
	public void test9() {
		checkEquals(asList(1, 2, 3, 4, 5), just(4, 5), just(2, 3), just(1)
				.onBackpressureBuffer());
	}

	private static void checkEquals(List<Integer> expected,
			Observable<Integer>... input) {
		Observable<Integer> o = Observable
				.create(new OnSubscribeRefreshSelect<Integer>(Arrays
						.asList(input), BigSort.minimum(COMPARATOR)));
		List<Integer> values = o.toList().toBlocking().single();
		assertEquals(expected, values);

	}

	@Test
	public void testRecursion() {
		final Func1<List<Integer>, Integer> total = new Func1<List<Integer>, Integer>() {

			@Override
			public Integer call(List<Integer> list) {
				int count = 0;
				for (int i : list)
					count += i;
				return count;
			}
		};
		Transformer<Integer, Integer> f = new Transformer<Integer, Integer>() {

			@Override
			public Observable<Integer> call(Observable<Integer> x) {
				return x.buffer(2).map(total);
			}
		};

		Observable<Integer> ones = Observable.just(1).repeat().take(100);
		System.out
				.println(ones.compose(f).compose(f).compose(f).compose(f)
						.compose(f).compose(f).compose(f).count().toBlocking()
						.single());

		int sum = ones
				.map(Util.<Integer> nested())
				.reduce(new Func2<Observable<Integer>, Observable<Integer>, Observable<Integer>>() {

					@Override
					public Observable<Integer> call(Observable<Integer> a,
							Observable<Integer> b) {
						return a.concatWith(b).toList().map(total);
					}
				}).flatMap(Functions.<Observable<Integer>> identity()).single()
				.toBlocking().single();
		assertEquals(100, sum);
	}

}
