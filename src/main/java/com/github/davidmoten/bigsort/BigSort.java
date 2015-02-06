package com.github.davidmoten.bigsort;

import static com.github.davidmoten.util.Optional.absent;
import static com.github.davidmoten.util.Optional.of;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Observable;
import rx.Scheduler;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.rx.operators.OperatorResourceMerger;
import com.github.davidmoten.util.Optional;

public class BigSort {

	public static <T, Resource> Observable<T> sort(Observable<T> source,
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func1<Resource, Observable<T>> reader,
			final Func0<Resource> resourceFactory,
			final Action1<Resource> resourceDisposer,
			int maxToSortInMemoryPerThread, final int maxTempResources,
			Scheduler scheduler) {
		return source
		// buffer into groups small enough to sort in memory
				.buffer(maxToSortInMemoryPerThread)
				// sort each buffer to a resource
				.flatMap(
						sortInMemoryAndWriteToAResource(comparator, writer,
								resourceFactory, scheduler))
				// reduce by merging groups of resources to a single resource
				// once the resource count is maxTempResources
				.lift(new OperatorResourceMerger<Resource, T>(comparator,
						writer, reader, resourceFactory, resourceDisposer,
						maxTempResources)).flatMap(reader);

	}

	private static <T, Resource> Func1<List<T>, Observable<Resource>> sortInMemoryAndWriteToAResource(
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func0<Resource> resourceFactory, final Scheduler scheduler) {
		return new Func1<List<T>, Observable<Resource>>() {
			@Override
			public Observable<Resource> call(List<T> list) {
				return Observable.just(list)
				// sort
						.map(sortList(comparator))
						// write to resource
						.flatMap(writeToResource(writer, resourceFactory))
						// subscribe on desired scheduler
						.subscribeOn(scheduler);
			}
		};
	}

	private static <T, Resource> Func1<List<T>, List<T>> sortList(
			final Comparator<T> comparator) {
		return new Func1<List<T>, List<T>>() {
			@Override
			public List<T> call(List<T> list) {
				Collections.sort(list, comparator);
				return list;
			}
		};
	}

	private static <T, Resource> Func1<List<T>, Observable<Resource>> writeToResource(
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func0<Resource> resourceFactory) {
		return new Func1<List<T>, Observable<Resource>>() {
			@Override
			public Observable<Resource> call(List<T> a) {
				Resource resource = resourceFactory.call();
				return writer.call(Observable.from(a), resource);
			}
		};
	}

	public static <T> Func1<List<T>, Integer> minimum(
			final Comparator<T> comparator) {
		return new Func1<List<T>, Integer>() {

			@Override
			public Integer call(List<T> list) {
				if (list.isEmpty())
					throw new RuntimeException("list cannot be empty");
				Optional<Integer> index = absent();
				Optional<T> min = Optional.absent();
				for (int i = 0; i < list.size(); i++) {
					T value = list.get(i);
					if (!index.isPresent()
							|| comparator.compare(value, min.get()) < 0) {
						index = of(i);
						min = of(value);
					}
				}
				return index.get();
			}
		};
	}

}
