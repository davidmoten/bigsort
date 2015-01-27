package com.github.davidmoten.bigsort;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Notification;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.FuncN;

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
				// make each resource an Observable<Resource>
				.nest()
				// reduce by merging groups of resources to a single resource
				// once the resource count is maxTempResources
				.reduce(Observable.<Resource> empty(),
						mergeResources(comparator, writer, reader,
								resourceFactory, resourceDisposer,
								maxTempResources))
				// flatten to a sequence of Resource
				.flatMap(
						com.github.davidmoten.rx.Functions
								.<Observable<Resource>> identity())
				// accumulate to a list
				.toList()
				// merge remaining resources
				.flatMap(mergeResourceList(comparator, reader));
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

	private static <T, Resource> Func2<Observable<Resource>, Observable<Resource>, Observable<Resource>> mergeResources(
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func1<Resource, Observable<T>> reader,
			final Func0<Resource> resourceFactory,
			final Action1<Resource> resourceDisposer, final int maxTempResources) {
		return new Func2<Observable<Resource>, Observable<Resource>, Observable<Resource>>() {

			@Override
			public Observable<Resource> call(Observable<Resource> resources,
					final Observable<Resource> resource) {
				return resources
						.concatWith(resource)
						.toList()
						.flatMap(
								mergeWhenSizeIsMaxTempResources(comparator,
										writer, reader, resourceFactory,
										resourceDisposer, maxTempResources));
			}
		};
	}

	private static <T, Resource> Func1<List<Resource>, Observable<T>> mergeResourceList(
			final Comparator<T> comparator,
			final Func1<Resource, Observable<T>> reader) {
		return new Func1<List<Resource>, Observable<T>>() {

			@Override
			public Observable<T> call(List<Resource> list) {
				return merge(list, comparator, reader);
			}
		};
	}

	private static <T, Resource> Func1<List<Resource>, Observable<Resource>> mergeWhenSizeIsMaxTempResources(
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func1<Resource, Observable<T>> reader,
			final Func0<Resource> resourceFactory,
			final Action1<Resource> resourceDisposer, final int maxTempResources) {
		return new Func1<List<Resource>, Observable<Resource>>() {

			@Override
			public Observable<Resource> call(final List<Resource> list) {
				System.out.println("checking for merge:" + list);
				if (list.size() < maxTempResources)
					return Observable.from(list);
				else {
					Resource resource = resourceFactory.call();
					Observable<T> items = merge(list, comparator, reader)
							.doOnCompleted(new Action0() {
								@Override
								public void call() {
									for (Resource r : list)
										resourceDisposer.call(r);
								}
							});
					return writer.call(items, resource);
				}
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

	private static <T, Resource> Observable<T> merge(List<Resource> resources,
			final Comparator<T> comparator,
			Func1<Resource, Observable<T>> reader) {
		return Observable
				.zip(Observable.from(resources)
				// read
						.map(reader)
						// materialize and ensure each stream does not complete
						.map(BigSort
								.<T> materializeAndRepeatOnCompleteIndefinitely()),
						BigSort.<Notification<T>> toList())
				// keep going till all observables complete
				.takeWhile(BigSort.<T> listHasOnNext())
				// take miniumum
				.map(BigSort.<T> toMinimum(comparator));

	}

	private static <T> Func1<List<Notification<T>>, T> toMinimum(
			final Comparator<T> comparator) {
		return new Func1<List<Notification<T>>, T>() {

			@Override
			public T call(List<Notification<T>> list) {
				T t = null;
				for (Notification<T> notification : list) {
					if (notification.isOnNext()) {
						T v = notification.getValue();
						if (t == null || comparator.compare(v, t) < 0)
							t = v;
					}
				}
				if (t == null)
					throw new RuntimeException("unexpected");
				else
					return t;
			}
		};
	}

	private static <T> Func1<List<Notification<T>>, Boolean> listHasOnNext() {
		return new Func1<List<Notification<T>>, Boolean>() {
			@Override
			public Boolean call(List<Notification<T>> list) {
				for (Notification<T> notif : list)
					if (notif.isOnNext())
						return true;
				return false;
			}
		};
	}

	private static <T> Func1<Observable<T>, Observable<Notification<T>>> materializeAndRepeatOnCompleteIndefinitely() {
		return new Func1<Observable<T>, Observable<Notification<T>>>() {

			@Override
			public Observable<Notification<T>> call(Observable<T> o) {
				return o.materialize().concatWith(
						Observable.just(Notification.<T> createOnCompleted())
								.repeat());
			}
		};
	}

	private static <T> FuncN<List<T>> toList() {
		return new FuncN<List<T>>() {

			@SuppressWarnings("unchecked")
			@Override
			public List<T> call(Object... items) {
				return Arrays.asList((T[]) items);
			}
		};
	}
}
