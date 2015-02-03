package com.github.davidmoten.bigsort;

import static com.github.davidmoten.util.Optional.absent;
import static com.github.davidmoten.util.Optional.of;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.rx.operators.OnSubscribeRefreshSelect;
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

	private static <T, Resource> Func1<List<Resource>, Observable<Resource>> mergeWhenSizeIsMaxTempResources(
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func1<Resource, Observable<T>> reader,
			final Func0<Resource> resourceFactory,
			final Action1<Resource> resourceDisposer, final int maxTempResources) {
		return new Func1<List<Resource>, Observable<Resource>>() {

			@Override
			public Observable<Resource> call(final List<Resource> resources) {
				System.out.println("checking for merge:" + resources);
				if (resources.size() < maxTempResources)
					return Observable.from(resources);
				else {
					Resource resource = resourceFactory.call();
					Observable<T> items = merge(resources, comparator, reader)
							.doOnCompleted(new Action0() {
								@Override
								public void call() {
									for (Resource r : resources)
										resourceDisposer.call(r);
								}
							});
					return writer.call(items, resource);
				}
			}
		};
	}

	private static <T, Resource> Observable<T> merge(List<Resource> resources,
			final Comparator<T> comparator,
			final Func1<Resource, Observable<T>> reader) {
		return Observable.just(resources).flatMap(
				new Func1<List<Resource>, Observable<T>>() {

					@Override
					public Observable<T> call(List<Resource> resources) {
						List<Observable<T>> obs = new ArrayList<Observable<T>>();
						for (Resource resource : resources)
							obs.add(reader.call(resource));
						return Observable.create(
								new OnSubscribeRefreshSelect<T>(obs, BigSort
										.<T> minimum(comparator)))
						// TODO remove this once honours backp
								.onBackpressureBuffer();
					}
				});
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

}
