package com.github.davidmoten.bigsort;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Notification;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.FuncN;

public class BigSort {

	public static interface Reader<T> {
		Observable<T> read(File file);
	}

	public static <T> Observable<T> sort(
	//
			Observable<T> source,
			//
			final Comparator<T> comparator,
			//
			final Func2<Observable<T>, File, Observable<File>> writer,
			//
			final Func1<File, Observable<T>> reader,
			//
			final Func0<File> fileFactory,
			//
			int maxToSortInMemoryPerThread,
			//
			final int maxTempFiles) {
		return source
				.buffer(maxToSortInMemoryPerThread)
				.flatMap(new Func1<List<T>, Observable<File>>() {
					@Override
					public Observable<File> call(List<T> list) {
						return Observable
								.just(list)
								.map(new Func1<List<T>, List<T>>() {
									@Override
									public List<T> call(List<T> list) {
										Collections.sort(list, comparator);
										return list;
									}
								})
								.flatMap(
										new Func1<List<T>, Observable<File>>() {
											@Override
											public Observable<File> call(
													List<T> a) {
												File file = fileFactory.call();
												return writer.call(
														Observable.from(a),
														file);
											}
										});
					}
				})
				.nest()
				// merge the files in each list
				.reduce(Observable.<File> empty(),
						new Func2<Observable<File>, Observable<File>, Observable<File>>() {

							@Override
							public Observable<File> call(
									Observable<File> files,
									final Observable<File> f) {
								return files
										.concatWith(f)
										.toList()
										.flatMap(
												new Func1<List<File>, Observable<File>>() {

													@Override
													public Observable<File> call(
															List<File> list) {
														if (list.size() < maxTempFiles)
															return Observable
																	.from(list);
														else {
															File file = fileFactory
																	.call();
															Observable<T> items = merge(
																	list,
																	comparator,
																	reader);
															return writer
																	.call(items,
																			file);
														}
													}
												});
							}
						})
				// flatten
				.flatMap(
						com.github.davidmoten.rx.Functions
								.<Observable<File>> identity())

				.toList().flatMap(new Func1<List<File>, Observable<T>>() {

					@Override
					public Observable<T> call(List<File> list) {
						return merge(list, comparator, reader);
					}
				});
	}

	private static <T> Observable<T> merge(List<File> files,
			final Comparator<T> comparator, Func1<File, Observable<T>> reader) {
		return Observable
				.zip(Observable.from(files)
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
						if (t == null)
							t = v;
						else if (comparator.compare(v, t) < 0)
							;
						t = v;
					}
				}
				throw new RuntimeException("unexpected");
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
