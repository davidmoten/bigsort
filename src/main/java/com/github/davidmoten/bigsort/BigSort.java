package com.github.davidmoten.bigsort;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

public class BigSort {

	public static interface Reader<T> {
		Observable<T> read(File file);
	}

	public static <T> Observable<T> sort(Observable<T> source,
			final Comparator<T> comparator,
			final Action2<Observable<T>, File> writer,
			final Func1<File, Observable<T>> reader,
			final Func0<File> fileFactory, int maxToSortInMemoryPerThread,
			final int maxTempFiles) {
		source.buffer(maxToSortInMemoryPerThread)
				.flatMap(new Func1<List<T>, Observable<File>>() {
					@Override
					public Observable<File> call(List<T> list) {
						return Observable.just(list)
								.map(new Func1<List<T>, List<T>>() {
									@Override
									public List<T> call(List<T> list) {
										Collections.sort(list, comparator);
										return list;
									}
								}).map(new Func1<List<T>, File>() {
									@Override
									public File call(List<T> a) {
										File file = fileFactory.call();
										writer.call(Observable.from(a), file);
										return file;
									}
								});
					}
				})
				// merge the files in each list
				.reduce(Collections.<File> emptyList(),
						new Func2<List<File>, File, List<File>>() {

							@Override
							public List<File> call(List<File> files, File f) {
								List<File> list = new ArrayList<File>(files);
								list.add(f);
								if (list.size() < maxTempFiles)
									return list;
								else {
									File file = fileFactory.call();
									items = merge(list, comparator, reader,
											file);
									return Collections.singletonList(file);
								}
							}
						});
		return null;

	}

	private static <T> Observable<T> merge(List<File> files,
			Comparator<T> comparator, Func1<File, Observable<T>> reader) {

	}
}
