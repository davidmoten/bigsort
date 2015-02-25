package com.github.davidmoten.bigsort;

import static com.github.davidmoten.util.Optional.absent;
import static com.github.davidmoten.util.Optional.of;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import rx.Observable;
import rx.Scheduler;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.operators.OperatorResourceMerger;
import com.github.davidmoten.util.Optional;
import com.github.davidmoten.util.Preconditions;

public class BigSort {

	public static <T, Resource> Observable<T> sort(Observable<T> source,
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func1<Resource, Observable<T>> reader,
			final Func0<Resource> resourceFactory,
			final Action1<Resource> resourceDisposer,
			int maxToSortInMemoryPerThread, final int maxTempResources,
			Scheduler scheduler) {
		Preconditions.checkArgument(maxToSortInMemoryPerThread > 0,
				"maxToSortInMemoryPerThread must be greater than 0");
		Preconditions.checkArgument(maxTempResources >= 2,
				"maxTempResources must be at least 2");
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
						maxTempResources))
				// help out backpressure because ResourceMerger doesn't support
				// yet
				.onBackpressureBuffer()
				// emit the contents of the last file in the reduction process
				.flatMap(reader);

	}

	public static Observable<String> sort(Observable<String> source,
			int maxToSortInMemoryPerThread, final int maxTempResources,
			Scheduler scheduler) {
		Comparator<String> comparator = (a, b) -> a.compareTo(b);
		Func2<Observable<String>, File, Observable<File>> writer = createLineWriter(Charset
				.forName("UTF8"));
		Func1<File, Observable<String>> reader = createLineReader();
		Func0<File> resourceFactory = createFileResourceFactory(Optional
				.absent());
		Action1<File> resourceDisposer = createFileResourceDisposer();
		return sort(source, comparator, writer, reader, resourceFactory,
				resourceDisposer, maxToSortInMemoryPerThread, maxTempResources,
				scheduler);
	}

	private static <T, Resource> Func1<List<T>, Observable<Resource>> sortInMemoryAndWriteToAResource(
			final Comparator<T> comparator,
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func0<Resource> resourceFactory, final Scheduler scheduler) {
		return list -> Observable.just(list)
		// sort
				.map(sortList(comparator))
				// write to resource
				.flatMap(writeToResource(writer, resourceFactory))
				// subscribe on desired scheduler
				.subscribeOn(scheduler);
	};

	private static <T, Resource> Func1<List<T>, List<T>> sortList(
			final Comparator<T> comparator) {
		return list -> {
			Collections.sort(list, comparator);
			return list;
		};
	}

	private static <T, Resource> Func1<List<T>, Observable<Resource>> writeToResource(
			final Func2<Observable<T>, Resource, Observable<Resource>> writer,
			final Func0<Resource> resourceFactory) {
		return a -> {
			Resource resource = resourceFactory.call();
			return writer.call(Observable.from(a), resource);
		};
	}

	public static <T> Func1<List<T>, Integer> minimum(
			final Comparator<T> comparator) {
		return list -> {
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
		};
	}

	public static Func0<File> createFileResourceFactory(
			Optional<String> tempDirectory) {
		return () -> {
			try {
				String directory = tempDirectory.or(System
						.getProperty("java.io.tmpdir"));
				return File.createTempFile("temp", ".txt", new File(directory));
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		};
	}

	public static Func0<File> createFileResourceFactory() {
		return createFileResourceFactory(Optional.absent());
	}

	public static Action1<File> createFileResourceDisposer() {
		return file -> file.delete();
	}

	public static Func1<File, Observable<String>> createLineReader() {
		return file -> {
			// split/join the strings by new line character
			return Strings.split(Strings.from(file), "\n").filter(
					s -> s.length() > 0);
		};
	}

	public static Func2<Observable<String>, File, Observable<File>> createLineWriter(
			final Charset charset) {
		return (lines, file) -> {
			// log.info("creating writer for " + file);
			Func0<OutputStream> resourceFactory = () -> {
				// log.info("opening writing " + file);
				try {
					return new BufferedOutputStream(new FileOutputStream(file));
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
				}
			};
			Func1<OutputStream, Observable<String>> observableFactory = os -> {
				return lines.doOnNext(n -> {
					// log.info("writing " + n + " to " + file);
						try {
							os.write((n + "\n").getBytes(charset));
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					});
			};
			Action1<OutputStream> disposeAction = os -> {
				try {
					// log.info("closing writing file " + file);
					os.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			};
			return Observable
					.using(resourceFactory, observableFactory, disposeAction,
							true).count().map(count -> file);
		};

	}
}
