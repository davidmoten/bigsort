package com.github.davidmoten.bigsort;

import static com.github.davidmoten.util.Optional.absent;
import static com.github.davidmoten.util.Optional.of;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
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
import rx.observables.AbstractOnSubscribe;

import com.github.davidmoten.rx.Checked;
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

	public static Observable<String> sortLines(Observable<String> source,
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

	public static <T extends Serializable> Observable<T> sort(
			Observable<T> source, Comparator<T> comparator,
			int maxToSortInMemoryPerThread, final int maxTempResources,
			Scheduler scheduler) {
		Func2<Observable<T>, File, Observable<File>> writer = createWriter();
		Func1<File, Observable<T>> reader = createReader();
		Func0<File> resourceFactory = createFileResourceFactory(Optional
				.absent());
		Action1<File> resourceDisposer = createFileResourceDisposer();
		return sort(source, comparator, writer, reader, resourceFactory,
				resourceDisposer, maxToSortInMemoryPerThread, maxTempResources,
				scheduler);
	}

	public static <T extends Serializable & Comparable<T>> Observable<T> sort(
			Observable<T> source, int maxToSortInMemoryPerThread,
			final int maxTempResources, Scheduler scheduler) {
		Func2<Observable<T>, File, Observable<File>> writer = createWriter();
		Func1<File, Observable<T>> reader = createReader();
		Func0<File> resourceFactory = createFileResourceFactory(Optional
				.absent());
		Action1<File> resourceDisposer = createFileResourceDisposer();
		return sort(source, (a, b) -> a.compareTo(b), writer, reader,
				resourceFactory, resourceDisposer, maxToSortInMemoryPerThread,
				maxTempResources, scheduler);
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
		return Checked.f0(() -> {
			String directory = tempDirectory.or(System
					.getProperty("java.io.tmpdir"));
			return File.createTempFile("temp", ".txt", new File(directory));
		});
	}

	public static Func0<File> createFileResourceFactory() {
		return createFileResourceFactory(Optional.absent());
	}

	public static Action1<File> createFileResourceDisposer() {
		return file -> file.delete();
	}

	private static class EndOfFile implements Serializable {

		private static final long serialVersionUID = -6138242084166387379L;

	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> Func1<File, Observable<T>> createReader() {
		return file -> Observable.create(AbstractOnSubscribe.create(s -> {
			ObjectInputStream ios = s.state();
			try {
				Object o = ios.readObject();
				if (o instanceof EndOfFile)
					s.onCompleted();
				else
					s.onNext((T) o);
			} catch (IOException e) {
				s.onError(e);
			} catch (ClassNotFoundException e) {
				s.onError(e);
			}
		},
		// creator
				Checked.f1(sub -> new ObjectInputStream(
						new BufferedInputStream(new FileInputStream(file)))),
				// disposer
				Checked.a1(ois -> ois.close())));
	}

	public static <T extends Serializable> Func2<Observable<T>, File, Observable<File>> createWriter() {
		return (items, file) -> {
			Func0<ObjectOutputStream> resourceFactory = Checked
					.f0(() -> new ObjectOutputStream(new BufferedOutputStream(
							new FileOutputStream(file))));
			Func1<ObjectOutputStream, Observable<T>> observableFactory = os -> items
					.doOnNext(Checked.a1(x -> os.writeObject(x)));
			Action1<ObjectOutputStream> disposeAction = Checked.a1(os -> {
				os.writeObject(new EndOfFile());
				os.close();
			});
			return Observable
					.using(resourceFactory, observableFactory, disposeAction,
							true).count().map(count -> file);
		};
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
			Func0<OutputStream> resourceFactory = Checked
					.f0(() -> new BufferedOutputStream(new FileOutputStream(
							file)));
			Func1<OutputStream, Observable<String>> observableFactory = os -> lines
					.doOnNext(Checked.a1(n -> os.write((n + "\n")
							.getBytes(charset))));
			Action1<OutputStream> disposeAction = Checked.a1(os -> os.close());
			return Observable
					.using(resourceFactory, observableFactory, disposeAction,
							true).count().map(count -> file);
		};

	}
}
