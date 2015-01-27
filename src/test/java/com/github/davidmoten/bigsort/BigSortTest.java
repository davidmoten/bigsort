package com.github.davidmoten.bigsort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.testing.TestingHelper;

public class BigSortTest extends TestCase {

	public static TestSuite suite() {

		return TestingHelper
				.function(SORTER)
				// test empty
				.name("testSortOfEmptyReturnsEmpty").fromEmpty().expectEmpty()
				.name("testSortAFew").from(3, 2, 1).expect(1, 2, 3)
				.name("testSortAFew").from(3, 2, 1).expect(1, 2, 3)
				// return suite
				.testSuite(BigSortTest.class);
	}

	public void testLarge() {
		final int n = 100000;
		Observable<Integer> source = Observable.range(0, n).map(
				new Func1<Integer, Integer>() {
					@Override
					public Integer call(Integer i) {
						return n - i;
					}
				});
		final AtomicInteger count = new AtomicInteger();
		SORTER.call(source).observeOn(Schedulers.immediate())
				.forEach(new Action1<Integer>() {
					@Override
					public void call(Integer i) {
						if (i != count.incrementAndGet())
							throw new RuntimeException("not expected");
					}
				});
		assertEquals(n, count.get());
	}

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static Func1<Observable<Integer>, Observable<Integer>> SORTER = new Func1<Observable<Integer>, Observable<Integer>>() {

		@Override
		public Observable<Integer> call(Observable<Integer> source) {
			Comparator<Integer> comparator = createComparator();
			Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
			Func1<File, Observable<Integer>> reader = createReader();
			Func0<File> resourceFactory = createResourceFactory();
			Action1<File> resourceDisposer = createResourceDisposer();
			int maxToSortInMemoryPerThread = 100;
			int maxTempResources = 10;

			return BigSort.sort(source, comparator, writer, reader,
					resourceFactory, resourceDisposer,
					maxToSortInMemoryPerThread, maxTempResources,
					Schedulers.immediate());
		}

	};

	private static Comparator<Integer> createComparator() {
		return new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return o1.compareTo(o2);
			}
		};
	}

	private static Func1<File, Observable<Integer>> createReader() {
		return new Func1<File, Observable<Integer>>() {

			@Override
			public Observable<Integer> call(final File file) {
				return Strings.split(Strings.from(file), "\n")
						.filter(new Func1<String, Boolean>() {

							@Override
							public Boolean call(final String s) {
								return s.length() > 0;
							}
						}).map(new Func1<String, Integer>() {

							@Override
							public Integer call(final String s) {
								return Integer.parseInt(s);
							}
						});
			}
		};
	}

	private static Func2<Observable<Integer>, File, Observable<File>> createWriter() {
		return new Func2<Observable<Integer>, File, Observable<File>>() {
			@Override
			public Observable<File> call(final Observable<Integer> lines,
					final File file) {
				return Observable.using(new Func0<FileOutputStream>() {

					@Override
					public FileOutputStream call() {
						try {
							return new FileOutputStream(file);
						} catch (FileNotFoundException e) {
							throw new RuntimeException(e);
						}
					}
				}, new Func1<FileOutputStream, Observable<File>>() {

					@Override
					public Observable<File> call(final FileOutputStream fos) {
						return lines.doOnNext(new Action1<Integer>() {

							@Override
							public void call(Integer s) {
								try {
									fos.write((s + "\n").getBytes(UTF8));
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
							}
						}).count().map(new Func1<Integer, File>() {

							@Override
							public File call(Integer count) {
								return file;
							}
						});
					}
				}, new Action1<FileOutputStream>() {

					@Override
					public void call(FileOutputStream fos) {
						try {
							fos.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			}
		};
	}

	private static Func0<File> createResourceFactory() {
		return new Func0<File>() {

			@Override
			public File call() {
				try {
					return File.createTempFile("temp", ".txt", new File(
							"target"));
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	private static Action1<File> createResourceDisposer() {
		return new Action1<File>() {
			@Override
			public void call(File file) {
				file.delete();
			}
		};
	}

}
