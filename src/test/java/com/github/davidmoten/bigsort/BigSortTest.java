package com.github.davidmoten.bigsort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.testing.TestingHelper;

public class BigSortTest extends TestCase {

	public static TestSuite suite() {

		return TestingHelper
				.function(SORTER)
				// test empty
				.name("testSortOfEmptyReturnsEmpty").fromEmpty().expectEmpty()
				.name("testSortAFew").from("10", "09", "08")
				.expect("08", "09", "10")
				// return suite
				.testSuite(BigSortTest.class);
	}

	public void testDummy() {
		// just here to fool eclipse
	}

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static Func1<Observable<String>, Observable<String>> SORTER = new Func1<Observable<String>, Observable<String>>() {

		@Override
		public Observable<String> call(Observable<String> source) {
			Comparator<String> comparator = new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			};

			Func2<Observable<String>, File, Observable<File>> writer = new Func2<Observable<String>, File, Observable<File>>() {
				@Override
				public Observable<File> call(final Observable<String> lines,
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
							return lines.doOnNext(new Action1<String>() {

								@Override
								public void call(String s) {
									try {
										fos.write(s.getBytes(UTF8));
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

			Func1<File, Observable<String>> reader = new Func1<File, Observable<String>>() {

				@Override
				public Observable<String> call(File file) {
					return null;
				}
			};

			Func0<File> resourceFactory = new Func0<File>() {

				@Override
				public File call() {
					try {
						return File.createTempFile("temp", ".txt");
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			};
			int maxToSortInMemoryPerThread = 100;
			int maxTempResources = 10;
			return BigSort.sort(source, comparator, writer, reader,
					resourceFactory, maxToSortInMemoryPerThread,
					maxTempResources, Schedulers.immediate());
		}
	};

}
