package com.github.davidmoten.bigsort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.testing.TestingHelper;

public class BigSortTest extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(BigSortTest.class);

    public static TestSuite suite() {
        TestingHelper.includeBackpressureRequestOverflowTest = false;
        return TestingHelper.function(sorter(2, 2))
                .waitForTerminalEvent(100, TimeUnit.MILLISECONDS)
                .waitForMoreTerminalEvents(100, TimeUnit.MILLISECONDS)
                // test empty
                .name("testSortOfEmptyReturnsEmpty").fromEmpty().expectEmpty().name("testSortAFew")
                .from(3, 2, 1).expect(1, 2, 3).name("testSortAFew").from(3, 2, 1).expect(1, 2, 3)
                // return suite
                .testSuite(BigSortTest.class);
    }

    @Test
    public void testSort128by1MaxTemp2() {
        performTest(128, 1, 2);
    }

    @Test
    public void testSort128by2MaxTemp2() {
        performTest(128, 2, 2);
    }

    @Test
    public void testSort128by1MaxTemp10() {
        performTest(128, 1, 10);
    }

    @Test
    public void testSort128by64MaxTemp10() {
        performTest(128, 64, 10);
    }

    @Test
    public void testSort128by256MaxTemp10() {
        performTest(128, 256, 10);
    }

    @Test
    public void testSort1Mby100KMaxTemp10() {
        performTest(1000000, 10000, 10);
    }

    private static void performTest(int size, int maxToSortPerThread, int maxTempFiles) {
        log.info("sorting " + size + " values using maxToSort=" + maxToSortPerThread
                + " and maxTempFiles=" + maxTempFiles);
        final int n = 128;// passes on 127!!
        // source is n, n-1, .., 0
        Observable<Integer> source = createDescendingRange(n);
        List<Integer> range = Observable.range(1, n).toList().toBlocking().single();
        assertEquals(range, sorter(1, 2).call(source).toList().toBlocking().single());
    }

    private static Observable<Integer> createDescendingRange(final int n) {
        return Observable.range(0, n).map(new Func1<Integer, Integer>() {
            @Override
            public Integer call(Integer i) {
                return n - i;
            }
        });
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static Func1<Observable<Integer>, Observable<Integer>> sorter(
            final int maxToSortInMemoryPerThread, final int maxTempResources) {
        return source -> {
            Comparator<Integer> comparator = createComparator();
            Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
            Func1<File, Observable<Integer>> reader = createReader();
            Func0<File> resourceFactory = createResourceFactory();
            Action1<File> resourceDisposer = createResourceDisposer();

            return BigSort.sort(source, comparator, writer, reader, resourceFactory,
                    resourceDisposer, maxToSortInMemoryPerThread, maxTempResources,
                    Schedulers.immediate());
        };
    }

    private static Comparator<Integer> createComparator() {
        return (o1, o2) -> o1.compareTo(o2);
    }

    private static Func1<File, Observable<Integer>> createReader() {
        return file -> {
            Observable<String> strings =
            // read the strings from a file
            Strings.from(file);

            return
            // split/join the strings by new line character
            Strings.split(strings, "\n")
            // non-blank lines only
                    .filter(nonEmptyLines())
                    // log
                    // .doOnNext(log())
                    // to an integer
                    .map(toInteger());
        };
    }

    private static Func1<String, Boolean> nonEmptyLines() {
        return s -> s.length() > 0;
    }

    private static Func1<String, Integer> toInteger() {
        return s -> Integer.parseInt(s);
    }

    private static Func2<Observable<Integer>, File, Observable<File>> createWriter() {
        return (lines, file) -> {
            // log.info("creating writer for " + file);
            Func0<FileOutputStream> resourceFactory = () -> {
                // log.info("opening writing " + file);
                try {
                    return new FileOutputStream(file);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            };
            Func1<FileOutputStream, Observable<File>> observableFactory = fos -> {
                return lines.doOnNext(n -> {
                    // log.info("writing " + n + " to " + file);
                        try {
                            fos.write((n + "\n").getBytes(UTF8));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).count().map(count -> file);
            };
            Action1<FileOutputStream> disposeAction = fos -> {
                try {
                    // log.info("closing writing file " + file);
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };
            return Observable.using(resourceFactory, observableFactory, disposeAction, true);
        };
    }

    private static Func0<File> createResourceFactory() {
        return () -> {
            try {
                return File.createTempFile("temp", ".txt", new File("target"));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Action1<File> createResourceDisposer() {
        return file -> file.delete();
    }

}
