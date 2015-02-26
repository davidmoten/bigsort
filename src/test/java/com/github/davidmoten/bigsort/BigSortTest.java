package com.github.davidmoten.bigsort;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.testing.TestingHelper;
import com.github.davidmoten.util.Optional;

public class BigSortTest extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(BigSortTest.class);

    public static TestSuite suite() {
        TestingHelper.includeBackpressureRequestOverflowTest = false;
        return TestingHelper.function(sorter(2, 2, Schedulers.immediate()))
                .waitForTerminalEvent(100, TimeUnit.MILLISECONDS)
                .waitForMoreTerminalEvents(100, TimeUnit.MILLISECONDS)
                // test empty
                .name("testSortOfEmptyReturnsEmpty").fromEmpty().expectEmpty().name("testSortAFew")
                .from(3, 2, 1).expect(1, 2, 3).name("testSortAFew").from(3, 2, 1).expect(1, 2, 3)
                // return suite
                .testSuite(BigSortTest.class);
    }

    public void testSort128by1MaxTemp2() {
        performTest(128, 1, 2);
    }

    public void testSort128by2MaxTemp2() {
        performTest(128, 2, 2);
    }

    public void testSort128by1MaxTemp10() {
        performTest(128, 1, 10);
    }

    public void testSort128by64MaxTemp10() {
        performTest(128, 64, 10);
    }

    public void testSort128by256MaxTemp10() {
        performTest(128, 256, 10);
    }

    public void testSort100Kby10KMaxTemp10() {
        performTest(100000, 10000, 10);
    }

    public void testSortUsingSerialization() {
        int n = 10;
        Observable<Integer> source = Observable.range(1, n).map(i -> n - i + 1);
        List<Integer> result = BigSort.sort(source, 2, 2, Schedulers.immediate()).toList()
                .toBlocking().single();
        assertEquals(Observable.range(1, n).toList().toBlocking().single(), result);
    }

    // public void testSort1Mby100KMaxTemp10() {
    // performTest(1000000, 100000, 10);
    // }

    private static void performTest(int size, int maxToSortPerThread, int maxTempFiles) {
        log.info("sorting " + size + " values using maxToSort=" + maxToSortPerThread
                + " and maxTempFiles=" + maxTempFiles);
        final int n = size;// passes on 127!!
        // source is n, n-1, .., 0
        Observable<Integer> source = createDescendingRange(n);
        List<Integer> range = Observable.range(1, n).toList().toBlocking().single();
        long t = System.currentTimeMillis();
        assertEquals(range,
                sorter(maxToSortPerThread, maxTempFiles, Schedulers.immediate()).call(source)
                        .toList().toBlocking().single());
        log.info("time to sort using immediate() = " + ((System.currentTimeMillis() - t) / 1000.0)
                + "s");
        t = System.currentTimeMillis();
        assertEquals(range, sorter(maxToSortPerThread, maxTempFiles, Schedulers.computation())
                .call(source).toList().toBlocking().single());
        log.info("time to sort using computation() = "
                + ((System.currentTimeMillis() - t) / 1000.0) + "s");
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
            final int maxToSortInMemoryPerThread, final int maxTempResources, Scheduler scheduler) {
        return source -> {
            Comparator<Integer> comparator = createComparator();
            Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
            Func1<File, Observable<Integer>> reader = createReader();
            Func0<File> resourceFactory = BigSort.createFileResourceFactory(Optional.of("target"));
            Action1<File> resourceDisposer = BigSort.createFileResourceDisposer();

            return BigSort.sort(source, comparator, writer, reader, resourceFactory,
                    resourceDisposer, maxToSortInMemoryPerThread, maxTempResources, scheduler);
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

    private static Func2<Observable<Integer>, File, Observable<File>> createWriter() {
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
            Func1<OutputStream, Observable<Integer>> observableFactory = os -> {
                return lines.doOnNext(n -> {
                    // log.info("writing " + n + " to " + file);
                        try {
                            os.write((n + "\n").getBytes(UTF8));
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
            return Observable.using(resourceFactory, observableFactory, disposeAction, true)
                    .count().map(count -> file);
        };
    }

    private static Func1<String, Boolean> nonEmptyLines() {
        return s -> s.length() > 0;
    }

    private static Func1<String, Integer> toInteger() {
        return s -> Integer.parseInt(s);
    }

}
