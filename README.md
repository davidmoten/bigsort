# bigsort
<a href="https://travis-ci.org/davidmoten/bigsort"><img src="https://travis-ci.org/davidmoten/bigsort.svg"/></a><br/>
Sorts an arbitrarily large stream by serializing sorted chunks to temporary resources (user-definable) and merging those resources

Status: *pre-alpha*

## Features
* uses [RxJava](https://github.com/ReactiveX/RxJava)
* Configurable concurrency

How to sort lines of text
-------------------------------
Simple string lines sorting example:

```java
import rx.Observable;
import rx.Scheduler;
import com.github.davidmoten.bigsort.BigSort;

Observable<String> source = Observable.just("c", "b", "a");

int maxToSortInMemoryPerThread = 3;
int maxTempFiles = 2;

// sort in parallel
Scheduler scheduler = Schedulers.computation();

Observable<String> sorted = 
        BigSort.sort(source, maxToSortInMemoryPerThread,
                     maxTempFiles, scheduler);
```

More generic example
-------------------------

A more generic example that sorts a stream of ```Integer```, also using files but some other resource type 
(for example database tables or S3) could be used provided *writer*, *reader*, *resourceFactory*,
*resourceDisposer* are supplied for that resource type.

Below is an extract from [BigSortTest.java](src/test/java/com/github/davidmoten/bigsort/BigSortTest.java):

```java
Comparator<Integer> comparator = createComparator();
Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
Func1<File, Observable<Integer>> reader = createReader();
Func0<File> resourceFactory = BigSort.createFileResourceFactory();
Action1<File> resourceDisposer = BigSort.createFileResourceDisposer();
// sort synchronously
Scheduler scheduler = Schedulers.immediate();

Observable<Integer> sorted = 
        BigSort.sort(source, comparator, writer, reader,
					resourceFactory, resourceDisposer, 
					maxToSortInMemoryPerThread, maxTempResources,
					scheduler);
```



