package main.java;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

public class SearchEngineProblem {
    static class DocumentWithRelevance {
        private final String documentId;
        private final int relevance;

        DocumentWithRelevance(String documentId, int relevance) {
            this.documentId = documentId;
            this.relevance = relevance;
        }

        String getDocumentId() {
            return documentId;
        }

        int getRelevance() {
            return relevance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DocumentWithRelevance that = (DocumentWithRelevance) o;
            return that.relevance == relevance && Objects.equals(documentId, that.documentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(documentId, relevance);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TODO: This is the interface you should use.
    ////////////////////////////////////////////////////////////////////////////////
    interface Backend {
        // Returns total number of shards.
        int getShardCount();

        // Returns total number of replicas of a given shard.
        int getReplicaCount(int shard);

        // This is a black-box implementation of the backend service.
        // No guarantees on document ids returned from different shards.
        //
        // See https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
        CompletableFuture<List<DocumentWithRelevance>> search(Object query, int shard, int replica);

        // You may use this scheduled executor service to schedule something to be done in the future.
        //
        // See https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledExecutorService.html
        ScheduledExecutorService getScheduler();
    }
    ////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////
    // TODO: This is the class you should implement.
    ////////////////////////////////////////////////////////////////////////////////
    static class SearchEngine {
        private final Backend backend;
        private Random r;

        SearchEngine(Backend backend) {
            this.backend = backend;
            r = new Random();
        }

        private CompletableFuture<List<DocumentWithRelevance>> getSearch(Object query, int shard) {
            int firstRep = r.nextInt(backend.getReplicaCount(shard));
            int secondRep = firstRep;
            while (secondRep == firstRep) {
                secondRep = r.nextInt(backend.getReplicaCount(shard));
            }
            int secondRepp = secondRep;
            CompletableFuture<List<DocumentWithRelevance>> myCompletableFuture = new CompletableFuture<>();
            CompletableFuture<List<DocumentWithRelevance>> firstTry = backend.search(query, shard, firstRep);
            firstTry.thenAccept(myCompletableFuture::complete);
            backend.getScheduler().schedule(() -> {
                if (!firstTry.isDone() && backend.getReplicaCount(shard) > 1) {
                    CompletableFuture<List<DocumentWithRelevance>> secondTry = backend.search(query, shard, secondRepp);
                    secondTry.thenAccept(myCompletableFuture::complete);
                }
            }, 2, TimeUnit.MILLISECONDS);
            return myCompletableFuture;
        }

        private List<DocumentWithRelevance> prettyResults(List<DocumentWithRelevance> allCombo, int limit) {
            List<DocumentWithRelevance> queriesResults = allCombo.stream()
                    .collect(collectingAndThen(toCollection(() ->
                                    new TreeSet<>((o1, o2) -> {
                                        if (o1.getDocumentId().equals(o2.getDocumentId())) return 0;
                                        return o1.getDocumentId().compareTo(o2.getDocumentId());
                                    })),
                            ArrayList::new));
            queriesResults.sort(comparingInt(DocumentWithRelevance::getRelevance).reversed());
            return queriesResults.subList(0, limit);
        }

        // Return a list of top K documents sorted by relevance (descending).
        // Returned documents must be unique.
        CompletableFuture<List<DocumentWithRelevance>> search(Object query, int limit) {
            List<CompletableFuture<List<DocumentWithRelevance>>> futures = new ArrayList<>();
            // Get results from all replicas
            for (int shard = 0; shard < backend.getShardCount(); shard++) {
                CompletableFuture<List<DocumentWithRelevance>> result = getSearch(query, shard);
                futures.add(result);
            }
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            CompletableFuture<List<DocumentWithRelevance>> allCombo =
                    allFutures.thenApply(future ->
                            futures.stream()
                                    .map(CompletableFuture::join)
                                    .flatMap(List::stream)
                                    .collect(Collectors.toList())
                    );
            return allCombo.thenApply(x -> prettyResults(x, limit));
        }
    }
    ////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        setInfoLogEnabled(true);
        setDebugLogEnabled(true);

        int k = 5;
        int requestCount = 1_000;
        int durationMillis = 15_000;

        double rho = 1.0;
        double lambda = (double) requestCount / (double) durationMillis;
        double mu = lambda / rho;

        long[] arrivalTimesMs = new long[requestCount];
        for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
            arrivalTimesMs[requestIndex] = ThreadLocalRandom.current().nextLong(durationMillis);
        }
        Arrays.sort(arrivalTimesMs);

        ExecutorService tank = Executors.newSingleThreadExecutor(new UniqueNamedThreadFactory("TANK"));
        FakeBackend backend = new FakeBackend(4.0, mu, 5, 3, 1);
        SearchEngine engine = new SearchEngine(backend);

        long[] requestTimesNs = new long[requestCount];
        List<CompletableFuture<List<DocumentWithRelevance>>> futures = new ArrayList<>(requestCount);
        for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
            final int finalRequestIndex = requestIndex;
            Thread.sleep(arrivalTimesMs[requestIndex] - (requestIndex > 0 ? arrivalTimesMs[requestIndex - 1] : 0));
            String query = String.format("query-%d", ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
            Future<CompletableFuture<List<DocumentWithRelevance>>> future = tank.submit(() -> {
                long startedAt = System.nanoTime();
                logDebug("Searching for '%s' (request %d)...", query, finalRequestIndex);
                return engine.search(query, k).whenCompleteAsync((strings, throwable) -> {
                    if (throwable != null) {
                        logInfo("Search for query '%s' (request %d) has failed: %s",
                                query, finalRequestIndex, throwable.toString());
                    } else {
                        long completedAt = System.nanoTime();
                        logInfo("Search for query '%s' (request %d) has completed in %dms",
                                query, finalRequestIndex, (completedAt - startedAt) / 1_000_000);
                        requestTimesNs[finalRequestIndex] = completedAt - startedAt;
                    }
                }, tank);
            });
            futures.add(finalRequestIndex, future.get());
        }
        logInfo("Waiting for all requests to complete...");
        futures.forEach(CompletableFuture::join);
        logInfo("All requests are completed.");
        tank.shutdown();

        boolean ok = true;
        for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
            boolean thisOk = true;
            List<DocumentWithRelevance> documents = futures.get(requestIndex).get();
            if (documents.size() != k) {
                logInfo("WRONG: Request %d has returned %d documents, while expecting %d documents!",
                        requestIndex, documents.size(), k);
                thisOk = false;
            }
            for (int documentIndex = 1; documentIndex < documents.size(); ++documentIndex) {
                DocumentWithRelevance current = documents.get(documentIndex);
                DocumentWithRelevance previous = documents.get(documentIndex - 1);
                if (current.getRelevance() > previous.getRelevance()) {
                    logInfo("WRONG: Request %d has returned documents %d and %d in the wrong order!",
                            requestIndex, documentIndex - 1, documentIndex);
                    thisOk = false;
                }
            }
            int uniqueDocuments = documents.stream().map(DocumentWithRelevance::getDocumentId).collect(Collectors.toSet()).size();
            int totalDocuments = documents.size();
            if (totalDocuments != uniqueDocuments) {
                logInfo("WRONG: Request %d has duplicate documents!", requestIndex);
                thisOk = false;
            }
            ok = ok & thisOk;
            if (!thisOk) {
                for (int documentIndex = 0; documentIndex < documents.size(); ++documentIndex) {
                    DocumentWithRelevance document = documents.get(documentIndex);
                    logInfo("WRONG:     D[%d] = { DocumentId: '%s', Relevance: %d }",
                            documentIndex, document.getDocumentId(), document.getRelevance());
                }
            }
        }

        if (!ok) {
            logInfo("*** THE SOLUTION PRODUCES WRONG RESULTS ***");
        }

        Arrays.sort(requestTimesNs);
        double sum = 0.0;
        double sumSq = 0.0;
        double min = 0.0;
        double max = 0.0;
        double q50 = 0.0;
        double q90 = 0.0;
        double q99 = 0.0;
        int q50Index = 50 * requestCount / 100;
        int q90Index = 90 * requestCount / 100;
        int q99Index = 99 * requestCount / 100;
        for (int i = 0; i < requestCount; ++i) {
            double t = (double) requestTimesNs[i] / 1_000_000.0;
            if (i == 0) {
                min = t;
            }
            if (i + 1 == requestCount) {
                max = t;
            }
            if (i == q50Index) {
                q50 = t;
            }
            if (i == q90Index) {
                q90 = t;
            }
            if (i == q99Index) {
                q99 = t;
            }
            sum += t;
            sumSq += t * t;
        }
        double mean = sum / (double) requestCount;
        double stddev = Math.sqrt(sumSq / (double) requestCount - mean * mean);
        logInfo("MIN: %8.3fms | MAX: %8.3fms | MEAN: %8.3fms +- %8.3fms", min, max, mean, stddev);
        logInfo("Q50: %8.3fms | Q90: %8.3fms |  Q99: %8.3fms", q50, q90, q99);

        backend.shutdown();
    }

    ////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////
    // Internals.
    ////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    static class FakeBackend implements Backend {
        static class FakeServer {
            static class Task {
                final CompletableFuture<List<DocumentWithRelevance>> future = new CompletableFuture<>();
                final Object query;
                final long serviceTimeMs;

                Task(Object query, long serviceTimeMs) {
                    this.query = query;
                    this.serviceTimeMs = serviceTimeMs;
                }
            }

            private final int shard;
            private final int replica;
            private final ScheduledExecutorService service;
            private final Queue<Task> queue;
            private final AtomicInteger semaphore;

            FakeServer(int shard, int replica, int concurrency) {
                this.shard = shard;
                this.replica = replica;
                this.service = Executors.newSingleThreadScheduledExecutor(
                        new UniqueNamedThreadFactory(String.format("SRV-Shard%d-Replica%d", shard, replica))
                );
                this.service.execute(() -> {
                });
                this.queue = new ConcurrentLinkedQueue<>();
                this.semaphore = new AtomicInteger(concurrency);
            }

            CompletableFuture<List<DocumentWithRelevance>> search(Object query, long serviceTimeMs) {
                Task task = new Task(query, serviceTimeMs);
                queue.add(task);
                service.execute(this::drain);
                return task.future;
            }

            List<DocumentWithRelevance> searchImpl(Object query) {
                List<DocumentWithRelevance> result = new ArrayList<>(10);
                for (int i = 0; i < 3; ++i) {
                    result.add(new DocumentWithRelevance(
                            String.format("common-doc-%d", i),
                            ThreadLocalRandom.current().nextInt(100)));
                }
                for (int i = 0; i < 7; ++i) {
                    result.add(new DocumentWithRelevance(
                            String.format("shard-%d-replica-%d-doc-%d", shard, replica, i),
                            ThreadLocalRandom.current().nextInt(100)));
                }
                return result;
            }

            void drain() {
                if (semaphore.decrementAndGet() < 0) {
                    semaphore.incrementAndGet();
                    return; // No slots.
                }
                Task task = queue.poll();
                if (task == null) {
                    semaphore.incrementAndGet();
                    return; // No tasks.
                }
                service.schedule(() -> {
                    try {
                        task.future.complete(searchImpl(task.query));
                    } catch (Throwable ex) {
                        task.future.completeExceptionally(ex);
                    } finally {
                        semaphore.incrementAndGet();
                        service.execute(this::drain);
                    }
                }, task.serviceTimeMs, TimeUnit.MILLISECONDS);
            }

            void shutdown() {
                service.shutdown();
            }
        }

        private final double alpha;
        private final double mu;
        private final int shardCount;
        private final int replicasPerShard;
        private final ScheduledExecutorService scheduler;
        private final FakeServer[][] servers;

        private FakeBackend(double alpha, double mu, int shardCount, int replicasPerShard, int concurrency) {
            this.alpha = alpha;
            this.mu = mu;
            this.shardCount = shardCount;
            this.replicasPerShard = replicasPerShard;
            this.scheduler = Executors.newSingleThreadScheduledExecutor(new UniqueNamedThreadFactory("SCHEDULER"));
            this.servers = new FakeServer[shardCount][replicasPerShard];
            for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
                for (int replicaIndex = 0; replicaIndex < replicasPerShard; ++replicaIndex) {
                    servers[shardIndex][replicaIndex] = new FakeServer(shardIndex, replicaIndex, concurrency);
                }
            }
        }

        void shutdown() {
            scheduler.shutdown();
            for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
                for (int replicaIndex = 0; replicaIndex < replicasPerShard; ++replicaIndex) {
                    servers[shardIndex][replicaIndex].shutdown();
                }
            }
        }

        @Override
        public int getShardCount() {
            return shardCount;
        }

        @Override
        public int getReplicaCount(int shard) {
            return replicasPerShard;
        }

        double pareto(double alpha, double xM) {
            double v = 0;
            while (v == 0) {
                v = ThreadLocalRandom.current().nextDouble();
            }
            return xM / Math.pow(v, 1.0 / alpha);
        }

        double exp(double lambda) {
            return -lambda * Math.log(ThreadLocalRandom.current().nextDouble());
        }

        @Override
        public CompletableFuture<List<DocumentWithRelevance>> search(Object query, int shard, int replica) {
            if (shard < 0 || shard >= shardCount) {
                CompletableFuture<List<DocumentWithRelevance>> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("Shard index is out of range"));
                return future;
            }
            if (replica < 0 || replica >= replicasPerShard) {
                CompletableFuture<List<DocumentWithRelevance>> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("Replica index is out of range"));
                return future;
            }
            long serviceTimeMs;
            if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                serviceTimeMs = (long) pareto(alpha, 1.0 / mu);
            } else {
                serviceTimeMs = (long) exp(-1.0 / mu);
            }
            return servers[shard][replica].search(query, serviceTimeMs);
        }

        @Override
        public ScheduledExecutorService getScheduler() {
            return scheduler;
        }
    }

    // Threading.
    static class UniqueNamedThreadFactory implements ThreadFactory {
        private final String name;

        UniqueNamedThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(name);
            return thread;
        }
    }

    // Logging.

    private final static long STARTED_AT = System.currentTimeMillis();
    private final static AtomicBoolean LOG_DEBUG_IS_ENABLED = new AtomicBoolean(true);
    private final static AtomicBoolean LOG_INFO_IS_ENABLED = new AtomicBoolean(true);

    private static boolean setDebugLogEnabled(boolean value) {
        return LOG_DEBUG_IS_ENABLED.getAndSet(value);
    }

    private static boolean setInfoLogEnabled(boolean value) {
        return LOG_INFO_IS_ENABLED.getAndSet(value);
    }

    private static void logDebug(String format, Object... args) {
        if (LOG_DEBUG_IS_ENABLED.get()) {
            log('D', format, args);
        }
    }

    private static void logInfo(String format, Object... args) {
        if (LOG_INFO_IS_ENABLED.get()) {
            log('I', format, args);
        }
    }

    private static void log(char level, String format, Object... args) {
        long delta = System.currentTimeMillis() - STARTED_AT;
        String message = String.format(format, args);
        System.out.printf("%c [+%8dms] T[%-10s]: %s%n", level, delta, Thread.currentThread().getName(), message);
    }
}
