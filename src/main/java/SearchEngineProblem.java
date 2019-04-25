package main.java;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SearchEngineProblem {
    static class DocumentWithRelevance {
        private final String documentId;
        private final int relevance;

        DocumentWithRelevance(String documentId, int relevance) {
            this.documentId = documentId;
            this.relevance = relevance;
        }

        public String getDocumentId() {
            return documentId;
        }

        public int getRelevance() {
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
    // TODO: This is the class you should implement.
    ////////////////////////////////////////////////////////////////////////////////
    static class SearchEngine {
        private final Backend backend;

        SearchEngine(Backend backend) {
            this.backend = backend;
        }

        // Return a list of top K documents sorted by relevance (descending).
        // Returned documents must be unique.
        CompletableFuture<List<DocumentWithRelevance>> search(Object query, int limit) throws ExecutionException, InterruptedException {
            List<CompletableFuture<List<DocumentWithRelevance>>> futures = new ArrayList<>();
            // Get results from all replicas
            for (int shard = 0; shard < backend.getShardCount(); shard++) {
                for (int replica = 0; replica < backend.getReplicaCount(shard); replica++) {
                    CompletableFuture<List<DocumentWithRelevance>> result = backend.search(query, shard, replica);
                    futures.add(result);
                }
            }
            CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            CompletableFuture<List<List<DocumentWithRelevance>>> allCombo =
                    allFutures.thenApply(future ->
                            futures.stream().map(CompletableFuture::join).collect(Collectors.toList())
                    );

            List<DocumentWithRelevance> queriesResults = new ArrayList<>();
            queriesResults.addAll(allCombo.get().stream().flatMap(List::stream).collect(Collectors.toList()));
            queriesResults.sort(Comparator.comparingInt(DocumentWithRelevance::getRelevance).reversed());
            return CompletableFuture.completedFuture(queriesResults.subList(0, limit));
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

        FakeBackend backend = new FakeBackend(mu, 5, 3);
        SearchEngine engine = new SearchEngine(backend);

        long[] requestTimesNs = new long[requestCount];
        List<CompletableFuture<List<DocumentWithRelevance>>> futures = new ArrayList<>(requestCount);
        for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
            final int finalRequestIndex = requestIndex;
            Thread.sleep(arrivalTimesMs[requestIndex] - (requestIndex > 0 ? arrivalTimesMs[requestIndex - 1] : 0));
            String query = String.format("query-%d", ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
            long startedAt = System.nanoTime();
            logDebug("Searching for '%s' (request %d)...", query, finalRequestIndex);
            futures.add(finalRequestIndex, engine.search(query, k).whenComplete((strings, throwable) -> {
                if (throwable != null) {
                    logInfo("Request %d for query '%s' has failed: %s", finalRequestIndex, query, throwable.toString());
                } else {
                    long completedAt = System.nanoTime();
                    requestTimesNs[finalRequestIndex] = completedAt - startedAt;
                }
            }));
        }
        logInfo("Waiting for all requests to complete...");
        futures.forEach(CompletableFuture::join);
        logInfo("All requests are completed.");
        boolean ok = true;
        for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
            List<DocumentWithRelevance> documents = futures.get(requestIndex).get();
            if (documents.size() != k) {
                logInfo("WRONG: Request %d has returned %d documents, while expecting %d documents!",
                        requestIndex, documents.size(), k);
                ok = false;
            }
            for (int documentIndex = 1; documentIndex < documents.size(); ++documentIndex) {
                DocumentWithRelevance current = documents.get(documentIndex);
                DocumentWithRelevance previous = documents.get(documentIndex - 1);
                if (current.getRelevance() > previous.getRelevance()) {
                    logInfo("WRONG: Request %d has returned documents %d and %d in the wrong order!",
                            requestIndex, documentIndex - 1, documentIndex);
                    logInfo("WRONG:     D[%d] = { DocumentId: '%s', Relevance: %d }",
                            documentIndex - 1, previous.getDocumentId(), previous.getRelevance());
                    logInfo("WRONG:     D[%d] = { DocumentId: '%s', Relevance: %d }",
                            documentIndex, current.getDocumentId(), current.getRelevance());
                    ok = false;
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
            double t = (double) requestTimesNs[i] / 1_000_000_000.0;
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
        logInfo("MIN: %8.3fs | MAX: %8.3fs | MEAN: %8.3fs +- %8.3fs", min, max, mean, stddev);
        logInfo("Q50: %8.3fs | Q90: %8.3fs |  Q99: %8.3fs", q50, q90, q99);

        backend.shutdown();
    }

    ////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////
    // Internals.
    ////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    static class FakeBackend implements Backend {
        private final double mu;
        private final int shardCount;
        private final int replicasPerShard;
        private final ScheduledExecutorService scheduler;
        private final ScheduledExecutorService[][] services;

        FakeBackend(double mu, int shardCount, int replicasPerShard) {
            this.mu = mu;
            this.shardCount = shardCount;
            this.replicasPerShard = replicasPerShard;
            this.scheduler = Executors.newSingleThreadScheduledExecutor(new UniqueNamedThreadFactory("SCHEDULER"));
            this.services = new ScheduledExecutorService[shardCount][replicasPerShard];
            for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
                for (int replicaIndex = 0; replicaIndex < replicasPerShard; ++replicaIndex) {
                    services[shardIndex][replicaIndex] = Executors.newSingleThreadScheduledExecutor(
                            new UniqueNamedThreadFactory(String.format("BASE-Shard%d-Replica%d", shardIndex, replicaIndex))
                    );
                    services[shardIndex][replicaIndex].submit(() -> {
                    }); // Warmup.
                }
            }
        }

        void shutdown() {
            scheduler.shutdown();
            for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
                for (int replicaIndex = 0; replicaIndex < replicasPerShard; ++replicaIndex) {
                    services[shardIndex][replicaIndex].shutdown();
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
            CompletableFuture<List<DocumentWithRelevance>> future = new CompletableFuture<>();
            long serviceTimeMs;
            if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                serviceTimeMs = (long) pareto(4.0, 1.0 / mu);
            } else {
                serviceTimeMs = (long) exp(-1.0 / mu);
            }
            services[shard][replica].schedule(() -> {
                try {
                    future.complete(queryImpl(query, shard, replica));
                } catch (Throwable ex) {
                    future.completeExceptionally(ex);
                }
            }, serviceTimeMs, TimeUnit.MILLISECONDS);
            return future;
        }

        List<DocumentWithRelevance> queryImpl(Object query, int shard, int replica) {
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
