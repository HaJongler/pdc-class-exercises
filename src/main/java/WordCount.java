package main.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class WordCount {

    ArrayList<String> allWords = new ArrayList<>();
    Counter<String> wordsCounter = new Counter<>();
    Counter<String> pairsCounter = new Counter<>();
    ReentrantLock lock = new ReentrantLock();
    ExecutorService tpe = Executors.newFixedThreadPool(20);
    int counter = 0;
    final Object monitor = new Object();

    synchronized void addToCounter() { counter++; }
    synchronized void decreaseCntr() {
        counter--;
        if (counter == 0) {
            synchronized (monitor) {
                monitor.notify();
            }
        }}

    class FileParser implements Runnable {

        Path path;
        BufferedReader reader = null;
        ArrayList<String> words = new ArrayList<>();

        public FileParser(Path path) {
            this.path = path;
        }

        private void parseLine(String line) {
            // Split to words
            String[] words = line.split("\\s+");
            // Replace non-alpha characters
            for (int i = 0; i < words.length; i++) {
                words[i] = words[i].replaceAll("[^\\w]", "").toLowerCase();
            }
            this.words.addAll(Arrays.stream(words).filter(word -> word.length() > 0).collect(Collectors.toList()));
        }

        @Override
        public void run() {
            try {
                reader = Files.newBufferedReader(path);
                reader.lines().forEach(this::parseLine);
                lock.lock();
                allWords.addAll(this.words);
                lock.unlock();
                decreaseCntr();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void streamData() throws IOException {
        Path dataFolder = Paths.get("src/main/data/word_count");
        DirectoryStream<Path> stream = null;
        try {
            stream = Files.newDirectoryStream(dataFolder);
            stream.forEach(path -> {
                FileParser fp = new FileParser(path);
                tpe.execute(fp);
                addToCounter();
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Counter<T> {
        final Map<T, Integer> counts = new HashMap<>();

        public void add(T t) {
            counts.merge(t, 1, Integer::sum);
        }

        public int count(T t) {
            return counts.getOrDefault(t, 0);
        }
    }

    private void countWords() {
        this.allWords.forEach(wordsCounter::add);
    }

    private void countPairs() {
        for (int i = 0; i < allWords.size() - 1; i++) {
            String pair = allWords.get(i) + ";" + allWords.get(i + 1);
            if (wordsCounter.count(allWords.get(i)) > 5 && wordsCounter.count(allWords.get(i + 1)) > 5) this.pairsCounter.add(pair);
        }
    }

    private void getTopWords(int k) {
        System.out.println("Top " + k + " words are:");
        PriorityQueue<Object[]> priorityQueue = new PriorityQueue<>(k, (a, b) -> (int) b[1] - (int) a[1]);
        wordsCounter.counts.forEach((key, value) -> {
            Object[] tup = {key, value};
            priorityQueue.add(tup);
        });
        for (int i = 0; i < k; i++) {
            Object[] tup = priorityQueue.poll();
            System.out.println(tup[0] + ":     " + tup[1]);
        }
    }

    private void getTopPairs(int k) {
        double logn = Math.log(allWords.size());
        PriorityQueue<Object[]> priorityQueue = new PriorityQueue<>(k, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                double o1d = (double) o1[1], o2d = (double) o2[1];
                if (Math.abs(o1d - o2d) < 1e-5) return 0;
                if (o1d < o2d) return 1;
                else return -1;
            }
        });
        pairsCounter.counts.forEach((key, value) -> {
            if (value < 10) return;
            String[] words = key.split(";");
            double npmi = -1 +
                    ((Math.log(wordsCounter.count(words[0])) - logn) / (Math.log(value) - logn)) +
                    ((Math.log(wordsCounter.count(words[1])) - logn) / (Math.log(value) - logn));
            Object[] tup = {key, npmi};
            priorityQueue.add(tup);
        });
        for (int i = 0; i < k; i++) {
            Object[] tup = priorityQueue.poll();
            System.out.println(tup[0] + ":     " + tup[1]);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        WordCount wc = new WordCount();
        int k = 100;
        long startTime = System.nanoTime();
        wc.streamData();
        while (wc.counter > 0) {
            synchronized (wc.monitor) {
            wc.monitor.wait();}
        }
        System.out.println("# words: " + wc.allWords.size());
        System.out.println("# unique words: " + wc.allWords.stream().distinct().count());
        System.out.println("============");
        wc.countWords();
        wc.countPairs();
        wc.getTopWords(k);
        System.out.println("============");
        wc.getTopPairs(k);
        System.out.println("Done in " + (System.nanoTime() - startTime) / Math.pow(10, 9) + " seconds");
    }
}
