package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

public class Main {

    // Parallel job
    /*public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        executorService.submit(() -> runPipeline(hz));
        // just wait for job submission
        executorService.shutdown();
        executorService.awaitTermination(5_000, TimeUnit.MILLISECONDS);
    }

    private static void runPipeline(HazelcastInstance hz) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .map(t -> t)
                .filter(event -> event.sequence() % 2 == 0)
                .setName("filter out odd numbers")
                .writeTo(Sinks.logger());
        hz.getJet().newJob(pipeline);
    }
    */
    // Simple job
    /* Simple
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .map(t -> t)
                .filter(event -> event.sequence() % 2 == 0)
                .setName("filter out odd numbers")
                .writeTo(Sinks.logger());

        HazelcastInstance hz = Hazelcast.bootstrappedInstance();

        hz.getJet().newJob(pipeline);
    }*/

    // Exception job
    /*
    public static void main(String[] args) {
        throw new RuntimeException("Everything's OK");

    }*/

    // Exception job
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .map(t -> t)
                .filter(event -> event.sequence() % 2 == 0)
                .setName("filter out odd numbers")
                .writeTo(Sinks.logger());

        HazelcastInstance hz = Hazelcast.bootstrappedInstance();

        hz.getJet().newJob(pipeline).join();

    }
}