package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

public class Main {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .filter(event -> event.sequence() % 2 == 0)
                .setName("filter out odd numbers")
                .writeTo(Sinks.logger());

        HazelcastInstance hz = Hazelcast.bootstrappedInstance();

        hz.getJet().newJob(pipeline);
    }
}