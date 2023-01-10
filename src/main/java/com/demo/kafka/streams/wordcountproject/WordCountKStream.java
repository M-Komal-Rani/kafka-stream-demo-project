package com.demo.kafka.streams.wordcountproject;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class WordCountKStream {

    @Autowired
    private KStreamConfig kStreamConfig;

    @PostConstruct
    public void wordCountStreamsBuilder() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
//        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input-topic");
//        KTable<String, Long> result = wordCountInput.peek((k,v) -> System.out.println(k + " " + v))
//                 .mapValues(value -> value.toLowerCase())
//                .flatMapValues(values -> Arrays.asList(values.split(" ")))
//                .selectKey((x, y) -> y)
//                .groupByKey()
//                .count(Named.as("COUNTS"));

                KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input-topic");
        wordCountInput.peek((k,v) -> System.out.println(k + " " + v))
            .mapValues(value -> value.toLowerCase())
            .flatMapValues(values -> Arrays.asList(values.split(" ")))
            .selectKey((x, y) -> "567")
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(
                Duration.ofMinutes(1), Duration.ZERO))
            .count(materializedAsWindowStore("test", Serdes.String(), Serdes.Long()))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                .withName("test"))
            .toStream()
            .foreach((k,v) -> System.out.println("Windowed Result : " + k.window().start() + " " + System.currentTimeMillis() + " " + v));

        //result.toStream().to("word-count-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, kStreamConfig.getKStreamProperties());
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
        String storeName, Serde<K> keySerde, Serde<V> valueSerde) {
        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName,
                Duration.ofMinutes(1),
                Duration.ofMinutes(1), true))
            .withKeySerde(keySerde).withValueSerde(valueSerde)
            .withLoggingDisabled()
            .withRetention(Duration.ofMinutes(10));
    }
}
