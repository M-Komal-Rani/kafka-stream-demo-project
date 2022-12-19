package com.demo.kafka.streams.wordcountproject;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
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
        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input-topic");
        KTable<String, Long> result = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(values -> Arrays.asList(values.split(" ")))
                .selectKey((x, y) -> y)
                .groupByKey()
                .count(Named.as("COUNTS"));

        result.toStream().to("word-count-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, kStreamConfig.getKStreamProperties());
        kafkaStreams.start();
    }
}
