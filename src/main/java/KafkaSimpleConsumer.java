import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.*;


public class KafkaSimpleConsumer {
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put("zookeeper.connect", "localhost:2181"); // Zookeeper address
        config.put("zookeeper.connectiontimeout.ms", "10000");
        config.put("group.id", "default");

        ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(config);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("couchbaseTopic", 1);  // Topic to track

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("couchbaseTopic");

        List<String> nodes = new ArrayList<>();
        nodes.add("localhost");

        Cluster cluster = CouchbaseCluster.create(nodes);
        final Bucket bucket = cluster.openBucket("kafkaExample");

        try { // Consuming messages
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                for (MessageAndMetadata<byte[], byte[]> msgAndMetaData : stream) {
                    String msg = convertPayloadToString(msgAndMetaData.message());
                    System.out.println(msgAndMetaData.topic() + ": " + msg);

                    try {
                        JsonObject doc = JsonObject.fromJson(msg);
                        String id = UUID.randomUUID().toString();
                        bucket.upsert(JsonDocument.create(id, doc));
                    } catch (Exception ex) {
                        System.out.println("Not a json object: " + ex.getMessage());
                    }

                }

            }
        } catch (Exception ex) {
            System.out.println("EXCEPTION!!!!" + ex.getMessage());
            cluster.disconnect();

        }

        cluster.disconnect();
    }

    private static String convertPayloadToString(final byte[] message) {
        String string = new String(message);
        return string;
    }

}
