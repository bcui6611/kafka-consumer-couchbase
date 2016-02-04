    import com.fasterxml.jackson.databind.JsonNode;
    import com.fasterxml.jackson.databind.ObjectMapper;
    import com.fasterxml.jackson.databind.node.ArrayNode;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;

    import java.io.File;
    import java.nio.charset.Charset;
    import java.nio.file.Files;
    import java.nio.file.Paths;
    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.concurrent.Future;


    public class KafkaSimpleProducer {
        public static void main(String[] args) throws Exception {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);

            File input = new File("sampleJsonData.json");
            byte[] encoded = Files.readAllBytes(Paths.get(input.getPath()   ));


            String jsons =  new String(encoded, Charset.defaultCharset());
            System.out.println("Splitting file to jsons....");

            List<String> splittedJsons = split(jsons);

            System.out.println("Converting to JsonDocuments....");

            int docCount = splittedJsons.size();

            System.out.println("Number of documents is: " + docCount );

            System.out.println("Starting sending msg to kafka....");
            int count = 0;
            for ( String doc : splittedJsons) {
                System.out.println("sending msg...." + count);
                ProducerRecord<String,String> record = new ProducerRecord<>( "couchbaseTopic", doc );
                Future<RecordMetadata> meta = producer.send(record);
                System.out.println("msg sent...." + count);

                count++;
            }

            System.out.println("Total of " + count + " messages sent");

            producer.close();
        }
        public static List<String> split(String jsonArray) throws Exception {
            List<String> splittedJsonElements = new ArrayList<String>();
            ObjectMapper jsonMapper = new ObjectMapper();
            JsonNode jsonNode = jsonMapper.readTree(jsonArray);

            if (jsonNode.isArray()) {
                ArrayNode arrayNode = (ArrayNode) jsonNode;
                for (int i = 0; i < arrayNode.size(); i++) {
                    JsonNode individualElement = arrayNode.get(i);
                    splittedJsonElements.add(individualElement.toString());
                }
            }
            return splittedJsonElements;
        }
    }
