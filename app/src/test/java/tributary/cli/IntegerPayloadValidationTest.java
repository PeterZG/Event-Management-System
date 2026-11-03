package tributary.cli;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class IntegerPayloadValidationTest {
    @TempDir
    private Path temporaryDirectory;

    @Test
    public void integerProducerRejectsLossyOrCoercedPayloadsBeforePublishing() throws IOException {
        TributarySystem system = TributarySystem.getInstance();
        String suffix = UUID.randomUUID().toString();
        String topicId = "integer-topic-" + suffix;
        String partitionId = "integer-partition-" + suffix;
        String producerId = "integer-producer-" + suffix;
        String consumerId = "integer-consumer-" + suffix;
        Path eventFile = temporaryDirectory.resolve("integer-payload.json");
        system.createTopic(topicId, "Integer");
        system.createPartition(topicId, partitionId);
        system.createConsumerGroup("integer-group-" + suffix, topicId, "range");
        system.createConsumer("integer-group-" + suffix, consumerId);
        system.createProducer(producerId, "Integer", "manual");
        try {
            for (String invalid : new String[] {"1.5", "2147483648", "-2147483649", "\"42\"", "true", "null"}) {
                Files.writeString(eventFile, "{\"id\":\"invalid\",\"value\":" + invalid + "}", StandardCharsets.UTF_8);
                assertThrows(IllegalArgumentException.class,
                        () -> system.produceEvent(producerId, topicId, eventFile.toString(), partitionId), invalid);
            }
            assertThrows(IllegalArgumentException.class, () -> system.consumeEvent(consumerId, partitionId));
            for (String valid : new String[] {"-2147483648", "2147483647", "42.0"}) {
                Files.writeString(eventFile, "{\"id\":\"valid\",\"value\":" + valid + "}", StandardCharsets.UTF_8);
                system.produceEvent(producerId, topicId, eventFile.toString(), partitionId);
                system.consumeEvent(consumerId, partitionId);
            }
        } finally {
            system.deleteTopic(topicId);
            system.deleteProducer(producerId);
        }
    }
}
