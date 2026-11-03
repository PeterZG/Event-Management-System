package tributary;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import tributary.cli.TributaryCLI;

public class CLILifecycleTest {
    @Test
    public void actualCliSupportsLifecycleRebalancingPlaybackAndErrorRecovery() {
        String commands = String.join("\n",
                "create topic lifecycle Integer",
                "create partition lifecycle lifecycle-p1",
                "create partition lifecycle lifecycle-p2",
                "create consumer group lifecycle-group lifecycle range",
                "create consumer lifecycle-group lifecycle-c1",
                "create consumer lifecycle-group lifecycle-c2",
                "create producer lifecycle-producer Integer manual",
                "produce event lifecycle-producer lifecycle events/integer-example.json lifecycle-p1",
                "show topic lifecycle",
                "show consumer group lifecycle-group",
                "consume event lifecycle-c1 lifecycle-p1",
                "playback lifecycle-c1 lifecycle-p1 1",
                "set consumer group rebalancing lifecycle-group roundrobin",
                "set producer allocation lifecycle-producer random",
                "parallel produce (lifecycle-producer, lifecycle, events/integer-example.json)",
                "show producer lifecycle-producer",
                "show consumer lifecycle-c1",
                "produce event lifecycle-producer lifecycle events/integer-example.json",
                "delete partition lifecycle lifecycle-p2",
                "produce event lifecycle-producer lifecycle events/integer-example.json",
                "parallel consume (lifecycle-c1, lifecycle-p1)",
                "delete consumer lifecycle-c2",
                "delete consumer group lifecycle-group",
                "show consumer group lifecycle-group",
                "create consumer group lifecycle-group lifecycle range",
                "delete topic lifecycle",
                "show topic lifecycle",
                "delete producer lifecycle-producer",
                "show producer lifecycle-producer",
                "exit", "");
        InputStream originalIn = System.in;
        PrintStream originalOut = System.out;
        PrintStream originalError = System.err;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setIn(new ByteArrayInputStream(commands.getBytes(StandardCharsets.UTF_8)));
            System.setOut(capture);
            System.setErr(capture);
            TributaryCLI.main(new String[0]);
        } finally {
            System.setIn(originalIn);
            System.setOut(originalOut);
            System.setErr(originalError);
        }
        String transcript = output.toString(StandardCharsets.UTF_8);
        assertTrue(transcript.contains("Event content is: 42"), transcript);
        assertTrue(transcript.contains("allocation: RandomProducer"), transcript);
        assertTrue(transcript.contains("Partition removed and consumer groups rebalanced"), transcript);
        assertTrue(transcript.contains("Consumer group does not exist"), transcript);
        assertTrue(transcript.contains("Topic does not exist"), transcript);
        assertTrue(transcript.contains("Producer does not exist"), transcript);
        assertTrue(transcript.contains("Goodbye."), transcript);
        assertFalse(transcript.contains("Parallel operation failed"), transcript);
        assertFalse(transcript.contains("Exception\n"), transcript);
    }
}
