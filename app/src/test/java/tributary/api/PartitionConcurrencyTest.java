package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class PartitionConcurrencyTest {
    @Test
    public void emptyPopDoesNotCreatePlayedEvent() {
        Partition<String> partition = new Partition<>("p1");

        assertNull(partition.popEvent());
        assertEquals(0, partition.getPlayedEvents().size());
    }

    @Test
    public void concurrentPushAndPopPreserveEveryEvent() throws Exception {
        Partition<Integer> partition = new Partition<>("p1");
        int workerCount = 8;
        int eventsPerWorker = 250;
        ExecutorService producers = Executors.newFixedThreadPool(workerCount);
        List<Callable<Void>> producerTasks = new ArrayList<>();

        for (int worker = 0; worker < workerCount; worker++) {
            int workerId = worker;
            producerTasks.add(() -> {
                for (int index = 0; index < eventsPerWorker; index++) {
                    int eventId = workerId * eventsPerWorker + index;
                    partition.pushEvent(new Event<>("e" + eventId, null, eventId));
                }
                return null;
            });
        }
        for (Future<Void> result : producers.invokeAll(producerTasks)) {
            result.get();
        }
        producers.shutdown();
        assertEquals(workerCount * eventsPerWorker, partition.size());

        AtomicInteger consumed = new AtomicInteger();
        ExecutorService consumers = Executors.newFixedThreadPool(workerCount);
        List<Callable<Void>> consumerTasks = new ArrayList<>();
        for (int worker = 0; worker < workerCount; worker++) {
            consumerTasks.add(() -> {
                while (partition.popEvent() != null) {
                    consumed.incrementAndGet();
                }
                return null;
            });
        }
        for (Future<Void> result : consumers.invokeAll(consumerTasks)) {
            result.get();
        }
        consumers.shutdown();

        List<Event<Integer>> played = partition.getPlayedEvents();
        assertEquals(workerCount * eventsPerWorker, consumed.get());
        assertEquals(consumed.get(), played.size());
        assertTrue(played.stream().allMatch(Objects::nonNull));
        assertThrows(UnsupportedOperationException.class, played::clear);
    }
}
