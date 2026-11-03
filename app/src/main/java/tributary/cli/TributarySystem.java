package tributary.cli;

import java.io.File;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.Event;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.ProducerFactory;
import tributary.api.Topic;

public class TributarySystem {
    private final Cluster cluster;
    private final Map<String, String> topicTypes = new HashMap<>();
    private final Map<String, String> producerTypes = new HashMap<>();

    private static TributarySystem instance;

    private TributarySystem() {
        cluster = new Cluster();
    }

    public static synchronized TributarySystem getInstance() {
        if (instance == null) {
            instance = new TributarySystem();
        }
        return instance;
    }

    private void checkType(String type) {
        if (!(type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("String"))) {
            throw new IllegalArgumentException("Not supported type: " + type);
        }
    }

    private void checkFileExists(String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            throw new IllegalArgumentException("File does not exist. " + filename);
        }
    }

    public void createTopic(String id, String type) {
        checkType(type);
        if (cluster.getTopic(id) != null) {
            throw new IllegalArgumentException("Topic with id '" + id + "' already exists.");
        }

        if (type.equalsIgnoreCase("String")) {
            cluster.addTopic(new Topic<String>(id));
        } else if (type.equalsIgnoreCase("Integer")) {
            cluster.addTopic(new Topic<Integer>(id));
        }
        System.out.println("Topic '" + id + "' for type '" + type + "' created.");
        topicTypes.put(id, type);
    }

    public void createPartition(String topicId, String partitionId) {
        cluster.checkTopic(topicId);
        cluster.createPartition(topicId, partitionId);
        System.out.println("Partition created. id: " + partitionId);
    }

    public void createConsumerGroup(String groupId, String topicId, String rebalancing) {
        cluster.createConsumerGroup(topicId, groupId, rebalancing);
        System.out.println("Consumer group created. id: " + groupId);
    }

    public void createConsumer(String groupId, String consumerId) {
        ConsumerGroup group = cluster.getConsumerGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + groupId);
        }
        if (cluster.getGroupOfConsumer(consumerId) != null) {
            throw new IllegalArgumentException("Consumer exists. id: " + consumerId);
        }
        Constructor<BasicConsumer> c;
        try {
            c = BasicConsumer.class.getConstructor(String.class);
            group.createConsumer(consumerId, c);
            System.out.println("Consumer created. id: " + consumerId);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Unable to create consumer " + consumerId, e);
        }
    }

    public void deleteConsumer(String id) {
        ConsumerGroup group = cluster.getGroupOfConsumer(id);
        if (group == null) {
            throw new IllegalArgumentException("Consumer does not exist. id: " + id);
        } else {
            group.removeConsumer(id);
            System.out.println("Consumer removed. id: " + id);
            System.out.println("Consumer group rebalanced. id: " + group.getId());
        }
    }

    public void createProducer(String id, String type, String allocation) {
        checkType(type);
        if (cluster.getProducer(id) != null) {
            throw new IllegalArgumentException("Producer already exists. id: " + id);
        }

        if (type.equalsIgnoreCase("String")) {
            ProducerFactory<String> factory = new ProducerFactory<>();
            cluster.addProducer(factory.create(id, allocation));
        } else if (type.equalsIgnoreCase("Integer")) {
            ProducerFactory<Integer> factory = new ProducerFactory<>();
            cluster.addProducer(factory.create(id, allocation));
        }
        System.out.println("Producer created. id: " + id);
        producerTypes.put(id, type);
    }

    public void deleteProducer(String id) {
        Producer producer = cluster.removeProducer(id);
        if (producer == null) {
            throw new IllegalArgumentException("Producer does not exist. id: " + id);
        } else {
            System.out.println("Producer removed. id: " + id);
            producerTypes.remove(id);
        }
    }

    public void deleteTopic(String id) {
        if (cluster.removeTopic(id) == null) {
            throw new IllegalArgumentException("Topic does not exist. id: " + id);
        }
        topicTypes.remove(id);
        System.out.println("Topic and its consumer groups removed. id: " + id);
    }

    public void deleteConsumerGroup(String id) {
        if (!cluster.removeConsumerGroup(id)) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + id);
        }
        System.out.println("Consumer group removed. id: " + id);
    }

    public void deletePartition(String topicId, String partitionId) {
        if (!cluster.checkTopic(topicId).removePartition(partitionId)) {
            throw new IllegalArgumentException("Partition does not exist. id: " + partitionId);
        }
        System.out.println("Partition removed and consumer groups rebalanced. id: " + partitionId);
    }

    public void setProducerAllocation(String id, String allocation) {
        cluster.checkProducer(id);
        cluster.addProducer(new ProducerFactory<>().create(id, allocation));
        System.out.println("Producer allocation changed. id: " + id + ", allocation: " + allocation);
    }

    public void showProducer(String id) {
        Producer producer = cluster.checkProducer(id);
        System.out.println("Producer id: " + producer.getId() + ", type: " + producerTypes.get(id)
                + ", allocation: " + producer.getClass().getSimpleName());
    }

    public void showConsumer(String id) {
        System.out.println(cluster.checkConsumer(id).getInfo(0));
    }

    public void produceEvent(String producerId, String topicId, String eventFile, String partitionId) {
        Producer producer = cluster.checkProducer(producerId);
        Topic topic = cluster.checkTopic(topicId);
        if (!producerTypes.get(producerId).equalsIgnoreCase(topicTypes.get(topicId))) {
            throw new IllegalArgumentException("Producer and topic payload types must match");
        }
        checkFileExists(eventFile);
        if (partitionId != null && topic.getPartition(partitionId) == null) {
            throw new IllegalArgumentException("Partition does not exist. id: " + partitionId);
        }

        JSONObject eventJson = null;
        String eventId = null;
        Object value = null;
        try {
            eventJson = new JSONObject(Files.readString(Paths.get(eventFile), StandardCharsets.UTF_8));
            eventId = eventJson.getString("id");
            value = "Integer".equalsIgnoreCase(producerTypes.get(producerId))
                    ? integerPayload(eventJson.get("value")) : eventJson.getString("value");
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to read event JSON: " + e.getMessage(), e);
        }

        Event event = producer.produce(eventId, partitionId, value);
        String allocated = producer.allocate(event, topic);

        System.out.println("Event (id " + event.getId() + ") allocated to partition (id: " + allocated + ")");
    }

    private int integerPayload(Object value) {
        if (value instanceof Number) {
            try {
                // JSONObject.getInt truncates fractions and wraps oversized values.
                return new BigDecimal(value.toString()).intValueExact();
            } catch (ArithmeticException | NumberFormatException exception) {
                throw new IllegalArgumentException("Integer payload must be a whole 32-bit JSON number", exception);
            }
        }
        throw new IllegalArgumentException("Integer payload must be a whole 32-bit JSON number");
    }

    public void consumeEvent(String consumerId, String partitionId) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        checkAssignment(consumer, partition);
        Event event = partition.popEvent();
        if (event != null) {
            consumer.consume(event);
        } else {
            throw new IllegalArgumentException("Partition is empty. id: " + partitionId);
        }
    }

    public void consumeEvents(String consumerId, String partitionId, int number) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        checkAssignment(consumer, partition);
        if (number < 0) {
            throw new IllegalArgumentException("Number of events must not be negative");
        }
        while (number > 0) {
            Event event = partition.popEvent();
            if (event == null) {
                break;
            }
            consumer.consume(event);
            number--;
        }
    }

    private void checkAssignment(Consumer consumer, Partition partition) {
        if (!consumer.getPartitions().contains(partition)) {
            throw new IllegalArgumentException("Partition is not assigned to consumer " + consumer.getId());
        }
    }

    public void showTopic(String id) {
        Topic topic = cluster.checkTopic(id);
        System.out.println(topic.getInfo(0));
    }

    public void showConsumerGroup(String id) {
        ConsumerGroup group = cluster.getConsumerGroup(id);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + id);
        }
        System.out.println(group.getInfo(0));
    }

    public void parallelProduce(String command) {
        Pattern pattern = Pattern.compile("^.*?((\\(.*?,.*?,.*?\\))(\\s*,\\s*)?)+$");
        Pattern paramPattern = Pattern.compile("\\(.*?,.*?,.*?\\)");
        List<String[]> params = parseParams(command, pattern, paramPattern);
        for (String[] strs : params) {
            cluster.checkProducer(strs[0]);
            cluster.checkTopic(strs[1]);
            checkFileExists(strs[2]);
        }
        List<Callable<Void>> tasks = new ArrayList<>();
        for (String[] strs : params) {
            tasks.add(() -> {
                produceEvent(strs[0], strs[1], strs[2], null);
                return null;
            });
        }
        runParallel(tasks);
    }

    public void parallelConsume(String command) {
        Pattern pattern = Pattern.compile("^.*?((\\(.*?,.*?\\))(\\s*,\\s*)?)+$");
        Pattern paramPattern = Pattern.compile("\\(.*?,.*?\\)");
        List<String[]> params = parseParams(command, pattern, paramPattern);
        for (String[] strs : params) {
            cluster.checkConsumer(strs[0]);
            cluster.checkPartition(strs[1]);
        }
        List<Callable<Void>> tasks = new ArrayList<>();
        for (String[] strs : params) {
            tasks.add(() -> {
                consumeEvent(strs[0], strs[1]);
                return null;
            });
        }
        runParallel(tasks);

    }

    private List<String[]> parseParams(String command, Pattern pattern, Pattern paramPattern) {
        if (!pattern.matcher(command).find()) {
            throw new IllegalArgumentException();
        }
        List<String[]> params = new ArrayList<>();
        Matcher paramMatcher = paramPattern.matcher(command);
        while (paramMatcher.find()) {
            String str = paramMatcher.group();
            str = str.trim().substring(1, str.length() - 1);
            params.add(str.replace(" ", "").split(","));
        }
        return params;
    }

    private void runParallel(List<Callable<Void>> tasks) {
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(tasks.size(), 8));
        try {
            List<Future<Void>> results = executor.invokeAll(tasks);
            for (Future<Void> result : results) {
                result.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Parallel operation interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("Parallel operation failed: " + e.getCause().getMessage(), e.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    public void setConsumerGroupRebalancing(String groupId, String rebalancing) {
        ConsumerGroup group = cluster.getConsumerGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + groupId);
        }
        group.setRebalancing(rebalancing);
        System.out.println("Consumer group set rebalancing strategy to " + rebalancing);
    }

    public void playback(String consumerId, String partitionId, int offset) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        checkAssignment(consumer, partition);
        List<Event> played = partition.getPlayedEvents();
        if (offset < 1 || offset > played.size()) {
            throw new IllegalArgumentException("Invalid offset, max played offset is " + played.size());
        }
        for (int i = offset - 1; i < played.size(); i++) {
            consumer.consume(played.get(i));
        }
    }

}
