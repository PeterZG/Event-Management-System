package tributary.cli;

import java.util.Scanner;

public class TributaryCLI {
    private Scanner scanner;
    private TributarySystem system;

    public static void main(String[] args) {
        TributaryCLI cli = new TributaryCLI();
        cli.start();
    }

    public TributaryCLI() {
        scanner = new Scanner(System.in);
        system = TributarySystem.getInstance();
    }

    private void start() {
        System.out.println("==========  Tributary System  ==========");
        System.out.println("Enter h/help for help.");
        while (true) {
            System.out.println("\nEnter your command:");
            if (!scanner.hasNextLine()) {
                return;
            }
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) {
                continue;
            }

            try {
                if (line.equals("exit")) {
                    System.out.println("Goodbye.");
                    return;
                }

                String[] strs = line.split("\\s+");
                if (specialCommands(strs)) {
                    continue;
                }
                switch (strs[0]) {
                    case "create":
                        createCommands(strs);
                        break;
                    case "delete":
                        deleteCommands(strs);
                        break;
                    case "produce":
                        produceCommands(strs);
                        break;
                    case "consume":
                        consumeCommands(strs);
                        break;
                    case "show":
                        showCommands(strs);
                        break;
                    case "parallel":
                        parallelCommands(strs, line);
                        break;
                    case "set":
                        setCommands(strs);
                        break;
                    case "playback":
                        playbackCommands(strs);
                        break;
                    default:
                        System.out.println("Invalid command.");
                }
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid command: " + line);
                System.err.println(e.getMessage() == null ? "" : e.getMessage());
            } catch (Exception e) {
                System.err.println("Command failed: " + e.getMessage());
            }
        }
    }

    private boolean specialCommands(String[] strs) {
        if (strs[0].equals("h") || strs[0].equals("help")) {
            printHelp();
            return true;
        }
        if (strs.length < 3) {
            System.out.println("Invalid command. Enter h/help for help.");
            return true;
        }
        return false;
    }

    private void printHelp() {
        String[] helps = new String[] {
                "help: show commands.  exit: close the simulator.",
                "Replace <...> with your own values; omit the angle brackets.",
                "",
                "Set up a message flow",
                "create topic <id> <type>",
                "create partition <topic> <id>",
                "create consumer group <id> <topic> <rebalancing>",
                "create consumer <group> <id>",
                "create producer <id> <type> <allocation>",
                "",
                "Send and receive",
                "produce event <producer> <topic> <event> <partition>",
                "consume event <consumer> <partition>",
                "consume events <consumer> <partition> <number of events>",
                "parallel produce (<producer>, <topic>, <event>), ...",
                "parallel consume (<consumer>, <partition>)",
                "",
                "Inspect and configure",
                "show topic <topic>",
                "show consumer group <group>",
                "show consumer <id>",
                "show producer <id>",
                "set consumer group rebalancing <group> <rebalancing>",
                "set producer allocation <producer> <manual|random>",
                "playback <consumer> <partition> <offset>",
                "",
                "Remove resources",
                "delete consumer <consumer>",
                "delete consumer group <group>",
                "delete producer <producer>",
                "delete topic <id> (also removes its consumer groups)",
                "delete partition <topic> <id> (discards its queued events)",
                ""
        };
        for (String help : helps) {
            System.out.println(help);
        }
    }

    private void checkArgCount(String[] args, int count) {
        if (args.length < count) {
            throw new IllegalArgumentException("Error argument count.");
        }
    }

    private void createCommands(String[] strs) {
        checkArgCount(strs, 4);
        switch (strs[1]) {
            case "topic":
                system.createTopic(strs[2], strs[3]);
                break;
            case "partition":
                system.createPartition(strs[2], strs[3]);
                break;
            case "consumer":
                if ("group".equals(strs[2])) {
                    checkArgCount(strs, 6);
                    system.createConsumerGroup(strs[3], strs[4], strs[5]);
                } else {
                    system.createConsumer(strs[2], strs[3]);
                }
                break;
            case "producer":
                checkArgCount(strs, 5);
                system.createProducer(strs[2], strs[3], strs[4]);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void deleteCommands(String[] strs) {
        switch (strs[1]) {
            case "consumer":
                if ("group".equals(strs[2])) {
                    checkArgCount(strs, 4);
                    system.deleteConsumerGroup(strs[3]);
                } else {
                    system.deleteConsumer(strs[2]);
                }
                break;
            case "producer":
                system.deleteProducer(strs[2]);
                break;
            case "topic":
                system.deleteTopic(strs[2]);
                break;
            case "partition":
                checkArgCount(strs, 4);
                system.deletePartition(strs[2], strs[3]);
                break;
            default:
                throw new IllegalArgumentException("Unknown entity type: " + strs[1]);
        }
    }

    private void produceCommands(String[] strs) {
        checkArgCount(strs, 5);
        if (!"event".equals(strs[1])) {
            throw new IllegalArgumentException("Use produce event");
        }
        String partitionId = strs.length > 5 ? strs[5] : null;
        system.produceEvent(strs[2], strs[3], strs[4], partitionId);
    }

    private void consumeCommands(String[] strs) {
        checkArgCount(strs, 4);
        switch (strs[1]) {
            case "event":
                system.consumeEvent(strs[2], strs[3]);
                break;
            case "events":
                checkArgCount(strs, 5);
                system.consumeEvents(strs[2], strs[3], Integer.parseInt(strs[4]));
                break;
            default:
                throw new IllegalArgumentException("Use consume event or consume events");
        }
    }

    private void showCommands(String[] strs) {
        switch (strs[1]) {
            case "topic":
                system.showTopic(strs[2]);
                break;
            case "consumer":
                if ("group".equals(strs[2])) {
                    checkArgCount(strs, 4);
                    system.showConsumerGroup(strs[3]);
                } else {
                    system.showConsumer(strs[2]);
                }
                break;
            case "producer":
                system.showProducer(strs[2]);
                break;
            default:
                throw new IllegalArgumentException("Unknown entity type: " + strs[1]);
        }
    }

    private void parallelCommands(String[] strs, String line) {
        switch (strs[1]) {
            case "produce":
                system.parallelProduce(line);
                break;
            case "consume":
                system.parallelConsume(line);
                break;
            default:
                throw new IllegalArgumentException("Use parallel produce or parallel consume");
        }
    }

    private void setCommands(String[] strs) {
        if (strs[1].equals("producer") && strs[2].equals("allocation")) {
            checkArgCount(strs, 5);
            system.setProducerAllocation(strs[3], strs[4]);
            return;
        }
        checkArgCount(strs, 6);
        if (!strs[1].equals("consumer")
                || !strs[2].equals("group")
                || !strs[3].equals("rebalancing")) {
            throw new IllegalArgumentException();
        }
        system.setConsumerGroupRebalancing(strs[4], strs[5]);
    }

    private void playbackCommands(String[] strs) {
        checkArgCount(strs, 4);
        system.playback(strs[1], strs[2], Integer.parseInt(strs[3]));
    }

}
