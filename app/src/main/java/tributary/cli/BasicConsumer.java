package tributary.cli;

import tributary.api.Consumer;
import tributary.api.Event;

public class BasicConsumer<T> extends Consumer<T> {

    public BasicConsumer(String id) {
        super(id);
    }

    @Override
    public void consume(Event<T> event) {
        System.out.println("Event id: " + event.getId() + ", Event content is: " + event.getValue());
    }

}
