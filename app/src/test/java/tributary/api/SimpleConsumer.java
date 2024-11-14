package tributary.api;

public class SimpleConsumer<T> extends Consumer<T> {

    public SimpleConsumer(String id) {
        super(id);
    }

    @Override
    public void consume(Event<T> event) {
    }

}
