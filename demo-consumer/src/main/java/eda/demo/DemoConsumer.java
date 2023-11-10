package eda.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DemoConsumer 
{
    public static void main( String[] args )
    {

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final List<ConsumerLoop> consumers = new ArrayList<>();

        ConsumerLoop consumer = new ConsumerLoop();
        consumers.add(consumer);
        executor.submit(consumer);

        Runtime.getRuntime().addShutdownHook(new Thread() { 
            @Override
            public void run() {
                for (ConsumerLoop consumer: consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
    }
}
