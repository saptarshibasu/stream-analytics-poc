package com.sensor;

import com.azure.messaging.eventhubs.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Sensor {
    private static final String connectionString = "";
    private static final String eventHubName = "eh-poc";

    public static void main(String[] args) throws IOException, InterruptedException {
        processEvents();
    }

    /**
     * Code sample for publishing events.
     * @throws IllegalArgumentException if the EventData is bigger than the max batch size.
     */
    private static void publishEvents(String event) {
        // create a producer client
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();

        // sample events in an array
        List<EventData> allEvents = Arrays.asList(new EventData(event));

        // create a batch
        EventDataBatch eventDataBatch = producer.createBatch();

        for (EventData eventData : allEvents) {
            // try to add the event from the array to the batch
            if (!eventDataBatch.tryAdd(eventData)) {
                // if the batch is full, send it and then create a new batch
                producer.send(eventDataBatch);
                eventDataBatch = producer.createBatch();

                // Try to add that event that couldn't fit before.
                if (!eventDataBatch.tryAdd(eventData)) {
                    throw new IllegalArgumentException("Event is too large for an empty batch. Max size: "
                            + eventDataBatch.getMaxSizeInBytes());
                }
            }
        }
        // send the last batch of remaining events
        if (eventDataBatch.getCount() > 0) {
            producer.send(eventDataBatch);
        }
        producer.close();
    }

    private static void processEvents() throws IOException, InterruptedException {
        File dir = new File(Sensor.class.getResource("/dataset").getPath());
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String path = child.getPath();
                System.out.println("Sending message " + child.getPath());
                byte[] encoded = Files.readAllBytes(Paths.get(path));
                publishEvents(new String(encoded, StandardCharsets.UTF_8));
                Thread.sleep(1000);
            }

            System.out.println("Completed sending all events");
        }
    }
}