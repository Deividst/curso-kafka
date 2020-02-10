package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FroudDetectorService {

    public static void main(String[] args) {
        var froudDetectorService = new FroudDetectorService();
        try (var service = new KafkaService<>("ECOMMERCE_NEW_ORDER", froudDetectorService::parse,
                FroudDetectorService.class.getSimpleName(),
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------------------------------");
        System.out.println("processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Order processed");
    }
}
