package org.example.generator;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.Order;

import java.util.Random;

public class OrderGenerator {
    private static final Random RANDOM = new Random();
    private static final String[] CLIENTS = {"John", "Alice", "Bob", "Emma"};
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:generateOrder?period=5000")
                        .process(exchange -> {
                            Order order = new Order();
                            order.setId(RANDOM.nextInt(1000));
                            order.setClient(CLIENTS[RANDOM.nextInt(CLIENTS.length)]);
                            order.setTotal(RANDOM.nextInt(500) + 50);

                            String orderJson = OBJECT_MAPPER.writeValueAsString(order);
                            exchange.getIn().setBody(orderJson);
                        })
                        .log("Generated Order: ${body}")
                        .multicast()
                        .to("activemq:queue:orders", "file:data/orders?fileName=order-${date:now:yyyyMMddHHmmss}.json");
            }
        });

        context.start();
        Thread.sleep(60000);
        context.stop();
    }
}

