package org.example.generator;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class OrderGenerator {
    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:generateOrder?period=5000")
                        .process(exchange -> {
                            String order = "{ \"id\": 1, \"client\": \"John\", \"total\": 150 }";
                            exchange.getIn().setBody(order);
                        })
                        .multicast()
                        .to("activemq:queue:orders", "file:data/orders?fileName=order.json");
            }
        });

        context.start();
        Thread.sleep(20000);
        context.stop();
    }
}
