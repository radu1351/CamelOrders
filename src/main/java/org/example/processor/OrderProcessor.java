package org.example.processor;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class OrderProcessor {
    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("activemq:queue:orders")
                        .filter(simple("${body} != null && ${bodyAs(String).contains('total')} && ${bodyAs(String).contains('150')}"))
                        .process(exchange -> {
                            String enrichedOrder = exchange.getIn().getBody(String.class)
                                    .replace("}", ", \"timestamp\": \"" + System.currentTimeMillis() + "\"}");
                            exchange.getIn().setBody(enrichedOrder);
                        })
                        .to("file:data/processedOrders?fileName=processed.json");
            }
        });

        context.start();
        Thread.sleep(20000);
        context.stop();
    }
}

