package org.example.transformer;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class OrderTransformer {
    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file:data/processedOrders?noop=true")
                        .unmarshal().json()
                        .marshal().jacksonXml()
                        .to("file:data/archivedOrders?fileName=archived.xml");
            }
        });

        context.start();
        Thread.sleep(20000);
        context.stop();
    }
}
