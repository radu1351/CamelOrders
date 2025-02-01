package org.example.processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class OrderProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String FILE_PATH = "data/processedOrders/aggregated.json";

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("activemq:queue:orders")
                        .filter().simple("${bodyAs(String).contains('total')}")
                        .filter().method(this, "hasValidTotal")
                        .process(exchange -> {
                            String body = exchange.getIn().getBody(String.class);
                            String client = body.replaceAll(".*\"client\"\\s*:\\s*\"(.*?)\".*", "$1");
                            exchange.getIn().setHeader("client", client);
                        })
                        .aggregate(header("client"), new GroupedBodyAggregationStrategy())
                        .completionTimeout(10000)
                        .log(LoggingLevel.INFO, "Aggregated message: ${body}")
                        .process(exchange -> {
                            String aggregatedBody = exchange.getIn().getBody(String.class);
                            addToJsonFile(aggregatedBody);
                        });
            }

            public boolean hasValidTotal(Exchange exchange) {
                String body = exchange.getIn().getBody(String.class);
                return body.matches(".*\"total\"\\s*:\\s*(1[0-9][0-9]|[2-9][0-9]{2,}).*");
            }
        });

        context.start();
        Thread.sleep(60000);
        context.stop();
    }

    private static void addToJsonFile(String aggregatedBody) {
        try {
            ObjectNode newOrder = (ObjectNode) OBJECT_MAPPER.readTree(aggregatedBody);
            String clientName = newOrder.get("client").asText();

            ArrayNode ordersArray = OBJECT_MAPPER.createArrayNode();
            Path path = Paths.get(FILE_PATH);
            if (Files.exists(path)) {
                String existingContent = new String(Files.readAllBytes(path));
                if (!existingContent.isEmpty()) {
                    ordersArray = (ArrayNode) OBJECT_MAPPER.readTree(existingContent);
                }
            }

            ObjectNode clientNode = null;
            for (JsonNode node : ordersArray) {
                if (node.has("client") && node.get("client").asText().equals(clientName)) {
                    clientNode = (ObjectNode) node;
                    break;
                }
            }

            if (clientNode == null) {
                clientNode = OBJECT_MAPPER.createObjectNode();
                clientNode.put("client", clientName);
                clientNode.set("orders", OBJECT_MAPPER.createArrayNode());
                ordersArray.add(clientNode);
            }

            ArrayNode clientOrders = (ArrayNode) clientNode.get("orders");
            clientOrders.add(newOrder);

            Files.write(path, OBJECT_MAPPER.writeValueAsBytes(ordersArray), StandardOpenOption.CREATE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static class GroupedBodyAggregationStrategy implements AggregationStrategy {
        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            if (oldExchange == null) {
                return newExchange;
            }
            String oldBody = oldExchange.getIn().getBody(String.class);
            String newBody = newExchange.getIn().getBody(String.class);
            try {
                ObjectNode aggregatedOrder = OBJECT_MAPPER.createObjectNode();
                aggregatedOrder.put("client", oldExchange.getIn().getHeader("client", String.class));
                aggregatedOrder.putArray("orders").add(OBJECT_MAPPER.readTree(oldBody));
                aggregatedOrder.putArray("orders").add(OBJECT_MAPPER.readTree(newBody));

                oldExchange.getIn().setBody(aggregatedOrder.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return oldExchange;
        }
    }
}
