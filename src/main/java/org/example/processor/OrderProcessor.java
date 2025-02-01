package org.example.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.example.model.Order;

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
                            Order order = OBJECT_MAPPER.readValue(body, Order.class);
                            order.setTimestamp(System.currentTimeMillis());
                            order.setStatus("Processed");
                            exchange.getIn().setBody(OBJECT_MAPPER.writeValueAsString(order));
                            exchange.getIn().setHeader("client", order.getClient());
                        })
                        .aggregate(header("client"), new GroupedBodyAggregationStrategy())
                        .completionTimeout(10000)
                        .log(LoggingLevel.INFO, "Aggregated message: ${body}")
                        .process(OrderProcessor::addToJsonFile);
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

    private static void addToJsonFile(Exchange exchange) {
        try {
            String clientName = exchange.getIn().getHeader("client", String.class);
            JsonNode ordersBatch = OBJECT_MAPPER.readTree(exchange.getIn().getBody(String.class));

            ArrayNode rootArray = OBJECT_MAPPER.createArrayNode();
            Path path = Paths.get(FILE_PATH);

            if (Files.exists(path)) {
                byte[] fileContent = Files.readAllBytes(path);
                if (fileContent.length > 0) {
                    rootArray = (ArrayNode) OBJECT_MAPPER.readTree(fileContent);
                }
            }

            ObjectNode clientEntry = findOrCreateClientEntry(rootArray, clientName);
            ArrayNode clientOrders = (ArrayNode) clientEntry.get("orders");

            if (ordersBatch.isArray()) {
                ordersBatch.forEach(clientOrders::add);
            }

            Files.write(path, OBJECT_MAPPER.writeValueAsBytes(rootArray),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ObjectNode findOrCreateClientEntry(ArrayNode rootArray, String clientName) {
        for (JsonNode node : rootArray) {
            if (node.get("client").asText().equals(clientName)) {
                return (ObjectNode) node;
            }
        }

        ObjectNode newEntry = OBJECT_MAPPER.createObjectNode();
        newEntry.put("client", clientName);
        newEntry.set("orders", OBJECT_MAPPER.createArrayNode());
        rootArray.add(newEntry);
        return newEntry;
    }

    private static class GroupedBodyAggregationStrategy implements AggregationStrategy {
        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            try {
                ArrayNode ordersArray;

                if (oldExchange == null) {
                    ordersArray = OBJECT_MAPPER.createArrayNode();
                    JsonNode newOrder = OBJECT_MAPPER.readTree(newExchange.getIn().getBody(String.class));
                    ordersArray.add(newOrder);
                    newExchange.getIn().setBody(ordersArray.toString());
                    return newExchange;
                }

                ordersArray = (ArrayNode) OBJECT_MAPPER.readTree(oldExchange.getIn().getBody(String.class));
                JsonNode newOrder = OBJECT_MAPPER.readTree(newExchange.getIn().getBody(String.class));
                ordersArray.add(newOrder);

                oldExchange.getIn().setBody(ordersArray.toString());
                return oldExchange;

            } catch (Exception e) {
                e.printStackTrace();
                return oldExchange;
            }
        }
    }
}
