package rif.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import rif.catalog.Result;

/** A .proto read at run time, and records framed as Confluent's Protobuf serializer frames them. */
class ProtobufTest {

    static final String PROTO = """
            syntax = "proto3";
            package shop.v1;
            import "google/protobuf/timestamp.proto";
            option java_package = "com.example.shop"; // dropped
            /* An order, as the shop writes it. */
            message OrderPlaced {
              string order_id = 1;
              string customer_id = 2 [deprecated = true];
              int64 total_cents = 3;
              Status status = 4;
              repeated Line lines = 5;
              map<string, string> labels = 6;
              google.protobuf.Timestamp placed_at = 7;
              optional string coupon = 8;
              oneof payment { string card = 9; string wallet = 10; }
              enum Status { NEW = 0; PAID = 1; }
              message Line { string sku = 1; int32 qty = 2; }
            }
            message Refunded { string order_id = 1; double amount = 2; }
            """;

    static byte[] framed(int[] indexes, byte[] payload) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[] {0, 0, 0, 0, 7});
        CodedOutputStream c = CodedOutputStream.newInstance(out);
        if (indexes.length == 1 && indexes[0] == 0) {
            c.writeSInt32NoTag(0);
        } else {
            c.writeSInt32NoTag(indexes.length);
            for (int i : indexes) {
                c.writeSInt32NoTag(i);
            }
        }
        c.flush();
        out.write(payload);
        return out.toByteArray();
    }

    @Test
    void aProtoIsParsedAndItsRecordsRead() throws Exception {
        Descriptors.FileDescriptor f = Protobuf.parse(PROTO);
        Descriptors.Descriptor order = f.findMessageTypeByName("OrderPlaced");
        Descriptors.Descriptor line = order.findNestedTypeByName("Line");
        DynamicMessage msg = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("order_id"), "ord-1")
                .setField(order.findFieldByName("customer_id"), "cus-9")
                .setField(order.findFieldByName("total_cents"), 12995L)
                .setField(order.findFieldByName("status"), order.findEnumTypeByName("Status").findValueByName("PAID"))
                .addRepeatedField(order.findFieldByName("lines"), DynamicMessage.newBuilder(line).setField(line.findFieldByName("sku"), "A1")
                        .setField(line.findFieldByName("qty"), 2).build())
                .setField(order.findFieldByName("placed_at"), Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setField(order.findFieldByName("wallet"), "apple")
                .build();
        Result<ObjectNode> r = Protobuf.decode("t1", PROTO, framed(new int[] {0}, msg.toByteArray()), 5);
        assertTrue(r.isOk(), r.error());
        ObjectNode o = r.value();
        assertEquals("ord-1", o.get("orderId").asText());
        assertEquals(12995, o.get("totalCents").asLong());
        assertEquals("PAID", o.get("status").asText());
        assertEquals("A1", o.get("lines").get(0).get("sku").asText());
        assertEquals("2023-11-14T22:13:20Z", o.get("placedAt").asText());
        assertEquals("apple", o.get("wallet").asText());
        assertFalse(o.has("coupon"));

        Descriptors.Descriptor refund = f.findMessageTypeByName("Refunded");
        byte[] second = DynamicMessage.newBuilder(refund).setField(refund.findFieldByName("amount"), 12.5).build().toByteArray();
        Result<ObjectNode> r2 = Protobuf.decode("t1", PROTO, framed(new int[] {1}, second), 5);
        assertEquals(12.5, r2.value().get("amount").asDouble());
        assertFalse(Protobuf.decode("t1", PROTO, framed(new int[] {5}, second), 5).isOk());
    }

    @Test
    void anUnreadableSchemaSaysWhy() {
        Result<ObjectNode> r = Protobuf.decode("t2", "syntax = \"proto3\"; import \"other.proto\"; message A { B b = 1; }", new byte[] {0, 0, 0, 0, 1, 0}, 5);
        assertFalse(r.isOk());
        assertTrue(r.error().contains("other.proto"), r.error());
    }
}
