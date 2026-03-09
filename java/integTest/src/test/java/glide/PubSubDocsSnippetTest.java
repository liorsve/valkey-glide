/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide;

import static org.junit.jupiter.api.Assertions.*;

import glide.api.GlideClient;
import glide.api.models.PubSubMessage;
import glide.api.models.configuration.BaseSubscriptionConfiguration.MessageCallback;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.StandaloneSubscriptionConfiguration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;

/**
 * Integration tests validating PubSub code snippets from the docs website.
 * Requires a Valkey server on localhost:6379.
 */
@Timeout(30)
public class PubSubDocsSnippetTest {

    private static final int TIMEOUT_MS = 1000;
    private final List<GlideClient> clients = new ArrayList<>();

    private GlideClient createPlainClient() throws Exception {
        GlideClient c =
                GlideClient.createClient(
                                GlideClientConfiguration.builder()
                                        .address(NodeAddress.builder().build())
                                        .requestTimeout(TIMEOUT_MS)
                                        .build())
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        clients.add(c);
        return c;
    }

    private GlideClient createClientWithCallback(MessageCallback callback) throws Exception {
        GlideClient c =
                GlideClient.createClient(
                                GlideClientConfiguration.builder()
                                        .address(NodeAddress.builder().build())
                                        .requestTimeout(TIMEOUT_MS)
                                        .subscriptionConfiguration(
                                                StandaloneSubscriptionConfiguration.builder()
                                                        .callback(callback)
                                                        .build())
                                        .build())
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        clients.add(c);
        return c;
    }

    private String ch(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void cleanup() {
        for (GlideClient c : clients) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
        clients.clear();
    }

    @Test
    void testPublish() throws Exception {
        GlideClient pub = createPlainClient();
        String result = pub.publish("Test message", ch("pub")).get();
        assertNotNull(result);
    }

    @Test
    void testDynamicSubscribePolling() throws Exception {
        String channel = ch("poll");
        GlideClient sub = createPlainClient();
        GlideClient pub = createPlainClient();

        sub.subscribeLazy(Collections.singleton(channel)).get();
        Thread.sleep(1500);

        pub.publish("hello-poll", channel).get();
        PubSubMessage msg = sub.getPubSubMessage().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertTrue(msg.getMessage().toString().contains("hello-poll"));
    }

    @Test
    void testDynamicSubscribeBlocking() throws Exception {
        String channel = ch("block");
        GlideClient sub = createPlainClient();
        GlideClient pub = createPlainClient();

        sub.subscribe(Collections.singleton(channel), TIMEOUT_MS).get();
        pub.publish("hello-block", channel).get();
        PubSubMessage msg = sub.getPubSubMessage().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertTrue(msg.getMessage().toString().contains("hello-block"));
    }

    @Test
    void testPatternSubscribe() throws Exception {
        String prefix = ch("pat");
        String pattern = prefix + "*";
        String channel = prefix + "-news";
        GlideClient sub = createPlainClient();
        GlideClient pub = createPlainClient();

        sub.psubscribeLazy(Collections.singleton(pattern)).get();
        Thread.sleep(1500);

        pub.publish("pattern-msg", channel).get();
        PubSubMessage msg = sub.getPubSubMessage().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertTrue(msg.getMessage().toString().contains("pattern-msg"));
    }

    @Test
    void testCallbackBased() throws Exception {
        String channel = ch("cb");
        List<String> received = new CopyOnWriteArrayList<>();
        MessageCallback callback =
                (msg, ctx) -> received.add(msg.getMessage().toString());

        GlideClient sub = createClientWithCallback(callback);
        GlideClient pub = createPlainClient();

        sub.subscribe(Collections.singleton(channel), TIMEOUT_MS).get();
        pub.publish("cb-hello", channel).get();
        Thread.sleep(1500);

        assertFalse(received.isEmpty(), "Callback should have received at least one message");
        assertTrue(received.get(0).contains("cb-hello"));
    }

    @Test
    void testUnsubscribe() throws Exception {
        String channel = ch("unsub");
        GlideClient client = createPlainClient();

        client.subscribe(Collections.singleton(channel), TIMEOUT_MS).get();
        // Lazy unsubscribe (current API: unsubscribe(channels) without timeout)
        client.unsubscribe(Collections.singleton(channel)).get();
        Thread.sleep(1000);
    }

    @Test
    void testStateIntrospection() throws Exception {
        String ch1 = ch("intr1");
        String ch2 = ch("intr2");
        GlideClient client = createPlainClient();

        client.subscribe(new HashSet<>(Arrays.asList(ch1, ch2)), TIMEOUT_MS).get();
        Object state = client.getSubscriptions().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(state, "getSubscriptions() should return non-null state");
    }
}
