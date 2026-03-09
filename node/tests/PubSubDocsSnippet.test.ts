/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 *
 * Integration tests validating PubSub code snippets from the docs website.
 * Requires a Valkey server on localhost:6379.
 */

import { afterAll, afterEach, describe, expect, it } from "@jest/globals";
import {
    ALL_CHANNELS,
    ALL_PATTERNS,
    GlideClient,
    GlideClientConfiguration,
    PubSubMsg,
} from "../build-ts";

const TIMEOUT_MS = 1000;

function unique(prefix: string): string {
    return `${prefix}-${Math.random().toString(36).substring(2, 10)}`;
}

async function createPlainClient(): Promise<GlideClient> {
    return GlideClient.createClient({
        addresses: [{ host: "localhost", port: 6379 }],
        requestTimeout: TIMEOUT_MS,
    });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("PubSub Docs Snippets", () => {
    const clients: GlideClient[] = [];

    afterEach(async () => {
        for (const c of clients) {
            try {
                c.close();
            } catch {
                // ignore
            }
        }

        clients.length = 0;
        // Allow sockets to release
        await sleep(50);
    });

    function track(client: GlideClient): GlideClient {
        clients.push(client);
        return client;
    }

    /**
     * Docs section: Publishing
     *
     * ```typescript
     * await publishingClient.publish("Test message", "ch1");
     * ```
     */
    it("publish", async () => {
        const pub = track(await createPlainClient());
        const result = await pub.publish("Test message", unique("ch"));
        expect(result).toBeDefined();
    });

    /**
     * Docs section: Dynamic Subscriptions — blocking subscribe + polling
     *
     * ```typescript
     * await client.subscribe(new Set(["news", "updates"]), 5000);
     * const msg = await client.getPubSubMessage();
     * ```
     */
    it("dynamic subscribe polling", async () => {
        const ch = unique("poll");
        const sub = track(await createPlainClient());
        const pub = track(await createPlainClient());

        await sub.subscribeLazy([ch]);
        await sleep(1500);

        await pub.publish("hello-poll", ch);
        const msg = await sub.getPubSubMessage();
        expect(msg).toBeDefined();
        expect(msg.message).toContain("hello-poll");
    });

    /**
     * Docs section: Dynamic Subscriptions — blocking subscribe
     *
     * ```typescript
     * await client.subscribe(new Set(["news"]), 5000);
     * ```
     */
    it("dynamic subscribe blocking", async () => {
        const ch = unique("block");
        const sub = track(await createPlainClient());
        const pub = track(await createPlainClient());

        await sub.subscribe([ch], TIMEOUT_MS);
        await pub.publish("hello-block", ch);
        const msg = await sub.getPubSubMessage();
        expect(msg).toBeDefined();
        expect(msg.message).toContain("hello-block");
    });

    /**
     * Docs section: Dynamic Subscriptions — pattern subscribe
     *
     * ```typescript
     * await client.psubscribe(new Set(["chat*", "event*"]), 5000);
     * await client.psubscribeLazy(new Set(["log*"]));
     * ```
     */
    it("pattern subscribe", async () => {
        const prefix = unique("pat");
        const pattern = `${prefix}*`;
        const channel = `${prefix}-news`;
        const sub = track(await createPlainClient());
        const pub = track(await createPlainClient());

        await sub.psubscribeLazy([pattern]);
        await sleep(1500);

        await pub.publish("pattern-msg", channel);
        const msg = await sub.getPubSubMessage();
        expect(msg).toBeDefined();
        expect(msg.message).toContain("pattern-msg");
    });

    /**
     * Docs section: Receiving Messages — Callback
     *
     * ```typescript
     * const callback = (msg: PubSubMsg, context: any) => {
     *     received.push(msg.message);
     * };
     * const config = {
     *     addresses: [{ host: "localhost", port: 6379 }],
     *     pubsubSubscriptions: {
     *         channelsAndPatterns: {},
     *         callback: callback,
     *         context: null,
     *     },
     * };
     * const client = await GlideClient.createClient(config);
     * await client.subscribe(new Set(["news"]), 5000);
     * ```
     */
    it("callback based", async () => {
        const ch = unique("cb");
        const received: string[] = [];

        const callback = (msg: PubSubMsg) => {
            received.push(msg.message as string);
        };

        const sub = track(
            await GlideClient.createClient({
                addresses: [{ host: "localhost", port: 6379 }],
                requestTimeout: TIMEOUT_MS,
                pubsubSubscriptions: {
                    channelsAndPatterns: {},
                    callback: callback,
                    context: null,
                },
            }),
        );
        const pub = track(await createPlainClient());

        await sub.subscribe([ch], TIMEOUT_MS);
        await pub.publish("cb-hello", ch);
        await sleep(1500);

        expect(received.length).toBeGreaterThan(0);
        expect(received[0]).toContain("cb-hello");
    });

    /**
     * Docs section: Receiving Messages — Polling (config-based subscriptions)
     *
     * ```typescript
     * const listeningConfig = {
     *     pubsubSubscriptions: {
     *         channelsAndPatterns: {
     *             [GlideClientConfiguration.PubSubChannelModes.Exact]: new Set(["ch1", "ch2"]),
     *         },
     *     },
     * };
     * const message = await listeningClient.getPubSubMessage();
     * const message2 = listeningClient.tryGetPubSubMessage();
     * ```
     */
    it("polling config-based", async () => {
        const ch = unique("poll-cfg");

        const sub = track(
            await GlideClient.createClient({
                addresses: [{ host: "localhost", port: 6379 }],
                requestTimeout: TIMEOUT_MS,
                pubsubSubscriptions: {
                    channelsAndPatterns: {
                        [GlideClientConfiguration.PubSubChannelModes.Exact]:
                            new Set([ch]),
                    },
                },
            }),
        );
        const pub = track(await createPlainClient());

        // tryGetPubSubMessage returns null when no message is available
        const noMsg = sub.tryGetPubSubMessage();
        expect(noMsg).toBeNull();

        await pub.publish("poll-cfg-msg", ch);
        const msg = await sub.getPubSubMessage();
        expect(msg).toBeDefined();
        expect(msg.message).toContain("poll-cfg-msg");
    });

    /**
     * Docs section: Unsubscribing
     *
     * ```typescript
     * await client.unsubscribe(new Set(["news"]), 5000);
     * await client.unsubscribeLazy(new Set(["alerts"]));
     * await client.unsubscribe(ALL_CHANNELS, 5000);
     * await client.punsubscribe(new Set(["chat*"]), 5000);
     * await client.punsubscribeLazy(new Set(["log*"]));
     * await client.punsubscribe(ALL_PATTERNS, 5000);
     * ```
     */
    it("unsubscribe", async () => {
        const ch = unique("unsub");
        const client = track(await createPlainClient());

        await client.subscribe([ch], TIMEOUT_MS);
        await client.unsubscribeLazy([ch]);
        await sleep(1000);
    });

    it("unsubscribe all channels", async () => {
        const ch1 = unique("unsub-all-1");
        const ch2 = unique("unsub-all-2");
        const client = track(await createPlainClient());

        await client.subscribe([ch1, ch2], TIMEOUT_MS);
        await client.unsubscribe(ALL_CHANNELS, TIMEOUT_MS);
        await sleep(1000);
    });

    it("punsubscribe", async () => {
        const pattern = unique("punsub") + "*";
        const client = track(await createPlainClient());

        await client.psubscribe([pattern], TIMEOUT_MS);
        await client.punsubscribeLazy([pattern]);
        await sleep(1000);
    });

    it("punsubscribe all patterns", async () => {
        const p1 = unique("punsub-all-1") + "*";
        const p2 = unique("punsub-all-2") + "*";
        const client = track(await createPlainClient());

        await client.psubscribe([p1, p2], TIMEOUT_MS);
        await client.punsubscribe(ALL_PATTERNS, TIMEOUT_MS);
        await sleep(1000);
    });

    /**
     * Docs section: Subscription State Introspection
     *
     * ```typescript
     * const state = await client.getSubscriptions();
     * console.log("Desired:", state.desiredSubscriptions);
     * console.log("Actual:", state.actualSubscriptions);
     * ```
     */
    it("state introspection", async () => {
        const ch1 = unique("intr1");
        const ch2 = unique("intr2");
        const client = track(await createPlainClient());

        await client.subscribe([ch1, ch2], TIMEOUT_MS);
        const state = await client.getSubscriptions();
        expect(state).toBeDefined();
        expect(state.desiredSubscriptions).toBeDefined();
        expect(state.actualSubscriptions).toBeDefined();
    });

    /**
     * Regression: a client created with NO subscription config should still
     * receive messages via polling after dynamic subscribe.
     */
    it("no config polling regression", async () => {
        const ch = unique("noconfig");
        const pub = track(await createPlainClient());
        const sub = track(await createPlainClient());

        await sub.subscribe([ch], TIMEOUT_MS);
        await pub.publish("no-config-msg", ch);
        const msg = await sub.getPubSubMessage();
        expect(msg).toBeDefined();
        expect(msg.message).toContain("no-config-msg");
        expect(msg.channel).toBe(ch);
    });
});
