import {
    afterAll,
    afterEach,
    beforeAll,
    describe,
    it
} from "@jest/globals";
import { v4 as uuidv4 } from "uuid";
import {
    GlideClient,
    ProtocolVersion
} from "..";
import { RedisCluster } from "../../utils/TestUtils.js";
import {
    flushAndCloseClient,
    getClientConfigurationOption,
    parseCommandLineArgs,
    parseEndpoints,
} from "./TestUtilities";




const TIMEOUT = 50000;

describe("GlideClusterClient Scan", () => {
    let testsFailed = 0;
    let cluster: RedisCluster;
    let client: GlideClient;
    beforeAll(async () => {
        const standaloneAddresses =
            await parseCommandLineArgs()["standalone-endpoints"];
        // Connect to cluster or create a new one based on the parsed addresses
        cluster = standaloneAddresses
            ? await RedisCluster.initFromExistingCluster(
                  parseEndpoints(standaloneAddresses),
              )
            : await RedisCluster.createCluster(false, 1, 1);
    }, 20000);

    afterEach(async () => {
        await flushAndCloseClient(false, cluster.getAddresses(), client);
    });

    afterAll(async () => {
        if (testsFailed === 0) {
            await cluster.close();
        }
    }, TIMEOUT);

    it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
        `GlideClient simple scan%p`,
        async (protocol) => {
            client = await GlideClient.createClient(
                getClientConfigurationOption(cluster.getAddresses(), protocol),
            );
            
            const key = uuidv4();
            const expectedKeys = Array.from({length: 100}, (_, i) => `${key}:${i}`);
            const keyValueMap = expectedKeys.reduce((map:Record<string, string>, key:string) => {
                map[key] = "value";
                return map;
            },  {});
            expect(await client.mset(keyValueMap)).toEqual("OK");
            let cursor = "0";
            let keys = new Set();

            do {
                const result = client.scan(cursor);
                cursor = result[0];
                const newKeysSet = new Set(result[1]);
                newKeysSet.forEach(key => keys.add(key));
            } while (cursor!=="0")

            expect(keys).toEqual(expectedKeys);
        },
        TIMEOUT,
    );

    it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
        `GlideClient scan with object type and pattern%p`,
        async (protocol) => {
            const client = await GlideClient.createClient(
                getClientConfigurationOption(cluster.getAddresses(), protocol),
            );

            const key = uuidv4();
            const expectedKeys = Array.from({length: 100}, (_, i) => `key:${key}:${i}`);
            for (const key of expectedKeys)
                expect (await client.set(key, "value")).toEqual("OK");
            const unexpectedTypeKeys = Array.from({length: 100}, (_, i) => `key:${i+100}`);
            for (const key of unexpectedTypeKeys)
                expect (await client.sadd(key, ["value"]));
            const unexpectedPatternKeys = Array.from({length:100}, (_,i)=>`${i+200}`)
            for (const key of unexpectedTypeKeys)
                expect (await client.set(key, "value"));
            let cursor = "0";
            let keys = new Set();
            
            do {
                const result = await client.scan(cursor, "key*", ObjectType.String);
                cursor = result[0];
                const newKeysSet = new Set(result[1]);
                newKeysSet.forEach(key => keys.add(key));
            } while (cursor!== "0")

            expect(keys).toEqual(expectedKeys);
            expect([...keys].every(key => !unexpectedTypeKeys.includes((key as string)))).toBe(true);
            expect([...keys].every(key => !unexpectedPatternKeys.includes((key as string)))).toBe(true);
        },
        TIMEOUT,
    );


    it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
        `GlideClient scan with count%p`,
        async (protocol) => {
            const client = await GlideClient.createClient(
                getClientConfigurationOption(cluster.getAddresses(), protocol),
            );

            const key = uuidv4();
            const expectedKeys = Array.from({length: 100}, (_, i) => `key:${key}:${i}`);
            for (const key of expectedKeys)
                expect (await client.set(key, "value")).toEqual("OK");
            
            let cursor = "0";
            let keys = new Set();
            let successfulComparedScans = 0; 
            do {
               const resultOf1 = await client.scan(cursor, 1);
               cursor = resultOf1[0];
               const newKeysSet1 = new Set(resultOf1[1]);
               newKeysSet1.forEach((key => keys.add(key)));
               const resultOf100 = await client.scan(cursor, 100);
               cursor = resultOf100[0];
               const newKeysSet2 = new Set(resultOf100[1]);
               newKeysSet2.forEach(key => keys.add(key));
               if (newKeysSet1.size > newKeysSet2.size)
                successfulComparedScans+=1;
            } while (cursor!== "0")
            
            expect(keys).toEqual(expectedKeys);
            expect(successfulComparedScans).toBeGreaterThan(0);
        },
        TIMEOUT,
    );

    it.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
        `GlideClient scan with match%p`,
        async (protocol) => {
            const client = await GlideClient.createClient(
                getClientConfigurationOption(cluster.getAddresses(), protocol),
            );

            const key = uuidv4();
            const expectedKeys = Array.from({length: 100}, (_, i) => `key:${key}:${i}`);
            for (const key of expectedKeys)
                expect (await client.set(key, "value")).toEqual("OK");
            const unexpectedKeys = Array.from({length:100}, (_,i)=>`${i}`)
            for (const key of unexpectedKeys)
                expect (await client.set(key, "value"));
            let keys = new Set();
            let cursor = "0";
            do {
                const result = await client.scan(cursor, "key:*");
                cursor = result[0];
                const newKeysSet = new Set(result[1]);
                newKeysSet.forEach(key => keys.add(key));
            } while (cursor!== "0")
            expect(keys).toEqual(expectedKeys);
            expect([...keys].every(key => !unexpectedKeys.includes((key as string)))).toBe(true);
        },
        TIMEOUT,
    );

    


    

});
