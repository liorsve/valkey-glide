import {
    afterAll,
    afterEach,
    beforeAll,
    describe,
    it
} from "@jest/globals";
import {
    GlideClient
} from "..";
import { RedisCluster } from "../../utils/TestUtils.js";
import {
    flushAndCloseClient,
    parseCommandLineArgs,
    parseEndpoints,
} from "./TestUtilities";



const TIMEOUT = 50000;

describe("GlideClient Scan", () => {
    let testsFailed = 0;
    let cluster: RedisCluster;
    let client: GlideClient;
    beforeAll(async () => {
        const standaloneAddresses =
            parseCommandLineArgs()["standalone-endpoints"];
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

    it.each[]

});
