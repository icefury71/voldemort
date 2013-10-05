package voldemort;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.DecoderException;

import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.ZoneRoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class SampleVoldemortClient {

    private static StoreClient<Object, Object> client;
    private static List<StoreDefinition> storeDefs;
    private static RoutingStrategy strategy;
    private static Cluster cluster;
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private static final Random randomGenerator = new Random(System.currentTimeMillis());
    private static Map<Integer, Store<ByteArray, byte[], byte[]>> nodeIdToStoreMap;
    private static Serializer<String> keySerializer;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() < 5 || nonOptions.size() > 6) {
            System.err.println("Usage: java VoldemortSampleClient store_name bootstrap_url zone_id keys binary_mode");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String storeName = nonOptions.get(0);
        String bootstrapUrl = nonOptions.get(1);
        int zoneId = Integer.parseInt(nonOptions.get(2));
        boolean binaryMode = Boolean.parseBoolean(nonOptions.get(4));
        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                      .setRequestFormatType(RequestFormatType.VOLDEMORT_V3)
                                                      .setClientZoneId(zoneId)
                                                      .setEnableInconsistencyResolvingLayer(false)
                                                      .setConnectionTimeout(2000,
                                                                            TimeUnit.MILLISECONDS);

        SocketStoreClientFactory factory = null;
        StoreDefinition storeDef = null;

        try {
            try {
                factory = new SocketStoreClientFactory(clientConfig);
                client = factory.getStoreClient(storeName);

                // String storesXml =
                // ((AbstractStoreClientFactory)factory).bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
                // storeDefs = storeMapper.readStoreList(new
                // StringReader(storesXml), false);

                storeDefs = ((AbstractStoreClientFactory) factory).getStoreDefs();
                for(StoreDefinition d: storeDefs) {
                    if(d.getName().equals(storeName)) {
                        storeDef = d;
                    }
                }
                if(storeDef == null) {
                    System.err.println("Provided store name does not exist !!!");
                    System.exit(-1);
                }
                cluster = ((AbstractStoreClientFactory) factory).getCluster();
                keySerializer = (Serializer<String>) clientConfig.getSerializerFactory()
                                                                 .getSerializer(storeDef.getKeySerializer());

            } catch(Exception e) {
                Utils.croak("Could not connect to server: " + e.getMessage());
            }

            System.out.println("Established connection to " + storeName + " via " + bootstrapUrl);

            // Set up the different SocketStores
            nodeIdToStoreMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
            for(Node node: cluster.getNodes()) {
                Store<ByteArray, byte[], byte[]> store = factory.getStore(storeDef.getName(),
                                                                          node.getHost(),
                                                                          node.getSocketPort(),
                                                                          RequestFormatType.VOLDEMORT_V2);
                // store = new CompressingStore(store,
                // new NoopCompressionStrategy(),
                // new GzipCompressionStrategy());
                nodeIdToStoreMap.put(node.getId(), store);
            }
            strategy = new ZoneRoutingStrategy(cluster,
                                               storeDef.getZoneReplicationFactor(),
                                               storeDef.getReplicationFactor());

            // strategy = new
            // ConsistentRoutingStrategy(((AbstractStoreClientFactory)factory).getCluster(),
            // storeDef.getReplicationFactor());

            fetchKeyVersions(nonOptions.get(3), binaryMode);

            // doEvolution();
        } finally {
            if(factory != null)
                factory.close();
        }
    }

    private static void fetchKeyVersions(String keys, boolean binary) throws DecoderException {
        String[] keysToLookup = keys.split(",");

        for(String key: keysToLookup) {
            System.out.println("\n************ [Key: " + key + "] ************");

            String keyString = "";
            ByteArray keyByteArray = null;

            if(binary) {
                byte[] byteArray = ByteUtils.fromHexString(key);
                keyString = keySerializer.toObject(byteArray);
                keyByteArray = new ByteArray(byteArray);

            } else {
                keyString = key;
                keyByteArray = new ByteArray(keySerializer.toBytes(key));
            }

            List<Node> prefNodeList = client.getResponsibleNodes(keyString);

            // System.out.println("Preflist: " + prefNodeList);
            for(Node node: prefNodeList) {
                Store<ByteArray, byte[], byte[]> store = nodeIdToStoreMap.get(node.getId());
                if(store == null) {
                    break;
                }

                try {
                    // System.out.println("Actual version: " +
                    // store.getVersions(new ByteArray(key.getBytes())));

                    List<Version> versions = store.getVersions(keyByteArray);
                    List<Versioned<byte[]>> resultList = store.get(keyByteArray, null);
                    StringBuilder builder = new StringBuilder();
                    builder.append("Replica: " + node.getHost());
                    builder.append(" Actual version: " + versions);
                    builder.append(" Actual value: "
                                   + new String(resultList.get(0).getValue()).trim());
                    System.out.println(builder.toString());
                } catch(Exception e) {
                    e.printStackTrace();
                    System.out.println("Exception occurred while talking to node: " + node + " = "
                                       + e);
                }
            }

            Versioned<Object> val = client.get(key);
            if(val == null) {
                continue;
            }

            Object value = val.getValue();
            if(value instanceof String) {
                System.out.println("Value = " + value);
            } else {
                System.out.println("Not a string, value = " + new String(((byte[]) value)));
            }

            System.out.println("Version = " + val.getVersion());
        }

    }

    private static void doOperation() throws Exception {
        System.out.println("Doing a whole bunch of puts");
        int i = 1;
        for(;;) {
            try {
                client.put("my-key" + i, "my-value" + i);
                Thread.sleep(10);
                client.get("my-key" + randomGenerator.nextInt(i));
                Thread.sleep(10);
                i++;
            } catch(VoldemortException ve) {
                ve.printStackTrace();
            }
        }
    }

    private static void doEvolution() throws Exception {
        System.out.println("Doing a whole bunch of puts");

        String versionZero = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" }]}";

        String versionOne = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" } ,"
                            + "{ \"name\": \"new-field\", \"type\": \"string\", \"default\":\"\" }]}";

        Schema s0 = Schema.parse(versionZero);
        Schema s1 = Schema.parse(versionOne);

        GenericData.Record recordOld = new GenericData.Record(s0);
        GenericData.Record recordNew = new GenericData.Record(s1);

        int i = 1;
        for(;;) {
            try {
                System.out.println("Writing via old schema: ");
                recordOld.put("original", new Utf8("my-value-type" + i));
                client.put("my-key" + i, recordOld);
                Thread.sleep(10);
                System.out.println("Getting via old schema: " + client.get("my-key" + i));
                Thread.sleep(2000);

                // System.out.println("Writing via new schema: ");
                // recordNew.put("original", new Utf8("my-value-type" + i));
                // recordNew.put("new-field", new Utf8("new-my-value-type" +
                // i));
                // client.put("my-key" + i, recordNew);
                // Thread.sleep(10);
                // System.out.println("Getting via new schema: " +
                // client.get("my-key" + i));
                // Thread.sleep(2000);

                i++;
            } catch(VoldemortException ve) {
                ve.printStackTrace();
            }
        }
    }
}
