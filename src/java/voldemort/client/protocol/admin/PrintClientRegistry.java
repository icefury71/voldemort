package voldemort.client.protocol.admin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.cluster.Node;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Helper class to print contents of the client registry System Store in a human
 * readable format
 * 
 * @author csoman
 * 
 */
public class PrintClientRegistry {

    /**
     * @param url: (Mandatory) Bootstrap URL of the Voldemort cluster
     * @param node: (Mandatory) Node for which to print the client registry (Any
     *        node should be fine since the client registry is the same on all
     *        the nodes).
     * @param zone: (Optional) Zone ID of the Admin client machine
     * @param file: (Optional) Print to this file
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("node", "node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("zone", "zone id")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
        parser.accepts("file", "output-file")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(String.class);

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printHelp(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "url", "node");
        if(missing.size() > 0) {
            printHelp(System.out);
            System.exit(0);
        }

        String url = (String) options.valueOf("url");
        int nodeId = CmdUtils.valueOf(options, "node", -1);
        int zoneId = CmdUtils.valueOf(options, "zone", -1);
        String file = (String) options.valueOf("file");
        String clientRegistryStoreName = SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name();

        int zone = zoneId == -1 ? 0 : zoneId;
        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), zone);

        // Pick up all the partitions
        List<Integer> partitionIdList = Lists.newArrayList();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            partitionIdList.addAll(node.getPartitionIds());
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = null;

        System.out.println("Fetching entries in partitions "
                           + Joiner.on(", ").join(partitionIdList) + " of "
                           + clientRegistryStoreName);
        entriesIterator = adminClient.bulkFetchOps.fetchEntries(nodeId,
                                                                clientRegistryStoreName,
                                                                partitionIdList,
                                                                null,
                                                                false);

        Writer writer = null;
        if(file == null || file.length() <= 0) {
            writer = new OutputStreamWriter(new FilterOutputStream(System.out) {

                @Override
                public void close() throws IOException {
                    flush();
                }
            });
        } else {
            System.out.println("Writing data to file: " + file);
            writer = new FileWriter(file);
        }
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        try {
            final StringWriter stringWriter = new StringWriter();
            while(entriesIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                byte[] valueBytes = kvPair.getSecond().getValue();
                String keyObject = new String(keyBytes);
                String valueObject = new String(valueBytes);
                stringWriter.write("key = " + keyObject + "\n");
                stringWriter.write(valueObject);
                stringWriter.write("===============================================\n");
            }
            bufferedWriter.write(stringWriter.toString());
        } finally {
            bufferedWriter.close();
        }

    }

    public static void printHelp(PrintStream stream) {
        stream.println("Usage: <PrintClientRegistry> --url <URL> --node <node-id> [--zone-id <zone-id>] [--file <file-path>]");
    }

}
