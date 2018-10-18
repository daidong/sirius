package edu.dair.sgdb.gclient;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.tengine.travel.GTravel;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClientMain {

    static Options options = new Options();

    //static final MetricRegistry metrics = new MetricRegistry();
    /*
     static {
     ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
     .convertRatesTo(TimeUnit.SECONDS)
     .convertDurationsTo(TimeUnit.MILLISECONDS)
     .build();
     reporter.start(10, TimeUnit.SECONDS);
     }
    
     static final Histogram responseTime = metrics.histogram(MetricRegistry.name(ClientMain.class, "response-time"));
     */

    private static void classPathCheck() {
        // test classpath
        try {
            //test if module dependency is in the classpath.
            Class.forName("edu.dair.sgdb.gclient.ClientMain");
            //test if other dependencies are in the classpath.
            Class.forName("org.apache.commons.lang.WordUtils");
        } catch (ClassNotFoundException e) {
            System.err.println("ClassPath is not well-configured. Please check.");
        }
    }

    private static void buildOptions() {
        // build option tables

        options.addOption(new Option("help", "print this message"));

        options.addOption(Option.builder("type").hasArg()
                .desc("client type indicates the algorithm you want to choose.")
                .build());

        options.addOption(Option.builder("op").hasArg()
                .desc("test operations;")
                .build());

        options.addOption(Option.builder("id").hasArg()
                .desc("ID of the requested vertex")
                .build());

        options.addOption(Option.builder("graph").hasArg()
                .desc("graph data directory")
                .build());

        options.addOption(Option.builder("srvlist").hasArgs()
                .desc("addresses of all servers")
                .build());

    }

    public static String[] parseArgs(String[] args, List<String> allsrvs) {
        String[] rst = new String[4];
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("iogp-client", options);
                System.exit(0);
            }

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("iogp-client", options);
                System.exit(0);
            }

            if (line.hasOption("type")) {
                rst[0] = line.getOptionValue("type", "iogp");
            } else {
                throw new ParseException("argument 'type' is required.");
            }

            if (line.hasOption("op")) {
                rst[1] = line.getOptionValue("op", "0");
            } else {
                throw new ParseException("argument 'op' is required.");
            }

            if (line.hasOption("id")) {
                rst[2] = line.getOptionValue("id");
            } else {
                throw new ParseException("argument 'id' is required.");
            }

            if (line.hasOption("graph")) {
                rst[3] = line.getOptionValue("graph");
            } else {
                //throw new ParseException("argument 'graph' is required.");
                rst[3] = "";
            }

            if (line.hasOption("srvlist")) {
                String[] srvs = line.getOptionValues("srvlist");
                allsrvs.addAll(Arrays.asList(srvs));
            }
        } catch (ParseException exp) {
            System.out.println("Arguments Error:" + exp.getMessage());
            System.exit(-1);
        }
        return rst;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, TException {
        classPathCheck();

        buildOptions();

        ArrayList<String> allsrvs = new ArrayList<>();

        String[] rst = parseArgs(args, allsrvs);

        String type = rst[0];

        String op = rst[1];

        AbstractClt client = null;

        switch (type) {
            case "edgecut":
                client = new EdgeCutClt(0, allsrvs);
                break;
            case "iogp":
                client = new IOGPClt(0, allsrvs);
                break;
            case "giga":
                client = new GIGAClt(0, allsrvs);
                break;
            case "dido":
                client = new DIDOClt(0, allsrvs);
                break;
            case "vertexcut":
                client = new VertexCutClt(0, allsrvs);
                break;            
            default:
                System.out.println("Undefined Client Type!");
                break;
        }

        String id = rst[2];
        String graphFile = rst[3];

        String line;
        long start = 0;
        BufferedReader br;

        String payload128 = "";
        for (int i = 0; i < 128; i++) payload128 += "a";
        byte[] val = payload128.getBytes();

        switch (op) {

            case "insert":
                br = new BufferedReader(new FileReader(graphFile));
                start = System.currentTimeMillis();
                while ((line = br.readLine()) != null) {
                    String[] splits = line.split(" ");
                    byte[] src = splits[0].getBytes();
                    byte[] dst = splits[1].getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                GLogger.info("Insert time: %d", (System.currentTimeMillis() - start));
                break;

            case "insert100":
                start = System.currentTimeMillis();
                for (int i = 1; i <= 100; i++) {
                    byte[] src = "vertex0".getBytes();
                    byte[] dst = ("vertex" + i).getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                GLogger.info("Insert100 time: %d", (System.currentTimeMillis() - start));
                break;

            case "insert1000":
                start = System.currentTimeMillis();
                for (int i = 1; i <= 1000; i++) {
                    byte[] src = "vertex0".getBytes();
                    byte[] dst = ("vertex" + i).getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                GLogger.info("Insert1000 time: %d", (System.currentTimeMillis() - start));
                break;

            case "insert10000":
                start = System.currentTimeMillis();
                for (int i = 1; i <= 10000; i++) {
                    byte[] src = "vertex0".getBytes();
                    byte[] dst = ("vertex" + i).getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                GLogger.info("Insert10000 time: %d", (System.currentTimeMillis() - start));
                break;
            case "insert100000":
                start = System.currentTimeMillis();
                for (int i = 1; i <= 100000; i++) {
                    byte[] src = "vertex0".getBytes();
                    byte[] dst = ("vertex" + i).getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                GLogger.info("Insert100000 time: %d", (System.currentTimeMillis() - start));
                break;

            case "scan":
                start = System.currentTimeMillis();
                List<KeyValue> r = client.scan((id).getBytes(), EdgeType.OUT);
                GLogger.info("Scan %s time: %d Size: %d",
                        id, (System.currentTimeMillis() - start), r.size());
                /*
                for (KeyValue kv : r){
                    DBKey tk = new DBKey(kv.getKey());
                    System.out.println("dst: " + new String(tk.dst));
                }
                */
                break;

            case "bfs":
                start = System.currentTimeMillis();
                List<ByteBuffer> rtn = client.bfs((id).getBytes(), EdgeType.OUT, 1);
                GLogger.info("BFS %s cost time: %d Size: %d",
                        id, (System.currentTimeMillis() - start), rtn.size());

                for (ByteBuffer v : rtn) {
                    GLogger.info("Vertex: %s", new String(NIOHelper.getActiveArray(v)));
                }
                break;

            case "travel":
                /*
                    First, Insert all edges.
                */
                br = new BufferedReader(new FileReader(graphFile));
                start = System.currentTimeMillis();
                int line_num = 0;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;

                    String[] splits = line.split("\\W+");
                    byte[] src = splits[0].getBytes();
                    byte[] dst = splits[1].getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                    line_num += 2;

                    if (line_num % 1000 == 2)
                        GLogger.info("insert %d cost %d",
                                line_num, (System.currentTimeMillis() - start));

                }
                GLogger.info("Insert time: %d", (System.currentTimeMillis() - start));

                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                client.sync();
                GLogger.info("Start Travel on vertex %s", id);
                byte[] bEdge = ArrayPrimitives.itob(EdgeType.OUT.get());
                GTravel gt = new GTravel();
                gt.v((id).getBytes());
                for (int i = 0; i < 10; i++) {
                    gt.et(bEdge).next();
                }
                gt.v();
                long cost = client.bfs_travel(gt.plan());
                GLogger.info("bfs takes: %d ms", cost);
                break;

            case "atravel":
                client.sync();
                GLogger.info("Start Async Travel on vertex %s", id);
                byte[] cEdge = ArrayPrimitives.itob(EdgeType.OUT.get());
                GTravel agt = new GTravel();
                agt.v((id).getBytes());
                for (int i = 0; i < 10; i++) {
                    agt.et(cEdge).next();
                }
                agt.v();
                cost = client.abfs_travel(agt.plan());
                GLogger.info("abfs takes: %d ms", cost);
                break;
            default:
                System.out.println("Undefined Op!");

                break;
        }

    }
}
