package edu.ttu.discl.iogp.gclient;

import edu.ttu.discl.iogp.gclient.edgecut.EdgeCutClt;
import edu.ttu.discl.iogp.gclient.iogp.IOGPClt;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.tengine.travel.GTravel;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.ArrayPrimitives;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientMain {

    static Options options = new Options();

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ClientMain.class);
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
            Class.forName("edu.ttu.discl.iogp.gclient.ClientMain");
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
                .desc("ID of the concurrent clients")
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

        GraphClt client = null;

        switch (type) {
            case "edgecut":
                client = new EdgeCutClt(0, allsrvs);
                break;
            case "iogp":
                client = new IOGPClt(0, allsrvs);
                break;
            default:
                System.out.println("Undefined Client Type!");
                break;
        }

        int id = Integer.valueOf(rst[2]);
        String graphDir = rst[3];
        String graphFile = graphDir + "/" + id;
        String summaryFile = graphDir + "/" + "rmat-sum";

        String line;
        long start = 0;
        BufferedReader br;
        int edge = EdgeType.IN.get();
        byte[] bEdge = ArrayPrimitives.itob(edge);

        switch (op) {

            case "insert":

                br = new BufferedReader(new FileReader(graphFile));

                String payload128 = "";
                for (int i = 0; i < 128; i++) payload128 += "a";
                byte[] val = payload128.getBytes();

                start = System.currentTimeMillis();
                while ((line = br.readLine()) != null) {
                    long sts = System.currentTimeMillis();
                    String[] splits = line.split(" ");
                    byte[] src = splits[0].getBytes();
                    byte[] dst = splits[1].getBytes();

                    client.insert(src, EdgeType.OUT, dst, val);
                    client.insert(dst, EdgeType.IN, src, val);
                }
                logger.info("[" + id + "] Insert time: " + (System.currentTimeMillis() - start));
                break;

            case "fullscan":
                br = new BufferedReader(new FileReader(summaryFile));
                while ((line = br.readLine()) != null) {
                    String[] splits = line.split(" ");
                    int degree = Integer.parseInt(splits[0]);
                    start = System.currentTimeMillis();
                    List<KeyValue> r = client.scan(("vertex" + splits[1]).getBytes(), EdgeType.IN);
                    logger.info("[" + id + "] Scan vertex" + splits[1] +
                            "[" + degree + "] " +
                            (System.currentTimeMillis() - start) +
                            " " + r.size() + " elements.");
                }
                break;

            case "scan":
                start = System.currentTimeMillis();
                List<KeyValue> r = client.scan(("vertex" + id).getBytes(), EdgeType.IN);
                logger.info("[" + id + "] Scan time: " +
                        (System.currentTimeMillis() - start) +
                        " " + r.size() + " elements.");
                break;


            default:
                if (op.endsWith("-SyncTravel")) {
                    start = System.currentTimeMillis();
                    int trav_round = Integer.valueOf(op.split("-")[0]);

                    String vid = String.valueOf(id);
                    GTravel gt = new GTravel();
                    gt.v(("vertex" + vid).getBytes());
                    for (int i = 0; i < trav_round; i++) {
                        gt.et(bEdge).next();
                    }
                    gt.v();

                    client.submitSyncTravel(gt.plan());
                    logger.info("SYNC [" + trav_round + "] Steps, VID[" + vid + "]: "
                            + (System.currentTimeMillis() - start) + ".");

                } else if (op.endsWith("-FullSyncTravel")) {
                    int trav_round = Integer.valueOf(op.split("-")[0]);

                    br = new BufferedReader(new FileReader(summaryFile));
                    while ((line = br.readLine()) != null) {
                        GTravel gt = new GTravel();
                        String[] splits = line.split(" ");
                        int degree = Integer.parseInt(splits[0]);
                        String vid = splits[1];

                        gt.v(("vertex" + vid).getBytes());
                        for (int i = 0; i < trav_round; i++) {
                            gt.et(bEdge).next();
                        }
                        gt.v();

                        start = System.currentTimeMillis();
                        client.submitSyncTravel(gt.plan());
                        logger.info("FullSyncTravel [" + trav_round + "] Steps, VID[" +
                                vid + "] Degree[" + degree + "]: " +
                                (System.currentTimeMillis() - start) + ".");
                    }

                } else {
                    System.out.println("Undefined Op!");
                }
                break;
        }

    }
}
