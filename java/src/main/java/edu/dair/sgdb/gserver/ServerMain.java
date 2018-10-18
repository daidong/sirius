package edu.dair.sgdb.gserver;

import org.apache.commons.cli.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangwei
 */
public class ServerMain {

    static Options options = new Options();

    private static void classPathCheck() {
        // test classpath
        try {
            //test if module dependency is in the classpath.
            Class.forName("edu.dair.sgdb.gserver.ServerMain");
            //test if other dependencies are in the classpath.
            Class.forName("org.apache.commons.lang.WordUtils");
        } catch (ClassNotFoundException e) {
            System.err.println("ClassPath is not well-configured. Please check.");
        }
    }

    private static void buildOptions() {
        // build option tables

        options.addOption(new Option("help", "print this message"));

        options.addOption(new Option("local", "passing this parameter will cause "
                + "the program running in standalone mode, "
                + "instead of standalone mode."));

        options.addOption(Option.builder("type").hasArg()
                .desc("server type indicates the algorithm you want to choose.")
                .build());

        options.addOption(Option.builder("db").hasArg()
                .desc("db = name of db file;")
                .build());

        options.addOption(Option.builder("id").hasArg()
                .desc("ID of the distributed server or "
                        + "total number of threads in a standalone server")
                .build());

        options.addOption(Option.builder("srvlist").hasArgs()
                .desc("addresses of all servers which must be set when '-local' is not passed")
                .build());
        options.addOption(Option.builder("stopSign").hasArg()
                .desc("for the safty of server data, you might need to specify a file so the threads in "
                        + "the server could pause whenever the file appears.")
                .build());

    }

    /**
     * return string array, str-0 = type, str-1 = dbnum, str-2 = idx, str-3 =
     * isSrvMode, str-4=stopSign if srvmode == false, server runs in standalone
     * mode, then str-2 = null;
     *
     * @param args
     * @param allsrvs
     * @return
     */
    public static String[] parseArgs(String[] args, List<String> allsrvs) {
        String[] rst = new String[5];
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("iogp-server", options);
                System.exit(0);
            }

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("iogp-server", options);
                System.exit(0);
            }

            if (line.hasOption("type")) {
                rst[0] = line.getOptionValue("type", "giga");
            } else {
                throw new ParseException("argument 'type' is required.");
            }

            if (line.hasOption("db")) {
                rst[1] = line.getOptionValue("db", "0");
            } else {
                throw new ParseException("argument 'db' is required.");
            }

            if (line.hasOption("id")) {
                rst[2] = line.getOptionValue("id");
            } else {
                throw new ParseException("argument 'id' is required.");
            }

            if (line.hasOption("local")) {
                // standalone mode
                rst[3] = "false";
                int threadnum = Integer.valueOf(rst[2]);
                for (int i = 0; i < threadnum; i++) {
                    allsrvs.add("localhost:" + (5555 + i));
                }
            } else {
                // distributed mode, only one thread.
                rst[3] = "true";
                if (line.hasOption("srvlist")) {
                    String[] srvs = line.getOptionValues("srvlist");
                    allsrvs.addAll(Arrays.asList(srvs));
                }
            }

            if (line.hasOption("stopSign")) {
                rst[4] = line.getOptionValue("stopSign");
            }
        } catch (ParseException exp) {
            System.out.println("Arguments Error:" + exp.getMessage());
            System.exit(-1);
        }
        return rst;
    }

    public static void main(String[] args) {

        System.out.println(Arrays.toString(args));
        System.out.println("Server Started @ " + System.currentTimeMillis());

        classPathCheck();

        buildOptions();

        List<String> allsrvs = new ArrayList<>();

        String[] rst = parseArgs(args, allsrvs);

        String type = rst[0];

        String dbFile = rst[1];

        int threads = Integer.valueOf(rst[2]);

        boolean isSrvMode = Boolean.valueOf(rst[3]).booleanValue();

        String stopSign = rst[4];

        System.out.println(type + ", " + dbFile + ", " + threads + ", "
                + isSrvMode + ", " + stopSign + ", "
                + Arrays.toString(allsrvs.toArray(new String[allsrvs.size()])));

        if (isSrvMode) {
            int idx = threads;
            Thread t = new Thread(new ServerExecutable(type, dbFile + idx, idx, allsrvs));
            t.start();

        } else {
            List<Thread> tlist = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                //TODO: ensure that the dbFile in distributed server are only used by itself.
                // if it is, then no need to change "dbFile + i" here.
                Thread t = new Thread(new ServerExecutable(type, dbFile + i, i, allsrvs));
                t.start();
                tlist.add(t);
            }
        }

        //using ExecutorService to block ServerMain thread is more elegant and more manageable.
        /*
        ExecutorService executor = Executors.newFixedThreadPool(10);
        File handle = new File(stopSign);
        while (true) {

            try {
                Thread.sleep(5000);

                if (!handle.exists()) {
                    continue;
                } else {
                    executor.shutdown();
                    executor.awaitTermination(30, TimeUnit.SECONDS);
                    break;
                }
            } catch (InterruptedException e) {

            }
        }
        */
    }
}
