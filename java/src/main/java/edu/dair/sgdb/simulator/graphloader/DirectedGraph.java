package edu.dair.sgdb.simulator.graphloader;

import edu.dair.sgdb.simulator.tools.Edge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by daidong on 2016/7/29.
 */
public class DirectedGraph {

    public static ArrayList<Edge> load(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        ArrayList<Edge> generated = new ArrayList<>();

        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#"))
                continue;

            String[] l = line.split("\\W+");
            generated.add(new Edge(Integer.valueOf(l[0]), Integer.valueOf(l[1])));
        }
        br.close();

        return generated;
    }
}
