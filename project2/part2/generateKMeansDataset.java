package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class generateKMeansDataset {
    private final Random rand = new Random();

    /**
     * generate a 2-dimensional data point
     * x and y values range from 0 to 10,000
     * @return string, "x,y"
     */
    public String randomCoordinate() {
        int x = rand.nextInt(10001);  // generate a random int between 0 and 10000
        int y = rand.nextInt(10001);
        return x+","+y;
    }

    public static void main(String[] args) throws IOException {
        generateKMeansDataset a = new generateKMeansDataset();

        // test part
//        System.out.println("Test!!");
//        for (int i=1; i<=100; i++) {
//            System.out.println(a.randomCoordinate());
//        }

        // generate csv file
        // at least 5,000 data points consisting of 2-dimensional points
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("KMeansDataset.csv"))) {
            for (int i=1; i<=5000; i++) {
                writer.write(a.randomCoordinate());
                writer.newLine();
            }
        }

        System.out.println("Finish generating K-means dataset!!");
    }
}
