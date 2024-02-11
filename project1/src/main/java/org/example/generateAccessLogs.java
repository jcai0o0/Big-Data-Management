package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class generateAccessLogs {
    private final Random rand = new Random();

    /**
     * generate a random person (num2) id between 1 to 200,000
     * return: int
     */
    private int randomUserId() {
        return ThreadLocalRandom.current().nextInt(Constant.MIN_USER_ID, Constant.MAX_USER_ID+1);
    }

    /**
     * generate the id of the person (num1) who has accessed the FaceInPage
     * @param excludeUserID int
     * @return int
     */
    private int randomUserIdExclude(int excludeUserID) {
        int ret = ThreadLocalRandom.current().nextInt(Constant.MIN_USER_ID, Constant.MAX_USER_ID+1);
        while (ret == excludeUserID) {
            ret = ThreadLocalRandom.current().nextInt(Constant.MIN_USER_ID, Constant.MAX_USER_ID+1);
        }
        return ret;
    }

    /**
     * generate access reason
     * return: String
     */
    private String randomReason() {
        int max = Constant.ACCESS_REASON.length - 1;
        int min = 0;
        int reasonIdx = rand.nextInt((max-min)+1) + min;
        return Constant.ACCESS_REASON[reasonIdx];
    }

    /**
     * generate access time
     * @return int, random number between 1 and 1,000,000
     */
    private int randomAccessTime() {
        return ThreadLocalRandom.current().nextInt(Constant.MIN_ACCESS_TIME, Constant.MAX_ACCESS_TIME+1);
    }

    public String randomRecord(int recordId) {
        int userID = randomUserId();
        int accessUserID = randomUserIdExclude(userID);
        String accessReason = randomReason();
        int accessTime = randomAccessTime();
        return String.format("%d,%d,%d,%s,%d", recordId, userID, accessUserID, accessReason, accessTime);
    }

    public static void main(String[] args) throws IOException {
        generateAccessLogs a = new generateAccessLogs();

        Date startTime = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        System.out.println("Current Time: " + dateFormat.format(startTime));

        // test part
//        System.out.println("Test!!");
//        for (int i=1; i<=10; i++) {
//            System.out.println(a.randomRecord(i));
//        }

        // generate csv file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("AccessLog.csv"))) {
            for (int i=1; i<=Constant.ACCESS_COUNT; i++) {
                writer.write(a.randomRecord(i));
                writer.newLine();
            }
        }

        Date finishTime = new Date();
        System.out.println("Current Time: " + dateFormat.format(finishTime));
        System.out.println("Finish generating AccessLog dataset.");

    }
}