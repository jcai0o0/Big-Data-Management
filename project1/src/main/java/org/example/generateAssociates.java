package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class generateAssociates {
    private final Random rand = new Random();

    /**
     * generate a random personA id between 1 to 200,000
     * return: int
     */
    private int randomUserId() {
        return ThreadLocalRandom.current().nextInt(Constant.MIN_USER_ID, Constant.MAX_USER_ID+1);
    }

    /**
     * generate the id of the PersonB between 1 to 200,000
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

    private int randomDateOfFriendship() {
        return ThreadLocalRandom.current().nextInt(Constant.MIN_DateOfFriendship, Constant.MAX_DateOfFriendship + 1);
    }

    private String randomDesc() {
        int max = Constant.DESCRIPTIONS.length-1;
        int min = 0;
        int descIdx = rand.nextInt((max-min)+1)+min;
        return Constant.DESCRIPTIONS[descIdx];
    }

    public String randomRecord(int recordID) {
        int personAID = randomUserId();
        int personBID = randomUserIdExclude(personAID);
        int dateOfFriendship = randomDateOfFriendship();
        String friendshipDesc = randomDesc();
        return String.format("%d,%d,%d,%d,%s", recordID, personAID, personBID, dateOfFriendship, friendshipDesc);
    }

    public static void main(String[] args) throws IOException {
        generateAssociates a = new generateAssociates();

        Date startTime = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("Current Time: " + dateFormat.format(startTime));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Associates.csv"))) {
            for (int i=1; i<Constant.ASSOCIATES_COUNT; i++) {
                writer.write(a.randomRecord(i));
                writer.newLine();
            }
        }

        Date finishTime = new Date();
        System.out.println("Current Time: " + dateFormat.format(finishTime));
        System.out.println("Finish generating Associates dataset.");

    }
}
