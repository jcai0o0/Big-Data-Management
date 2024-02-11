package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class generateFaceInPage {
    private final Random rand = new Random();

    private String randomName() {
        int nameLength = ThreadLocalRandom.current().nextInt(Constant.MIN_USER_NAME_CHARS, Constant.MAX_USER_NAME_CHARS);
        StringBuilder name = new StringBuilder();

        int alphabet_min = 0;
        int alphabet_max = Constant.NAME_ALPHABET.length-1;

        for (int i=0; i<nameLength; i++) {
            name.append(Constant.NAME_ALPHABET[rand.nextInt((alphabet_max-alphabet_min)+1+alphabet_min)]);
        }

        return name.toString();
    }

    private String randomNationality() {
        int max = Constant.NATIONALITY.length-1;
        int min = 0;
        int nationaityIdx = rand.nextInt((max-min)+1)+min;
        return Constant.NATIONALITY[nationaityIdx];
    }

    private int randomCountryCode() {
        return ThreadLocalRandom.current().nextInt(Constant.MIN_COUNTRY_CODE, Constant.MAX_COUNTRY_CODE + 1);
    }

    private String randomHobby() {
        int max = Constant.HOBBY.length-1;
        int min = 0;
        int hobbyIdx = rand.nextInt((max-min)+1)+min;
        return Constant.HOBBY[hobbyIdx];
    }

    public String randomRecord(int recordID) {
        String name = randomName();
        String nationality = randomNationality();
        int countryCode = randomCountryCode();
        String hobby = randomHobby();
        return String.format("%d,%s,%s,%d,%s", recordID, name, nationality, countryCode, hobby);
    }

    public static void main(String[] args) throws IOException {
        generateFaceInPage a = new generateFaceInPage();

        Date startTime = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("Current Time: " + dateFormat.format(startTime));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("FaceInPage.csv"))) {
            for (int i=1; i<Constant.FACE_IN_PAGE_COUNT+1; i++) {
                writer.write(a.randomRecord(i));
                writer.newLine();
            }
        }

        Date finishTime = new Date();
        System.out.println("Current Time: " + dateFormat.format(finishTime));
        System.out.println("Finish generating FaceInPage dataset.");
    }



}