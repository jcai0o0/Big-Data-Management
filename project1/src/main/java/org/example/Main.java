package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {

            String[] nationalities = {
                "Afghanistan",
                "Short Country Name",
                "Long Country Name",
                "Azerbaijan",
                "Bangladesh",
                "Guatemalan",
                "WPI Country",
                "Lithuanian",
                "Singaporean",
                "New Zealand",
                "Czech Republic",
                "Dominican Republic",
                "El Salvador",
                "Equatorial Guinea",
                "Ethiopia Land",
                "Honduras Land",
                "Indonesia Land",
                "Kyrgyzstan",
                "Madagascar",
                "Mauritania",
                "Mozambique",
                "Netherlands",
                "Nicaragua Land",
                "North Macedonia",
                "Philippines",
                "Sierra Leone",
                "Singapore Land",
                "Slovakia Land",
                "Slovenia Land",
                "South African",
                "South Korean",
                "Sri Lanka Land",
                "Switzerland",
                "Tajikistan",
                "Tanzania Land",
                "Turkmenistan",
                "Uzbekistan",
                "French Guiana",
                "Antigua and Barbuda",
                "Herzegovina",
                "Solomon Islands",
                "Trinidad and Tobago",
                "United Arab Emirates",
                "United Kingdom",
                "United States"
            };

            String[] hobbies = {
                    "Photography",
                    "Gardening",
                    "Meditation",
                    "Painting",
                    "Birdwatching",
                    "Woodworking",
                    "Genealogy",
                    "Skateboarding",
                    "Calligraphy",
                    "Collecting",
                    "Astrology",
                    "Bodybuilding",
                    "Backpacking",
                    "Candle making",
                    "Videography",
                    "Salsa dancing",
                    "Needlepoint",
                    "Sculpting",
                    "Microscopy",
                    "Skate surfing",
                    "Geocaching",
                    "Rock climbing",
                    "Horseback riding",
                    "Numismatics",
                    "Spelunking",
                    "Home brewing",
                    "Beekeeping",
                    "Sky diving",
                    "Bungee jumping",
                    "Stamp collecting",
                    "Kite flying",
                    "Windsurfing",
                    "Taekwondo",
                    "Ice skating",
                    "Paddle boarding",
                    "Taxidermy",
                    "Ice fishing",
                    "Leather craft",
                    "Rappelling",
                    "Cardistring",
                    "Whittling",
                    "Entomology",
                    "Blacksmithing"
            };

            String[] descriptions = {
                    "Lifelong companionship",
                    "Childhood buddies",
                    "College confidants",
                    "Workday comrades",
                    "Adventurous allies",
                    "Creative collaborators",
                    "Traveling companions",
                    "Supportive soulmates",
                    "Fitness motivators",
                    "Gaming comrades",
                    "Literary confidants",
                    "Artistic co-conspirators",
                    "Volunteer partners",
                    "Music festival pals",
                    "Foodie exploration pals",
                    "Hiking trail buddies",
                    "Sports enthusiasts",
                    "Tech-savvy sidekicks",
                    "Adventure travel pals",
                    "Movie marathon buddies"
            };

            String FaceInPageCSV = "face_in_page.csv";
            String associates = "associates.csv";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(FaceInPageCSV))) {
                //int numFaceInPages = 200000;
                int numFaceInPages = 10;
                for (int i = 1; i <= numFaceInPages; i++) {

                    StringBuilder faceInPageString = new StringBuilder();

                    //id number
                    faceInPageString.append(i);
                    faceInPageString.append(",");

                    //name
                    faceInPageString.append(generateRandomName() + ",");

                    //nationality and country code
                    Random random = new Random();
                    int country_index = random.nextInt(50);
                    int country_code = country_index + 1;
                    faceInPageString.append(nationalities[country_index] + "," + country_code + ",");

                    //hobby
                    int hobby_index = random.nextInt(15);
                    faceInPageString.append(hobbies[hobby_index]);

                    writer.write(faceInPageString.toString());
                    writer.newLine();
                }

                //int numAssociates = 20000000;
                int numAssociates = 10;

                //for these, associate the first with each of the second, then increment the first and repeat.
                int aID = 1;
                int bID = 1;

                for(int i = 0; i < numAssociates; i++){
                    StringBuilder associateString  = new StringBuilder();

                    associateString.append(i);
                    associateString.append(",");




                    Random random = new Random();
                    int DateOfFriendship = random.nextInt(1000000) + 1;
                    associateString.append(DateOfFriendship + ",");

                    writer.write(associateString.toString());
                    writer.newLine();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    private static String generateRandomName() {
        Random random = new Random();
        int nameLength = random.nextInt(11) + 10; // Generates a length between 10 and 20 characters

        StringBuilder nameBuilder = new StringBuilder();
        for (int i = 0; i < nameLength; i++) {
            char randomChar = (char) (random.nextInt(26) + 'a'); // Generates a random lowercase letter
            nameBuilder.append(randomChar);
        }

        return nameBuilder.toString();
    }

}