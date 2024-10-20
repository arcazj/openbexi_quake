package org.openbexi.earthquake;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

class EarthquakeJsonConverter {

    public static void main(String[] args) {
        try {
            Map<String, String> parsedArgs = parseArgs(args);

            if (parsedArgs.containsKey("all")) {
                processAllMonths();
            } else {
                String jsonInput = getJsonInput(parsedArgs);
                Map<String, JSONArray> eventsByFilePath = processJson(jsonInput);
                saveEventsToFile(eventsByFilePath);
            }

            System.out.println("All events processed and saved to respective files.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsedArgs = new HashMap<>();

        if (args.length == 0) {
            // Default case: no arguments, use URL with current date range
            Date endDate = new Date();
            Date startDate = new Date(endDate.getTime() - (2 * 24 * 60 * 60 * 1000)); // Current date - 2 days
            parsedArgs.put("starttime", formatDate(startDate));
            parsedArgs.put("endtime", formatDate(endDate));
        } else if (args.length == 2 && "-jsonFilePath".equals(args[0])) {
            parsedArgs.put("jsonFilePath", args[1]);
        } else if (args.length == 3 && "-url".equals(args[0])) {
            parsedArgs.put("starttime", args[1]);
            parsedArgs.put("endtime", args[2]);
        } else if (args.length == 2 && "-url".equals(args[0]) && "default".equals(args[1])) {
            Date endDate = new Date();
            Date startDate = new Date(endDate.getTime() - (2 * 24 * 60 * 60 * 1000)); // Current date - 2 days
            parsedArgs.put("starttime", formatDate(startDate));
            parsedArgs.put("endtime", formatDate(endDate));
        } else if (args.length == 1 && "-all".equals(args[0])) {
            parsedArgs.put("all", "true");
        } else {
            printUsageAndExit();
        }

        return parsedArgs;
    }

    private static String getJsonInput(Map<String, String> parsedArgs) throws IOException {
        if (parsedArgs.containsKey("jsonFilePath")) {
            // Use existing code to read from jsonFilePath
            String jsonFilePath = parsedArgs.get("jsonFilePath");
            File jsonFile = new File(jsonFilePath);
            if (!jsonFile.exists()) {
                throw new FileNotFoundException("The file " + jsonFilePath + " does not exist.");
            }
            return readJsonFromFile(jsonFilePath);
        } else {
            // Fetch JSON data from the URL
            String starttime = parsedArgs.get("starttime");
            String endtime = parsedArgs.get("endtime");
            String urlString = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&" + starttime + "&" + endtime;
            return fetchJsonFromUrl(urlString);
        }
    }

    private static void processAllMonths() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2024);
        calendar.set(Calendar.MONTH, 12);

        while (true) {
            // Set the calendar to the last day of the current month
            calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));

            // Initialize endtime as the last day of the current month
            String endtime = formatDate(calendar.getTime());

            // Iterate day by day within the current month, moving backward
            while (true) {
                // Initialize starttime as the current day in the loop
                String starttime = formatDate(calendar.getTime());

                // Extract year, month, and day for file path checking
                String year = new SimpleDateFormat("yyyy").format(calendar.getTime());
                String month = new SimpleDateFormat("MM").format(calendar.getTime());
                String day = new SimpleDateFormat("dd").format(calendar.getTime());

                // Define the directory and file path
                String outputDirPath = String.format("/data/earthquake/%s/%s/%s/", year, month, day);
                String outputFilePath = String.format("%searthquake_%s_%s_%s" + "_00" + ".json", outputDirPath, year, month, day);

                // Check if all expected files exist before skipping processing
                if (new File(outputDirPath).listFiles() != null && new File(outputDirPath).listFiles().length == 24) {
                    System.out.println("All files already exist, skipping: " + outputDirPath);
                } else {
                    try {
                        // Construct the URL string with the starttime and endtime parameters
                        String urlString = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=" + starttime + "&endtime=" + endtime;
                        String jsonInput = fetchJsonFromUrl(urlString);
                        Map<String, JSONArray> eventsByFilePath = processJson(jsonInput);

                        // Ensure the key exists in the map before accessing it
                        if (!eventsByFilePath.isEmpty()) {
                            if (eventsByFilePath.containsKey(outputFilePath)) {
                                saveEventsToFile(eventsByFilePath);
                                System.out.println("Event saved to: " + outputFilePath + " with " + eventsByFilePath.get(outputFilePath).length() + " events.");
                            } else {
                                System.out.println("Expected key not found in map for: " + outputFilePath);
                                saveEventsToFile(eventsByFilePath);
                            }
                        } else {
                            System.out.println("No events found for: " + outputFilePath);
                        }
                    } catch (IOException e) {
                        // Stop processing if an error occurs
                        if (e instanceof FileNotFoundException || e.getMessage().contains("HTTP response code: 400")) {
                            System.out.println("Error 400 encountered, stopping at: " + starttime);
                            return;
                        }
                        System.out.println("Error encountered (" + e.getMessage() + "), stopping at: " + starttime);
                        return;
                    }
                }

                // Move to the previous day
                calendar.add(Calendar.DAY_OF_MONTH, -1);

                // Break the loop if we've reached the first day of the month
                if (calendar.get(Calendar.DAY_OF_MONTH) < calendar.getActualMaximum(Calendar.DAY_OF_MONTH)) {
                    // Update endtime for the next iteration (to be the end of the next period)
                    endtime = starttime;
                }

                if (calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMinimum(Calendar.DAY_OF_MONTH)) {
                    break;
                }
            }

            // Move the calendar back by one month for the next iteration
            calendar.add(Calendar.MONTH, -1);

            // Reset to the last day of the previous month
            calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));

            // Sleep for 2 seconds between each month to avoid overloading the API
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Thread was interrupted, stopping the process.");
                return;
            }
        }
    }


    private static Map<String, JSONArray> processJson(String jsonInput) {
        JSONObject inputJson = new JSONObject(jsonInput);
        JSONArray features = inputJson.getJSONArray("features");

        Map<String, JSONArray> eventsByFilePath = new HashMap<>();

        // Loop through all items under features
        for (int i = 0; i < features.length(); i++) {
            JSONObject feature = features.getJSONObject(i);
            JSONObject properties = feature.getJSONObject("properties");

            // Convert epoch time to formatted date
            long epochTime = properties.getLong("time");
            String formattedDate = convertEpochToDate(epochTime);

            // Extract the year, month, day, and hour for file naming
            String[] dateParts = formattedDate.split(" ");
            String year = dateParts[5];
            String month = getMonthNumber(dateParts[1]);
            String day = dateParts[2];
            String hour = dateParts[3].split(":")[0];

            // Create the directory path dynamically
            String outputDirPath = String.format("/data/earthquake/%s/%s/%s/", year, month, day);
            File outputDir = new File(outputDirPath);
            if (!outputDir.exists()) {
                outputDir.mkdirs();  // Create directories if they do not exist
            }

            // Define the output file path
            String outputFilePath = String.format("%searthquake_%s_%s_%s_%s.json", outputDirPath, year, month, day, hour);

            // Create the event object
            JSONObject event = createEvent(properties, epochTime, formattedDate);

            // Add the event to the appropriate file's event list
            eventsByFilePath.computeIfAbsent(outputFilePath, k -> new JSONArray()).put(event);
        }

        return eventsByFilePath;
    }

    private static JSONObject createEvent(JSONObject properties, long epochTime, String formattedDate) {
        JSONObject event = new JSONObject();
        event.put("id", UUID.randomUUID().toString());
        event.put("start", formattedDate);
        event.put("end", "");
        event.put("namespace", "earthquake");

        // Create the data object, supporting any data type and null values
        JSONObject data = new JSONObject();
        addIfNotNull(data, "description", "description_" + epochTime + " Session_" + epochTime);
        addIfNotNull(data, "title", properties.opt("title"));  // Can be String or null
        addIfNotNull(data, "mag", handleNonFiniteNumber(properties.optDouble("mag")));
        addIfNotNull(data, "place", properties.opt("place"));  // Can be String or null
        addIfNotNull(data, "time", epochTime);
        addIfNotNull(data, "updated", properties.opt("updated"));
        addIfNotNull(data, "tz", properties.opt("tz"));
        addIfNotNull(data, "url", properties.opt("url"));
        addIfNotNull(data, "detail", properties.opt("detail"));
        addIfNotNull(data, "felt", properties.opt("felt"));
        addIfNotNull(data, "cdi", properties.opt("cdi"));
        addIfNotNull(data, "mmi", properties.opt("mmi"));
        addIfNotNull(data, "alert", properties.opt("alert"));
        addIfNotNull(data, "status", properties.opt("status"));
        addIfNotNull(data, "tsunami", properties.opt("tsunami"));
        addIfNotNull(data, "sig", properties.opt("sig"));
        addIfNotNull(data, "net", properties.opt("net"));
        addIfNotNull(data, "code", properties.opt("code"));
        addIfNotNull(data, "ids", properties.opt("ids"));
        addIfNotNull(data, "sources", properties.opt("sources"));
        addIfNotNull(data, "types", properties.opt("types"));
        addIfNotNull(data, "nst", properties.opt("nst"));
        addIfNotNull(data, "dmin", handleNonFiniteNumber(properties.optDouble("dmin")));
        addIfNotNull(data, "rms", handleNonFiniteNumber(properties.optDouble("rms")));
        addIfNotNull(data, "gap", properties.opt("gap"));
        addIfNotNull(data, "magType", properties.opt("magType"));
        addIfNotNull(data, "type", properties.opt("type"));

        event.put("data", data);

        // Determine the icon based on the magnitude, handle nulls safely
        String iconPath = determineIconPath(data.optDouble("mag"));
        JSONObject render = new JSONObject();
        render.put("image", iconPath);
        render.put("color", "#731616");

        event.put("render", render);

        return event;
    }

    private static void saveEventsToFile(Map<String, JSONArray> eventsByFilePath) throws IOException {
        for (Map.Entry<String, JSONArray> entry : eventsByFilePath.entrySet()) {
            String filePath = entry.getKey();
            JSONArray events = entry.getValue();

            // Create the final JSON structure
            JSONObject outputJson = new JSONObject();
            outputJson.put("dateTimeFormat", "iso8601");
            outputJson.put("events", events);

            // Save the JSON to the appropriate file
            saveJsonToFile(filePath, outputJson);

            // Print the number of events saved
            System.out.println("Event saved to: " + filePath + " with " + events.length() + " events.");
        }
    }

    // Function to print usage instructions and exit
    private static void printUsageAndExit() {
        System.out.println("Usage:");
        System.out.println("  -jsonFilePath <jsonFilePath>");
        System.out.println("    Use an existing JSON file to process earthquake data.");
        System.out.println("  -url starttime=<yyyy-mm-dd> endtime=<yyyy-mm-dd>");
        System.out.println("    Fetch earthquake data from the USGS website for the specified date range.");
        System.out.println("  -url default");
        System.out.println("    Fetch earthquake data from the USGS website for the last 2 days.");
        System.out.println("  -all");
        System.out.println("    Iterate through each month from now into the past, fetching earthquake data until an error occurs.");
        System.out.println("If no arguments are provided, the default URL mode will be used to fetch data for the last 2 days.");
        System.exit(1);
    }

    // Function to convert epoch time to formatted date string
    private static String convertEpochToDate(long epochTime) {
        Date date = new Date(epochTime);
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss 'UTC' yyyy", Locale.US);
        return sdf.format(date);
    }

    // Function to read JSON content from a file
    private static String readJsonFromFile(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }
        reader.close();
        return stringBuilder.toString();
    }

    // Function to extract JSON content from a .7z file
    private static String extractJsonFrom7z(String filePath) throws IOException {
        File file = new File(filePath);
        SevenZFile sevenZFile = new SevenZFile(file);
        SevenZArchiveEntry entry = sevenZFile.getNextEntry();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;

        while ((len = sevenZFile.read(buffer)) > 0) {
            baos.write(buffer, 0, len);
        }

        sevenZFile.close();

        return baos.toString(StandardCharsets.UTF_8);
    }

    // Function to fetch JSON content from a URL
    private static String fetchJsonFromUrl(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("Failed to fetch data from URL. Response code: " + responseCode);
        }

        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            return content.toString();
        }
    }

    // Function to save JSON content to a file
    private static void saveJsonToFile(String filePath, JSONObject jsonObject) throws IOException {
        try (FileWriter file = new FileWriter(filePath)) {
            file.write(jsonObject.toString(4));  // Indent by 4 spaces for readability
        }
    }

    // Function to handle non-finite numbers
    private static Double handleNonFiniteNumber(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;  // Return null if the number is non-finite
        }
        return value;
    }

    // Function to add a value to a JSONObject if the value is not null
    private static void addIfNotNull(JSONObject jsonObject, String key, Object value) {
        if (value != null) {
            jsonObject.put(key, value);
        }
    }

    // Function to determine the icon path based on the magnitude value
    private static String determineIconPath(Double mag) {
        if (mag == null) return "icon/ob_earthquake_mag_1_black.png"; // Default if mag is null

        if (mag > 9) {
            return "icon/ob_red_flag.png";
        } else if (mag > 8) {
            return "icon/ob_red_flag.png";
        } else if (mag > 7) {
            return "icon/ob_red_flag.png";
        } else if (mag > 6) {
            return "icon/ob_red_flag.png";
        } else if (mag > 5) {
            return "icon/ob_yellow_flag.png";
        } else if (mag > 4) {
            return "icon/ob_yellow_flag.png";
        } else if (mag > 3) {
            return "icon/ob_yellow_flag.png";
        } else if (mag > 2) {
            return "icon/ob_green_flag.png";
        } else if (mag > 0) {
            return "icon/ob_green_flag.png";
        } else {
            return "icon/ob_green_flag.png"; // Default icon for very low or negative magnitudes
        }
    }

    // Function to get the numeric month from the month abbreviation
    private static String getMonthNumber(String monthName) {
        return switch (monthName.toLowerCase()) {
            case "jan" -> "01";
            case "feb" -> "02";
            case "mar" -> "03";
            case "apr" -> "04";
            case "may" -> "05";
            case "jun" -> "06";
            case "jul" -> "07";
            case "aug" -> "08";
            case "sep" -> "09";
            case "oct" -> "10";
            case "nov" -> "11";
            case "dec" -> "12";
            default -> throw new IllegalArgumentException("Invalid month: " + monthName);
        };
    }

    // Function to format Date to yyyy-MM-dd
    private static String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
        return sdf.format(date);
    }
}
