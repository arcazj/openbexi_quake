package org.openbexi.volcano;

// see :
// https://volcanoes.usgs.gov/vsc/api/volcanoApi/volcanoesGVP
//
// https://volcanoes.usgs.gov/vsc/api/volcanoMessageApi/
// https://volcanoes.usgs.gov/hans-public/api/notice/getNewestOrRecent
//
// https://volcanoes.usgs.gov/vsc/api/volcanoMessageApi/volcanoNewest/332010
// https://volcanoes.usgs.gov/vsc/api/volcanoApi/vhpstatus/311240
//
// https://volcanoes.usgs.gov/vsc/api/volcanoApi/volcanoesUS
//

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

public class VolcanoJsonConverter {

    public static void main(String[] args) {
        try {
            Map<String, String> parsedArgs = parseArgs(args);

            if (parsedArgs.containsKey("NewestOrRecent")) {
                String jsonInput = fetchJsonFromUrl("https://volcanoes.usgs.gov/hans-public/api/notice/getNewestOrRecent");
                Map<String, JSONArray> eventsByFilePath = processJson(jsonInput, "NewestOrRecent");
                saveEventsToFile(eventsByFilePath);
            } else if (parsedArgs.containsKey("all")) {
                String jsonInput = fetchJsonFromUrl("https://volcanoes.usgs.gov/vsc/api/volcanoApi/volcanoesUS");
                processAllVolcanoes(jsonInput);
            } else {
                printUsageAndExit();
            }

            System.out.println("All events processed and saved to respective files.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsedArgs = new HashMap<>();

        if (args.length == 1 && "-NewestOrRecent".equals(args[0])) {
            parsedArgs.put("NewestOrRecent", "true");
        } else if (args.length == 1 && "-all".equals(args[0])) {
            parsedArgs.put("all", "true");
        } else {
            printUsageAndExit();
        }

        return parsedArgs;
    }

    private static void processAllVolcanoes(String jsonInput) throws IOException {
        JSONArray volcanoes = new JSONArray(jsonInput);

        for (int i = 0; i < volcanoes.length(); i++) {
            JSONObject volcano = volcanoes.getJSONObject(i);
            String vnum = volcano.getString("vnum");
            String volcanoData = fetchJsonFromUrl("https://volcanoes.usgs.gov/hans-public/api/notice/getNewestOrRecent/" + vnum);
            if (volcanoData.length() > 2) {
                Map<String, JSONArray> eventsByFilePath = processJson(volcanoData, "volcano");
                saveEventsToFile(eventsByFilePath);
            }
        }
    }

    private static Map<String, JSONArray> processJson(String jsonInput, String type) {
        JSONArray notices = new JSONArray(jsonInput);

        Map<String, JSONArray> eventsByFilePath = new HashMap<>();

        for (int i = 0; i < notices.length(); i++) {
            JSONObject notice = notices.getJSONObject(i);
            long epochTime = notice.getLong("sent_unixtime");
            String formattedDate = convertEpochToDate(epochTime);

            String[] dateParts = formattedDate.split(" ");
            String year = dateParts[5];
            String month = getMonthNumber(dateParts[1]);
            String day = dateParts[2];

            String outputDirPath = String.format("/data/volcano/%s/%s/%s/", year, month, day);
            File outputDir = new File(outputDirPath);
            if (!outputDir.exists()) {
                outputDir.mkdirs();
            }

            String outputFilePath = String.format("%svolcano_%s_%s_%s" + "_00" + ".json", outputDirPath, year, month, day);
            JSONObject event = createEvent(notice, formattedDate);

            eventsByFilePath.computeIfAbsent(outputFilePath, k -> new JSONArray()).put(event);
        }

        return eventsByFilePath;
    }

    private static JSONObject createEvent(JSONObject notice, String formattedDate) {
        JSONObject event = new JSONObject();
        event.put("id", UUID.randomUUID().toString());
        event.put("start", formattedDate);
        event.put("end", "");
        event.put("namespace", "volcano");

        JSONObject data = new JSONObject();
        addIfNotNull(data, "description",  notice.opt("notice_url"));
        addIfNotNull(data, "noticeIdentifier", notice.opt("noticeIdentifier"));
        addIfNotNull(data, "noticeType", notice.opt("noticeType"));
        addIfNotNull(data, "noticeCategory", notice.opt("noticeCategory"));
        addIfNotNull(data, "obs", notice.opt("obs"));
        addIfNotNull(data, "obsFullname", notice.opt("obsFullname"));
        addIfNotNull(data, "volcanoes", notice.opt("volcanoes"));
        addIfNotNull(data, "notice_data", notice.opt("notice_data"));

        JSONArray sections = notice.optJSONArray("sections");
        if (sections != null && sections.length() > 0) {
            JSONObject section = sections.getJSONObject(0);
            addIfNotNull(data, "synopsis", section.opt("synopsis"));
            addIfNotNull(data, "colorCode", section.opt("colorCode"));
            addIfNotNull(data, "alertLevel", section.opt("alertLevel"));
            addIfNotNull(data, "volcanoCd", section.opt("volcanoCd"));
            addIfNotNull(data, "volcanoName", section.opt("volcanoName"));
            addIfNotNull(data, "vnum", section.opt("vnum"));

            String colorCode = section.optString("colorCode", "UNKNOWN");
            String volcanoName = section.optString("volcanoName", "Unknown Volcano");
            addIfNotNull(data, "title", colorCode + " - " + volcanoName);
            addIfNotNull(data, "type", "volcano");

            String iconPath = determineIconPath(colorCode);
            JSONObject render = new JSONObject();
            render.put("image", iconPath);
            render.put("color", "#731616");

            event.put("render", render);
        }

        event.put("data", data);

        return event;
    }

    private static String determineIconPath(String colorCode) {
        return switch (colorCode.toUpperCase()) {
            case "RED" -> "icon/ob_volcano_very_active.png";
            case "ORANGE" -> "icon/ob_volcano_active.png";
            case "YELLOW" -> "icon/ob_volcano.png";
            default -> "icon/ob_volcano_no_active.png";
        };
    }

    private static void saveEventsToFile(Map<String, JSONArray> eventsByFilePath) throws IOException {
        for (Map.Entry<String, JSONArray> entry : eventsByFilePath.entrySet()) {
            String filePath = entry.getKey();
            JSONArray events = entry.getValue();

            JSONObject outputJson = new JSONObject();
            outputJson.put("dateTimeFormat", "iso8601");
            outputJson.put("events", events);

            saveJsonToFile(filePath, outputJson);

            System.out.println("Event saved to: " + filePath + " with " + events.length() + " events.");
        }
    }

    private static void printUsageAndExit() {
        System.out.println("Usage:");
        System.out.println("  -NewestOrRecent");
        System.out.println("    Fetch the newest or most recent volcano notices.");
        System.out.println("  -all");
        System.out.println("    Fetch and process all volcano data from USGS.");
        System.exit(1);
    }

    private static String convertEpochToDate(long epochTime) {
        Date date = new Date(epochTime * 1000);
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss 'UTC' yyyy", Locale.US);
        return sdf.format(date);
    }

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

    private static void saveJsonToFile(String filePath, JSONObject jsonObject) throws IOException {
        try (FileWriter file = new FileWriter(filePath)) {
            file.write(jsonObject.toString(4));
        }
    }

    private static void addIfNotNull(JSONObject jsonObject, String key, Object value) {
        if (value != null) {
            jsonObject.put(key, value);
        }
    }

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
}
