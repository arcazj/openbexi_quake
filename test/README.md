# Converter regression tests

## Volcano

Run from the project root with JDK 17 and the `org.json:json:20210307` dependency declared in `pom.xml`. In PowerShell:

```powershell
$jsonJar = "$env:USERPROFILE\.m2\repository\org\json\json\20210307\json-20210307.jar"
javac -encoding UTF-8 -cp $jsonJar -d target/volcano-test-classes src/org/openbexi/volcano/VolcanoJsonConverter.java test/org/openbexi/volcano/VolcanoJsonConverterTest.java
java -cp "target/volcano-test-classes;$jsonJar" org.openbexi.volcano.VolcanoJsonConverterTest
```

The tests use a local HTTP server and temporary output directories. They cover archive pagination, overlapping pages, historical dates, UTC day boundaries, merging existing daily files, repeat imports, preserving recent-feed metadata, empty archives, truncated responses, repeated pages, and HTTP errors. They also assert HTTP request counts and date bounds to verify that completed ranges are skipped, empty intervals need no page requests, and interrupted imports resume from the first unsaved page. They make no requests to USGS.

## Historical import

After compiling, run the converter with `-all` to fetch the available USGS HANS notice archive:

```powershell
java -cp "target/volcano-test-classes;$jsonJar" org.openbexi.volcano.VolcanoJsonConverter -all
```

Output remains under `/data/volcano/YYYY/MM/DD/`. The first import searches all observatories and notice types, with no starting date. The ending date is frozen at the last complete second before the run. It reads pages until an empty page and checks the imported count against the archive's preflight count.

Progress is saved atomically in `/data/volcano/.archive-progress.json` after each page's daily files are written:

- Completed date ranges are excluded from later queries. Subsequent runs request only newer dates.
- If there is no newer interval to check, the run makes no HTTP requests. If a new interval has no notices, only the preflight count is queried.
- Interrupted imports reuse their original query, count, and next page number, then check for newer notices. A crash between saving events and saving the checkpoint can cause the last page to be requested again; merging by notice identifier prevents duplicates.
- Daily files created before checkpoint support need one archive pass to establish completeness. A file may contain only some of a day's notices, so its presence alone cannot safely exclude that day.

Keep the checkpoint with the output files. To rebuild deleted files or refresh corrected/backdated historical notices, remove `.archive-progress.json` while the converter is stopped and run `-all` again. Errors return a nonzero exit status and leave the last saved progress available for resuming. `-NewestOrRecent` continues to query the recent feed and does not mark historical ranges complete.

This covers notices available through the [USGS HANS search API](https://volcanoes.usgs.gov/hans-public/api/search/). It is not a catalog of every historical eruption worldwide. Archive records retain the full HTML notice in `data.noticeHtml`, the original notice link, observatory, notice type code, and volcano codes. Search results do not include the recent feed's structured alert sections; existing alert metadata is preserved, and later `-NewestOrRecent` imports can enrich matching records.

## Earthquake

Compile and run the standalone tests from the project root with JDK 17:

```powershell
$jsonJar = "$env:USERPROFILE\.m2\repository\org\json\json\20210307\json-20210307.jar"
$compressJar = "$env:USERPROFILE\.m2\repository\org\apache\commons\commons-compress\1.21\commons-compress-1.21.jar"
javac -encoding UTF-8 -cp "$jsonJar;$compressJar" -d target/earthquake-test-classes 'src/org/openbexi/earthquake/EarthquakeJsonConverter .java' src/org/openbexi/earthquake/EarthquakeArchiveImporter.java test/org/openbexi/earthquake/EarthquakeJsonConverterTest.java
java -cp "target/earthquake-test-classes;$jsonJar;$compressJar" org.openbexi.earthquake.EarthquakeJsonConverterTest
```

These tests use a local HTTP server and temporary files. They cover skipping completed queries, filling gaps, remembering empty intervals, resuming failed imports, splitting a 10,001-event response, rejecting incomplete responses, UTC and leap-day/month boundaries, legacy event IDs, duplicate imports, and preserving newer event revisions. They also exercise real read timeouts, transient/permanent HTTP failures, recovery by splitting failed ranges, and the initial seven-day request window.

Run the converter using the same classpath and `org.openbexi.earthquake.EarthquakeJsonConverter`, followed by one of:

- `-all`: search from year 0001 through the current time, with recent ranges imported first. Requests start with seven days and expand after successful sparse responses, up to 3,660 days. Ranges split further when their count exceeds 10,000 events. This avoids the [USGS API's 20,000-event limit](https://earthquake.usgs.gov/fdsnws/event/1/).
- `-url starttime=2024-03-01 endtime=2024-03-02`: import the specified interval. Bare dates or ISO timestamps also work. Date-only values mean midnight UTC; endpoints are inclusive.
- `-url default`, or no arguments: import the last 48 hours through now.
- `-jsonFilePath <path>`: merge a local GeoJSON file into the hourly output files.

All network modes share `/data/earthquake/.import-progress.json`. Successfully saved ranges, including empty ones, are excluded before any count or event query. Failed ranges remain eligible for retry. Future dates are not marked complete. Output remains under `/data/earthquake/YYYY/MM/DD/earthquake_YYYY_MM_DD_HH.json`, with records merged by their USGS event ID; legacy files use `net` + `code` for matching.

Read/connect timeouts and HTTP 408, 429, 500, 502, 503, and 504 are retried up to three attempts, with one- and two-second delays. If those attempts fail for a range longer than a day, the importer splits it and reduces subsequent request sizes. Once a failing range is at most one day, it stops with an error containing the URL and attempt count. Completed ranges remain saved. Permanent HTTP errors and local output failures stop immediately. Progress output identifies each date range before its count request starts.

Existing hourly files alone do not establish completeness, and local file imports do not mark date ranges complete. Files created before this checkpoint feature need one validating import. Keep the checkpoint with the outputs. To rebuild deleted output or fetch later catalog revisions/backdated events, stop the converter, remove `.import-progress.json`, and rerun the desired range. The cache deliberately avoids rechecking completed history.
