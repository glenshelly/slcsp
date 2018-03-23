package com.glen.slcsp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Find the Slcsp for a set of zip codes in a given file.
 * <p>
 * The functionality in the process() method of this class uses data in 3 input files (location specifed
 * by a program arguement), and writes the results to the same directory, overwriting one of the input files.
 * <p>
 * A full description of the expected funcdtionality can be found here: https://github.com/adhocteam/homework/tree/master/slcsp
 *
 * Here's a summary of the approach:
 *
 * 1. Marshal the zipcodes we need to find values for into a List
 *      - simple reading in of slcsp.csv (conains zip codes)
 *
 * 2. Marshal the SLCSP values for area codes into a map
 *      - reading in of plans.csv (contains plans-details & rate area codes)
 *      - filter out values we don't care about (e.g., anything that's not the slcsp)
 *
 * 3. Marshal the SLCSP values for the zip codes we care about into a map
 *      - reading in of zips.csv (contains rate area codes and zip codes)
 *      - filter out values we don't care about (e.g., zip codes with more than one rate area)
 *
 * 4. Loop through the initial zipcodes we care about, and write out the results
 */
@SuppressWarnings("ALL")
public class SlcspFinder {

    private static final String SILVER_PLAN_IDENTIFYING_NAME = "Silver";

    /**
     * Required single argument: file location
     *
     * @param args
     */
    public static void main(String args[]) {
        if (args == null || args.length < 1) {
            throw new IllegalStateException("The location of the data must be specified as the first argument.");
        }
        SlcspFinder slcspFinder = new SlcspFinder();
        String baseDirWithFinalSeparator = args[0].endsWith(File.separator) ? args[0] : args[0] + File.separator;
        slcspFinder.process(baseDirWithFinalSeparator);
    }


    /**
     * Find the SLCSP.  Write out the results to the same file as the original input was in.
     *
     * @param baseDirWithFinalSeparator the location for the input files
     */
    private void process(String baseDirWithFinalSeparator) {

        long start = System.currentTimeMillis();
        String inputAndOutputFileName = "slcsp.csv";

        /*
         * 1. Marshal the zipcodes we need to find values for into a List
         *      - simple reading in of slcsp.csv (conains zip codes)
         */
        List<String> slcspInputList = buildInputList(baseDirWithFinalSeparator, inputAndOutputFileName);


        /*
         * 2. Marshal the SLCSP values for area codes into a map
         *      - reading in of plans.csv (contains plans-details & rate area codes)
         *      - filter out values we don't care about (e.g., anything that's not the slcsp)
         */
        Map<String, Float> rateAreaToSlcspMap = buildRateAreaToSlcspMap(baseDirWithFinalSeparator);


        /*
         * 3. Marshal the SLCSP values for the zip codes we care about into a map
         *      - reading in of zips.csv (contains rate area codes and zip codes)
         *      - filter out values we don't care about (e.g., zip codes with more than one rate area)
         */
        Map<String, Float> zipToSlcspMap = buildZipToSlcspPriceMap(baseDirWithFinalSeparator, slcspInputList, rateAreaToSlcspMap);


        /*
         * 4. Loop through the initial zipcodes we care about, and write out the results
         */
        writeResults(baseDirWithFinalSeparator, inputAndOutputFileName, slcspInputList, zipToSlcspMap);


        renderMessage("\nComplete in " + (System.currentTimeMillis() - start) + "ms: Results written to: " + baseDirWithFinalSeparator + inputAndOutputFileName + "\n");
    }


    /**
     * @param baseDir
     * @param inputFileName
     * @return an ordered List of the input strings (zip codes)
     */
    private List<String> buildInputList(String baseDir, String inputFileName) {
        List<String> slcspList = new ArrayList<>();
        String filespec = baseDir + inputFileName;
        try (
                BufferedReader br = new BufferedReader(new FileReader(filespec))
        ) {
            br.readLine();   // skip the first header line
            String line;
            while ((line = br.readLine()) != null) {
                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1);
                }
                slcspList.add(line);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problem loading file ('" + filespec + "'): " + e);
        }
        return slcspList;
    }

    /**
     * Get the (possible) slcsp by rate area
     * <p>
     * It is possible that the value associated with a particular rate area is null, indicating that there was no slcsp
     *
     * @param baseDir
     * @return a rate area map, with key=rate area (State + Number) and value=slcsp for that rate area (if any)
     */
    private Map<String, Float> buildRateAreaToSlcspMap(String baseDir) {
        Map<String, Float> rateAreaMap;
        String fileSpec = baseDir + "plans.csv";
        try (
                // The stream will initially include the header row, but that entry will filtered out, since its
                // 'rate area' won't have 2 plans.
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            // 1. Group the CostData by RateArea information into an interim map...
            Map<String, Set<RateAreaPlanCostData>> rateAreaToMultiplePlanMap = stream
                    .map(this::parseInputStringIntoSilverPlanObject)
                    .filter(Objects::nonNull)
                    .collect(Collectors.groupingBy(RateAreaPlanCostData::getRateAreaCode, Collectors.toSet()));
                    // Collecting the RateAreaPlanCostData objects into a set eliminates any duplicate plans
                    // (in this case, a duplicate plan is defined as a plan that has the same cost)

            // 2. ...then get the the 2nd lowest plan (if any) for the Rate Area
            rateAreaMap = rateAreaToMultiplePlanMap.entrySet().stream()
                    .filter(entrySet -> entrySet.getValue().size() >= 2)
                    .collect(Collectors.toMap(Map.Entry::getKey, x -> getSecondLowestPlanCost(x.getValue()), (a, b) -> b));
        } catch (IOException e) {
            throw new IllegalStateException("Problem loading rateArea file ('" + fileSpec + "'): " + e);
        }

        return rateAreaMap;
    }

    /**
     * Get the second lowest plan cost
     *
     * @param rateAreaPlanCostDataSet will not have duplicates
     * @return the 2nd lowest plan cost.  Null if unable to determine.
     */
    private Float getSecondLowestPlanCost(Set<RateAreaPlanCostData> rateAreaPlanCostDataSet) {
        ArrayList<RateAreaPlanCostData> rateAreaPlanCostDataList = new ArrayList<>(rateAreaPlanCostDataSet);
        Collections.sort(rateAreaPlanCostDataList);
        return rateAreaPlanCostDataList.get(1).planCost;
    }


    /**
     * Get the final map, linking the zip codes to a slcsp price.
     *
     * @param baseDir
     * @param slcspInputList
     * @param rateAreaToSlcspMap    may contain null values
     * @return a map with key=zipcode and value=the slcsp price.  Will only return items for which the slcsp was determined.
     */
    private Map<String, Float> buildZipToSlcspPriceMap(String baseDir, List<String> slcspInputList,
                                                       Map<String, Float> rateAreaToSlcspMap) {
        Set<String> slcspSet = new HashSet<>(slcspInputList);
        Map<String, Float> zipToSlcspMap = new HashMap<>();
        String fileSpec = baseDir + "zips.csv";
        try (
                // The stream will include the header row, it will be filtered out below: it won't include a valid zip.
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            // 1. Group the ZipRateAreaData objects by zip code into an interim map...
            Map<String, Set<ZipRateAreaData>> zipGroupedMapByRateArea = stream
                    .map(this::parseInputStringIntoZipToRateObject)
                    .filter(x -> rateAreaToSlcspMap.get(x.rateAreaCode) != null)
                    .filter(x -> slcspSet.contains(x.getZip()))
                    .collect(Collectors.groupingBy(ZipRateAreaData::getZip, Collectors.toSet()));
                    // Collecting the ZipRateAreaData objects into a Set eliminates any duplicates.
                    // (in this case, a duplicate ZipRateArea is one that has an identical Zip and Rate area,
                    //  even if they have the different counties)

            // 2.  ...then get the second-lowest-cost for particular zip codes.
            for (String zipCode : zipGroupedMapByRateArea.keySet()) {
                Set<ZipRateAreaData> rateAreasForSingleZipcode = zipGroupedMapByRateArea.get(zipCode);
                // Skip if there's more than one rate area represented for the single zip code
                if (rateAreasForSingleZipcode.size() == 1) {
                    // Get the single rate area, and see if there's a cost associated with it.  If so, add a map entry
                    String rateAreaForZipcode = rateAreasForSingleZipcode.iterator().next().getRateAreaCode();
                    Float secondLowestForArea = rateAreaToSlcspMap.get(rateAreaForZipcode);
                    if (null != secondLowestForArea) {
                        zipToSlcspMap.put(zipCode, secondLowestForArea);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem loading file ('" + fileSpec + "'): " + e);
        }
        return zipToSlcspMap;
    }


    /**
     * Write the output to the specified location.  The output must be written in the same order
     *
     * @param baseDirWithFinalSeparator
     * @param inputAndOutputFile
     * @param slcspInputList            The ordered input list.  The output will be written to
     * @param zipToSlcspMap
     */
    private void writeResults(String baseDirWithFinalSeparator, String inputAndOutputFile, List<String> slcspInputList,
                              Map<String, Float> zipToSlcspMap) {
        String filespec = baseDirWithFinalSeparator + inputAndOutputFile;
        try (
                OutputStream resultOutputStream = new FileOutputStream(filespec);
                PrintWriter resultPrintWriter = new PrintWriter(new OutputStreamWriter(resultOutputStream, "UTF-8"))
        ) {
            resultPrintWriter.println("zipcode,rate");
            slcspInputList.stream()
                    .map(x -> x + "," + (zipToSlcspMap.containsKey(x) ? zipToSlcspMap.get(x) : ""))
                    .forEach(resultPrintWriter::println);
        } catch (IOException e) {
            throw new IllegalStateException("Problem writing file ('" + filespec + "'): " + e);
        }
    }


    private void renderMessage(String msg) {
        // swap out for log4j, or some such logging mechanism....
        System.out.println(msg);
    }

    /**
     * @param inputString
     * @return an object based on the input String, if it's a Silver plan; else, null
     */
    private RateAreaPlanCostData parseInputStringIntoSilverPlanObject(String inputString) {
        /*
            plan_id,state,metal_level,rate,rate_area
            74449NR9870320,GA,Silver,298.62,7
        */
        // StringTokenizer would be prettier.  And slower
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        String planMetalType = inputString.substring(secondComma + 1, thirdComma);
        boolean isSilver = SILVER_PLAN_IDENTIFYING_NAME.equals(planMetalType);

        return isSilver ? new RateAreaPlanCostData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                Float.parseFloat(inputString.substring(thirdComma + 1, fourthComma))) : null;
    }

    /**
     * @param inputString
     * @return an object based on the input String.  Will not return null.
     */
    private ZipRateAreaData parseInputStringIntoZipToRateObject(String inputString) {
        /*
            zipcode,state,county_code,name,rate_area
            36749,AL,01001,Autauga,11
         */
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        return new ZipRateAreaData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                inputString.substring(0, firstComma));
    }


    /**
     * Holds a rate-area to plan-cost relationship
     */
    class RateAreaPlanCostData implements Comparable<RateAreaPlanCostData> {
        final float planCost;
        final String rateAreaCode;

        RateAreaPlanCostData(String rateAreaCode, float planCost) {
            this.rateAreaCode = rateAreaCode;
            this.planCost = planCost;
        }

        String getRateAreaCode() {
            return rateAreaCode;
        }

        /*
         * Note that we only compare planCost here - this allows sets of this object to only hold a single item of
         * any particular cost
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RateAreaPlanCostData that = (RateAreaPlanCostData) o;
            return Objects.equals(planCost, that.planCost);
        }

        @Override
        public int hashCode() {
            return Objects.hash(planCost);
        }

        @Override
        public int compareTo(RateAreaPlanCostData givenSpd) {
            return Float.compare(this.planCost, givenSpd.planCost);
        }
    }

    /**
     * Holds a zip-code to rate-area relationship
     */
    class ZipRateAreaData {
        final String zip;
        final String rateAreaCode;

        ZipRateAreaData(String rateAreaCode, String zip) {
            this.rateAreaCode = rateAreaCode;
            this.zip = zip;
        }

        String getZip() {
            return zip;
        }

        String getRateAreaCode() {
            return rateAreaCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ZipRateAreaData that = (ZipRateAreaData) o;
            return Objects.equals(zip, that.zip) &&
                    Objects.equals(rateAreaCode, that.rateAreaCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zip, rateAreaCode);
        }

        @Override
        public String toString() {
            return zip + ':' + rateAreaCode;
        }
    }


}
