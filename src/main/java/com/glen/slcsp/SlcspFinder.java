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
 * This class uses data found in 3 input files and writes the results to the same directory,
 * overwriting one of the input files.  The location of that directory is specified by a program argument.
 * <p>
 * A full description of the expected functionality can be found here: https://github.com/adhocteam/homework/tree/master/slcsp
 * <p>
 * The process is initiated by calling the process() method in this class
 */

public class SlcspFinder {

    private static final String SILVER_PLAN_IDENTIFYING_NAME = "Silver";

    /**
     * Required single argument: file location
     *
     * @param args command line arguments.  A single argument is expected, namely, the directory location of the input files
     */
    public static void main(String args[]) {
        if (args == null || args.length < 1) {
            throw new IllegalStateException("The location of the data must be specified as the first argument.");
        }
        SlcspFinder slcspFinder = new SlcspFinder();

        slcspFinder.process(args[0]);
    }


    /**
     * Find the SLCSP.  Write out the results to the same file as the original input was in.
     *
     * @param baseDir the location for the input files
     */
    public void process(String baseDir) {

        String baseDirWithFinalSeparator = baseDir.endsWith(File.separator) ? baseDir : baseDir + File.separator;

        long start = System.currentTimeMillis();
        String inputAndOutputFileName = "slcsp.csv";

        /*
         * 1. Read in the zipcodes we are tasked to find values for, into a List
         *      - Data in slcsp.csv
         */
        final String inputOutputFileSpec = baseDirWithFinalSeparator + inputAndOutputFileName;
        final List<String> slcspInputList = buildInputList(inputOutputFileSpec);


        /*
         * 2. Marshal the rate area codes to SLCSP values into a map
         *      - Data in plans.csv (contains plans-details and rate area codes)
         *      - Filter out values we don't care about (e.g., anything that's not the slcsp)
         */
        final String plansPileSpec = baseDirWithFinalSeparator + "plans.csv";
        final Map<String, Float> rateAreaToSlcspMap = buildRateAreaToSlcspMap(plansPileSpec);


        /*
         *  3. Marshal the zip code to sets-of-rate area daa into a map
         *      - Data in zips.csv (contains rate area codes and zip codes)
         *      - Filter out values we don't care about (e.g., zip codes not in the input file, or rate areas not
         *        found in the previous step)
         */
        final String zipsFileSpec = baseDirWithFinalSeparator + "zips.csv";
        final Map<String, Set<ZipRateAreaData>> zipToRateAreaSetMap = buildZipToRateAreaSetMap(zipsFileSpec, slcspInputList, rateAreaToSlcspMap);


        /*
         * 4. Using the gathered data, create a final map linking the zip codes to the SLCSPs
         *      - Filter out values we don't care about (e.g., zip codes with more than one rate area)
         */
        final Map<String, Float> zipToSlcspMap = buildFinalZipToSlcspPriceMap(zipToRateAreaSetMap, rateAreaToSlcspMap);


        /*
         * 5. Loop through the list of input zipcodes and write out the results, using the final zip code to SLCSP map
         */
        writeResults(inputOutputFileSpec, slcspInputList, zipToSlcspMap);


        renderMessage("\nComplete in " + (System.currentTimeMillis() - start) + "ms: Results written to: "
                + baseDirWithFinalSeparator + inputAndOutputFileName + "\n");
    }


    /**
     * Read in the input file with zip codes.  It's these zip codes that we want to find the SLCSP, if any
     *
     * @param fileSpec the full filespec of the input file to read in
     * @return an ordered List of the input strings (zip codes)
     */
    private List<String> buildInputList(final String fileSpec) {
        final List<String> slcspList = new ArrayList<>();
        try (
                final BufferedReader br = new BufferedReader(new FileReader(fileSpec))
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
            throw new IllegalStateException("Problem loading file ('" + fileSpec + "'): " + e);
        }
        return slcspList;
    }

    /**
     * Get the (possible) slcsp by rate area, using data in plans.csv
     * <p>
     * It is possible that the value associated with a particular rate area will be null, indicating that there was no slcsp
     *
     * @param fileSpec the full filespec of the plans.csv file to read in
     * @return a rate area map, with key=rate area (State + Number) and value=slcsp for that rate area (if any).
     * The value may be null.
     */
    private Map<String, Float> buildRateAreaToSlcspMap(final String fileSpec) {
        final Map<String, Float> rateAreaMap;
        try (
                // The stream will initially include the header row, but that entry will filtered out, since its
                // 'rate area' won't have 2 plans.
                final Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            // 1. Group the CostData-by-RateArea information into an interim map, linked to a single rate area..
            final Map<String, Set<RateAreaPlanCostData>> rateAreaToMultiplePlanMap = stream
                    .map(this::parseInputStringIntoSilverPlanObject)
                    .filter(Objects::nonNull)
                    .collect(Collectors.groupingBy(RateAreaPlanCostData::getRateAreaCode, Collectors.toSet()));
            // Collecting the RateAreaPlanCostData objects into a set eliminates any duplicate plans
            // (in this case, a duplicate plan is defined as a plan that has the same cost)

            // 2. ...then get the the 2nd lowest silver plan (if any) for the Rate Area into the final map
            rateAreaMap = rateAreaToMultiplePlanMap.entrySet().stream()
                    .filter(entrySet -> entrySet.getValue().size() >= 2)
                    .collect(Collectors.toMap(Map.Entry::getKey, x -> getSecondLowestPlanCost(x.getValue()), (a, b) -> b));
        } catch (IOException e) {
            throw new IllegalStateException("Problem loading rateArea file ('" + fileSpec + "'): " + e);
        }

        return rateAreaMap;
    }


    /**
     * Get the rate areas for a zip code, using the data in zips.csv.  Do not return zipcode info for zipcodes
     * not in the given slcspInputList,  Also, do not return any rate area data for rate areas not found in the given
     * rateArea map
     *
     * @param fileSpec           the full filespec of the zips.csv file to read in
     * @param slcspInputList     The original input list of zip codes.  Used to limit the end result;
     *                           we'll only return the data we need
     * @param rateAreaToSlcspMap a map associating rate areas with (possible) SLCSPs.  Used to limit the end result;
     *                           we'll only return the data we need
     * @return a Map of zipcodes-to-Sets of RateArea items
     */
    private Map<String, Set<ZipRateAreaData>> buildZipToRateAreaSetMap(final String fileSpec, final List<String> slcspInputList,
                                                                       final Map<String, Float> rateAreaToSlcspMap) {
        final Map<String, Set<ZipRateAreaData>> zipGroupedMapByRateArea;
        try (
                // The stream will include the header row.  It will be filtered out below, since it won't include a valid zip.
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            /*
                Group the ZipRateAreaData objects by zip code into an interim map

                Note: Collecting the ZipRateAreaData objects into a Set eliminates any duplicates.
                (in this case, a duplicate ZipRateArea is one that has an identical Zip and Rate area,
                even if they have different counties)
            */

            final Set<String> slcspSet = new HashSet<>(slcspInputList);
            zipGroupedMapByRateArea = stream
                    .map(this::parseInputStringIntoZipToRateObject)
                    .filter(x -> rateAreaToSlcspMap.get(x.rateAreaCode) != null)
                    .filter(x -> slcspSet.contains(x.getZip()))
                    .collect(Collectors.groupingBy(ZipRateAreaData::getZip, Collectors.toSet()));
        } catch (IOException e) {
            throw new IllegalStateException("Problem loading file ('" + fileSpec + "'): " + e);
        }
        return zipGroupedMapByRateArea;
    }


    /**
     * Get the final map, linking the zip codes to a slcsp price.
     *
     * @param zipGroupedMapByRateArea a map containing zip codes pointing to sets of ZipRateAreaData objects
     * @param rateAreaToSlcspMap      a mpa containing rate areas pointing to SLCSP prices.  Some of the values in
     *                                this map may be null.
     * @return a map with key=zipcode and value=the slcsp price.  Will only return items for which the slcsp was determined.
     */
    private Map<String, Float> buildFinalZipToSlcspPriceMap(final Map<String, Set<ZipRateAreaData>> zipGroupedMapByRateArea,
                                                            final Map<String, Float> rateAreaToSlcspMap) {
        final Map<String, Float> zipToSlcspMap = new HashMap<>();
        for (String zipCode : zipGroupedMapByRateArea.keySet()) {
            final Set<ZipRateAreaData> rateAreasForSingleZipcode = zipGroupedMapByRateArea.get(zipCode);
            // Skip if there's more than one rate area represented for the single zip code
            if (rateAreasForSingleZipcode.size() == 1) {
                // Only add an entry if there's a cost associated with the rate area
                final String rateAreaForZipcode = rateAreasForSingleZipcode.iterator().next().getRateAreaCode();
                final Float secondLowestForArea = rateAreaToSlcspMap.get(rateAreaForZipcode);
                if (null != secondLowestForArea) {
                    zipToSlcspMap.put(zipCode, secondLowestForArea);
                }
            }
        }
        return zipToSlcspMap;
    }


    /**
     * Write the output to the specified location.  The output must be written in the same order
     *
     * @param fileSpec       the full filespec of where to write the results
     * @param slcspInputList The ordered input list.
     * @param zipToSlcspMap  a map of zip codes pointing to SLCSP values determined for those zip codes.  May or may
     *                       not contain all the zip codes that are in the given slcspInputList.
     */
    private void writeResults(final String fileSpec, final List<String> slcspInputList,
                              final Map<String, Float> zipToSlcspMap) {
        try (
                final OutputStream resultOutputStream = new FileOutputStream(fileSpec);
                final PrintWriter resultPrintWriter = new PrintWriter(new OutputStreamWriter(resultOutputStream, "UTF-8"))
        ) {
            resultPrintWriter.println("zipcode,rate");
            slcspInputList.stream()
                    .map(x -> x + "," + (zipToSlcspMap.containsKey(x) ? zipToSlcspMap.get(x) : ""))
                    .forEach(resultPrintWriter::println);
        } catch (IOException e) {
            throw new IllegalStateException("Problem writing file ('" + fileSpec + "'): " + e);
        }
    }



    /**
     * Get the second lowest plan cost
     *
     * @param rateAreaPlanCostDataSet will not have duplicates
     * @return the 2nd lowest plan cost.  Null if unable to determine.
     */
    private Float getSecondLowestPlanCost(final Set<RateAreaPlanCostData> rateAreaPlanCostDataSet) {
        final List<RateAreaPlanCostData> rateAreaPlanCostDataList = new ArrayList<>(rateAreaPlanCostDataSet);
        Collections.sort(rateAreaPlanCostDataList);
        return rateAreaPlanCostDataList.get(1).planCost;
    }

    private void renderMessage(final String msg) {
        // swap out for log4j, or some such logging mechanism....
        System.out.println(msg);
    }

    /**
     * Get a RateAreaPlanCostData object corresponding to the given string
     * @param inputString data from a file that will be parsed and transformed into the return object
     * @return an object based on the input String, if it's a Silver plan; else, null
     */
    private RateAreaPlanCostData parseInputStringIntoSilverPlanObject(final String inputString) {
        /*
            plan_id,state,metal_level,rate,rate_area
            74449NR9870320,GA,Silver,298.62,7
        */
        // StringTokenizer would be prettier.  And slower.
        final char delimiter = ',';
        final int firstComma = inputString.indexOf(delimiter);
        final int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        final int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        final int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        final String planMetalType = inputString.substring(secondComma + 1, thirdComma);
        final boolean isSilver = SILVER_PLAN_IDENTIFYING_NAME.equals(planMetalType);

        return isSilver ? new RateAreaPlanCostData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                Float.parseFloat(inputString.substring(thirdComma + 1, fourthComma))) : null;
    }

    /**
     * Get a ZipRateAreaData object corresponding to the given string
     * @param inputString data from a file that will be parsed and transformed into the return object
     * @return an object based on the input String.  Will not return null.
     */
    private ZipRateAreaData parseInputStringIntoZipToRateObject(final String inputString) {
        /*
            zipcode,state,county_code,name,rate_area
            36749,AL,01001,Autauga,11
         */
        final char delimiter = ',';
        final int firstComma = inputString.indexOf(delimiter);
        final int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        final int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        final int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        return new ZipRateAreaData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                inputString.substring(0, firstComma));
    }


    /**
     * Holds a rate-area to plan-cost relationship
     */
    private class RateAreaPlanCostData implements Comparable<RateAreaPlanCostData> {
        final float planCost;
        final String rateAreaCode;

        RateAreaPlanCostData(final String rateAreaCode, final float planCost) {
            this.rateAreaCode = rateAreaCode;
            this.planCost = planCost;
        }

        String getRateAreaCode() {
            return rateAreaCode;
        }

        /*
         * Note that we only compare planCost here; a Set of these objects will therefore not contain two items with same cost
         */
        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RateAreaPlanCostData that = (RateAreaPlanCostData) o;
            return Objects.equals(planCost, that.planCost);
        }

        @Override
        public int hashCode() {
            return Objects.hash(planCost);
        }

        @Override
        public int compareTo(final RateAreaPlanCostData givenSpd) {
            return Float.compare(this.planCost, givenSpd.planCost);
        }
    }

    /**
     * Holds a zip-code to rate-area relationship
     */
    class ZipRateAreaData {
        final String zip;
        final String rateAreaCode;

        ZipRateAreaData(final String rateAreaCode, final String zip) {
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
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ZipRateAreaData that = (ZipRateAreaData) o;
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
