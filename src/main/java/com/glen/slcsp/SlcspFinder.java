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
 * Find the Slcsp for given zip codes.
 *
 * This implementation assumes the given limited use case, namely, a file with (only) 51 zip codes that we want to find the
 * data for.
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

        // Read in the input zipcodes from slcsp.csv - it's these codes we'll want to find the SLCSP for
        List<String> slcspInputList = buildInputList(baseDirWithFinalSeparator, inputAndOutputFileName);

        // Get the relevant (silver) rateAreas from plans.sv. 
        // Map.key=rateArea, map.value=slcsp.
        Map<String, Float> rateAreaMap = buildRateAreaToSlcspMap(baseDirWithFinalSeparator);

        // Build the final zip:slcsp map, using previous data and zips.csv. 
        // Map.key=zip code, map.value=matching slcsp
        Map<String, Float> zipToSlcspMap = buildZipToSlcspPriceMap(baseDirWithFinalSeparator, slcspInputList, rateAreaMap);

        // Overwrite the input file with the results
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
     * Return slcsp, by rate area (State + number)
     * <p>
     * Note that in rare cases, the value could be null, indicating that there was no slcsp
     *
     * @param baseDir
     * @return a rate area map, with key=the rate area (State + Number) and value=2nd lowest silver-plan cost for that area.
     */
    private Map<String, Float> buildRateAreaToSlcspMap(String baseDir) {
        Map<String, Float> rateAreaMap;
        String fileSpec = baseDir + "plans.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            /*
              1. Group the CostData by RateArea infor into an interim map...

                 (The stream includes the header row, but it will filtered out below: its 'rate area' won't have 2 plans.
                 Collecting the RateAreaPlanCostData objects into a set eliminates any duplicate plans (in this case, plans with the same cost)
            */
            Map<String, Set<RateAreaPlanCostData>> rateAreaToMultiplePlanMap = stream
                    .map(this::parseInputStringIntoSilverPlanObject)
                    .filter(Objects::nonNull)
                    .collect(Collectors.groupingBy(RateAreaPlanCostData::getRateAreaCode, Collectors.toSet()));

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
     * Get the final necessary map, linking the zip codes to a slcsp price.
     * @param baseDir
     * @param slcspInputList
     * @param rateAreaMap    may contain null values
     * @return a map with key=zipcode and value=the slcsp price.  Will only return items for which the slcsp was determined.
     */
    private Map<String, Float> buildZipToSlcspPriceMap(String baseDir, List<String> slcspInputList,
                                                       Map<String, Float> rateAreaMap) {
        Set<String> slcspSet = new HashSet<>(slcspInputList);
        Map<String, Float> zipToSlcspMap = new HashMap<>();
        String fileSpec = baseDir + "zips.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            /*
             1. Group the ZipRateAreaData objects by zip code into an interim map...

                (The stream will include the header row, it will be filtered out below: it won't include a valid zip.
                Collecting the ZipRateAreaData objects into a Set eliminates any duplicates.)
            */
            Map<String, Set<ZipRateAreaData>> zipGroupedMapByRateArea = stream
                    .map(this::parseInputStringIntoZipToRateObject)
                    .filter(x -> rateAreaMap.get(x.rateAreaCode) != null)
                    .filter(x -> slcspSet.contains(x.getZip()))
                    .collect(Collectors.groupingBy(ZipRateAreaData::getZip, Collectors.toSet()));

            // 2.  ... then get the second-lowest-cost for particular zip codes.
            for (String zipCode : zipGroupedMapByRateArea.keySet()) {
                Set<ZipRateAreaData> rateAreasForSingleZipcode = zipGroupedMapByRateArea.get(zipCode);
                // Skip if there's more than one rate area represented for the single zip code
                if (rateAreasForSingleZipcode.size() == 1) {
                    String areaCodeForZip = rateAreasForSingleZipcode.iterator().next().getRateAreaCode();
                    Float secondLowestForArea = rateAreaMap.get(areaCodeForZip);
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
     *
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
     *
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
