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
import java.util.Arrays;
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
 * Find the Slcsp for given zip codes
 */
public class SlcspFinder {

    private static final String SILVER_PLAN_IDENTIFYING_NAME = "Silver";

    /**
     * Required argument: file location
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

        String inputAndOutputFile = "slcsp.csv";

        // Read in the inpuut zipcodes from Slcsp.csv - it's these codes we'll want to find the SLCSP for
        List<String> slcspInputList = buildInputList(baseDirWithFinalSeparator, inputAndOutputFile);

        // Get the relevant (silver) rateareas from plans.sv. 
        // Map.key=ratearea, map.value=slcsp.
        Map<String, Float> rateareaMap = buildRateareaMapOfSlcsp(baseDirWithFinalSeparator);

        // Build the final zip:slcsp map, using previous data and zips.csv. 
        // Map.key=zip code, map.value=matching slcsp
        Map<String, Float> zipToSlcspMap = buildZipToSlcspPriceMap(baseDirWithFinalSeparator, slcspInputList, rateareaMap);

        // Overwrite the input file with the results
        writeOutResults(baseDirWithFinalSeparator, inputAndOutputFile, slcspInputList, zipToSlcspMap);

    }


    /**
     * @param baseDir
     * @param inputFileName
     * @return an ordered List of the input strings (zip codes)
     */
    private List<String> buildInputList(String baseDir, String inputFileName) {
        long start = System.currentTimeMillis();
        List<String> slcspList = new ArrayList<>();
        String filespec = baseDir + inputFileName;
        try (
                BufferedReader br = new BufferedReader(new FileReader(filespec))
        ) {
            /* Expected format:
                zipcode,rate
                64148,
             */
            br.readLine();   // skip the first header line
            String line;
            while ((line = br.readLine()) != null) {
                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1);
                }
                slcspList.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem loading file ('" + filespec + "'): " + e);
        }
        renderMessage((System.currentTimeMillis() - start) + "ms to read  " + slcspList.size() + " input zipcodes");
        return slcspList;
    }

    /**
     * Return slcsp, by rate area (State + number)
     * @param baseDir
     * @return a rate area map, with key=the rate area (State + Number) and value=2nd lowest cost for that area.
     * Will only include silver plans.
     */
    private Map<String, Float> buildRateareaMapOfSlcsp(String baseDir) {
        long start = System.currentTimeMillis();
        Map<String, Float> rateareaMap = new HashMap<>();
        String fileSpec = baseDir + "plans.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            // The stream will include the header row, but it will be tossed out since it's not a silver plan
            Map<String, List<SilverPlanData>> groupedMap =
                    stream
                            .map(this::parseInputStringIntoSilverPlanObject)
                            .filter(Objects::nonNull)
                            .collect(Collectors.groupingBy(SilverPlanData::getRateAreaCode));
            groupedMap.entrySet().stream()
                .filter(x -> x.getValue().size() >= 2)
                    .forEach((Map.Entry<String, List<SilverPlanData>> x) -> {
                        Collections.sort(x.getValue());
                        rateareaMap.put(x.getKey(), x.getValue().get(0).planCost);
                    });
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem loading ratearea file ('" + fileSpec + "'): " + e);
        }

        renderMessage((System.currentTimeMillis() - start) + "ms to get " + rateareaMap.size() + " rate areas in map");

        return rateareaMap;
    }


    /**
     * @param baseDir
     * @param slcspInputList
     * @param rateareaMap
     * @return a map with key=zipcode and value=the slcsp price.  Will only return items for which the slcsp was determined.
     */
    private Map<String, Float> buildZipToSlcspPriceMap(String baseDir, List<String> slcspInputList,
                                                       Map<String, Float> rateareaMap) {
        long start = System.currentTimeMillis();
        Set<String> slcspSet = new HashSet<>(slcspInputList);
        Map<String, Float> zipToSlcspMap = new HashMap<>();
        String fileSpec = baseDir + "zips.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec))
        ) {
            /*
             The stream will include the header row, but it will be tossed out, since it won't include a valid zip
             map key = zip code; map value: Set of RateAreas

             note: input file might have the same rate area in 2 zips, with different counties
                   and so the same zip will appear 2x in the file.  Putting the resulting ZipRateareaData object into
                   a Set (with appropriate .equals implementation) will eliminate this duplication)
            */
            Map<String, Set<ZipRateareaData>> zipGroupedMap =
                    stream
                            .map(this::parseInputStringIntoZipToRateObject)
                            .filter(x -> rateareaMap.get(x.rateAreaCode) != null)
                            .filter(x -> slcspSet.contains(x.getZip()))
                            .collect(Collectors.groupingBy(ZipRateareaData::getZip, Collectors.toSet() ));
            for (String zipCode : zipGroupedMap.keySet()) {
                // skip it if there's more than two rate areas represented for the single zip code
                Set<ZipRateareaData> rateAreasForSingleZipcode = zipGroupedMap.get(zipCode);
                if (rateAreasForSingleZipcode.size() == 1) {
                    String areaCodeForZip = rateAreasForSingleZipcode.iterator().next().getRateAreaCode();
                    Float secondLowestForArea = rateareaMap.get(areaCodeForZip);
                    if (null == secondLowestForArea) {
                        // this should never happen
                        renderMessage("second lowest is null, for rateAreaCode=" + areaCodeForZip);
                    } else {
                        zipToSlcspMap.put(zipCode, secondLowestForArea);
                    }
                } else {
                    // ignore it - too many plans for this single area
                    renderMessage("toss out " + rateAreasForSingleZipcode.size() + " rate areas for zip=" + zipCode + ": " + Arrays.toString(rateAreasForSingleZipcode.toArray()));
                    // toss out zip codes that have more than one area
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem loading file ('" + fileSpec + "'): " + e);
        }

        renderMessage((System.currentTimeMillis() - start) + "ms to get " + zipToSlcspMap.size() + " zip-to-cost items in map");
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
    private void writeOutResults(String baseDirWithFinalSeparator, String inputAndOutputFile, List<String> slcspInputList,
                                 Map<String, Float> zipToSlcspMap) {
        long start = System.currentTimeMillis();
        String filespec = baseDirWithFinalSeparator + inputAndOutputFile;
        try ( // output file
              OutputStream resultOutputStream = new FileOutputStream(filespec);
              PrintWriter resultPrintWriter = new PrintWriter(new OutputStreamWriter(resultOutputStream, "UTF-8")
              )) {
            resultPrintWriter.println("zipcode,rate");
            slcspInputList.stream()
                    .map(x -> x + "," + (zipToSlcspMap.containsKey(x) ? zipToSlcspMap.get(x) : ""))
                    .forEach(resultPrintWriter::println);

        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem writing file ('" + filespec + "'): " + e);
        }
        renderMessage((System.currentTimeMillis() - start) + "ms to write output to: " + filespec);


    }


    private void renderMessage(String msg) {
        // swap out for log4j, or some such logging mechanism....
        System.out.println(msg);
    }

    private SilverPlanData parseInputStringIntoSilverPlanObject(String inputString) {
          /*
                    plan_id,state,metal_level,rate,rate_area
                    74449NR9870320,GA,Silver,298.62,7
                 */
        // StringTokenizer would be prettier, and slower
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        String planType = inputString.substring(secondComma + 1, thirdComma);
        boolean isSilver = SILVER_PLAN_IDENTIFYING_NAME.equals(planType);

        return isSilver ? new SilverPlanData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                Float.parseFloat(inputString.substring(thirdComma + 1, fourthComma))) : null;
    }

    private ZipRateareaData parseInputStringIntoZipToRateObject(String inputString) {
        /*
            zipcode,state,county_code,name,rate_area
            36749,AL,01001,Autauga,11
         */
        // StringTokenizer would be prettier, and slower
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);

        return new ZipRateareaData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                inputString.substring(0, firstComma));
    }


    /**
     * Class used both for the raw data, as well as the calculated Second-Lowest-Cost of all the plans for the area
     */
    class SilverPlanData implements Comparable {
        float planCost;
        String rateAreaCode;

        SilverPlanData(String rateAreaCode, float planCost) {
            this.rateAreaCode = rateAreaCode;
            this.planCost = planCost;
        }

        String getRateAreaCode() {
            return rateAreaCode;
        }

        @Override
        public int compareTo(Object o) {
            SilverPlanData givenSpd = (SilverPlanData) o;
            return Float.compare(this.planCost, givenSpd.planCost);
        }
    }

    class ZipRateareaData {
        String zip;
        String rateAreaCode;

        ZipRateareaData(String rateAreaCode, String zip) {
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
            ZipRateareaData that = (ZipRateareaData) o;
            return Objects.equals(zip, that.zip) &&
                    Objects.equals(rateAreaCode, that.rateAreaCode);
        }

        @Override
        public int hashCode() {

            return Objects.hash(zip, rateAreaCode);
        }

        @Override
        public String toString() {
            return zip + ':'  + rateAreaCode;
        }
    }


}
