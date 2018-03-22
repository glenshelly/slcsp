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
 * Find the Slcsp for given zip codes
 */
public class SlcspFinder {

    /**
     * Required argument: file location
     *
     * @param args
     */
    public static void main(String args[]) {
        if (args == null || args.length < 1) {
            throw new IllegalStateException("The location of the data must be specified as the first argument");
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

        // Read in the zipcodes from Slcsp.csv
        List<String> slcspInputList = buildInputList(baseDirWithFinalSeparator, inputAndOutputFile);

        // Get the relevant (sliver) planAreas from plans.sv.  Key will be planArea, value is slcsp.
        Map<String, Float> planareaMap = buildPlanAreaMapOfSlcsp(baseDirWithFinalSeparator);

        // Build the final zip:slcsp map, using previous data and zips.csv.  Key is zip code, value is the matching slcsp
        Map<String, Float> zipToSlcspMap = buildZipToSlcspPriceMap(baseDirWithFinalSeparator, slcspInputList, planareaMap);

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
                BufferedReader br = new BufferedReader(new FileReader(filespec));
        ) {
            // skip the first header line
            String line = br.readLine();
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
     * @param baseDir
     * @return a plan area map, with key=the plan area (State + Number) and value=2nd lowest cost for that area.
     * Will only include silver plans.
     */
    private Map<String, Float> buildPlanAreaMapOfSlcsp(String baseDir) {
        long start = System.currentTimeMillis();
        Map<String, Float> planareaMap = new HashMap<>();
        String fileSpec = baseDir + "plans.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec));
        ) {
            // The stream will include the header row, but it will be tossed out since it's not a silver plan
            Map<String, List<SilverPlanData>> groupedMap =
                    stream
                            .map(this::makeSilverPlanObjectFromInput)
                            .filter(Objects::nonNull)
                            .collect(Collectors.groupingBy(SilverPlanData::getAreaCode));
            groupedMap.entrySet().stream()
                    .filter(x -> x.getValue().size() >= 2)
                    .map(x -> {

                        Collections.sort(x.getValue());
                        return x;
                    })
                    .forEach(x -> planareaMap.put(x.getKey(), x.getValue().get(0).planCost));

        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Problem loading planarea file ('" + fileSpec + "'): " + e);
        }

        renderMessage((System.currentTimeMillis() - start) + "ms to get " + planareaMap.size() + " plan areas in map");
        return planareaMap;
    }


    /**
     * @param baseDir
     * @param slcspInputList
     * @param planareaMap
     * @return a map with key=zipcode and value=the slcsp price.  Will only return items for which the slcsp was determined.
     */
    private Map<String, Float> buildZipToSlcspPriceMap(String baseDir, List<String> slcspInputList,
                                                       Map<String, Float> planareaMap) {
        long start = System.currentTimeMillis();
        Set<String> slcspSet = new HashSet<>(slcspInputList);
        Map<String, Float> zipToSlcspMap = new HashMap<>();
        String fileSpec = baseDir + "zips.csv";
        try (
                Stream<String> stream = Files.lines(Paths.get(fileSpec));

        ) {
            // The stream will include the header row, but it will be tossed out, since it won't include a valid zip
            Map<String, List<ZipPlanAreaData>> zipGroupedMap =
                    stream
                            .map(this::makeZipToPlanObjectFromInput)
                            .filter(x -> planareaMap.get(x.areaCode) != null)
                            .filter(x -> slcspSet.contains(x.getZip()))
                            .collect(Collectors.groupingBy(ZipPlanAreaData::getZip));
            for (String zipCode : zipGroupedMap.keySet()) {
                // skip it if there's more than 1
                List<ZipPlanAreaData> matchingObjects = zipGroupedMap.get(zipCode);
                if (matchingObjects.size() == 1) {
                    String areaCodeForZip = matchingObjects.get(0).areaCode;
                    Float secondLowestForArea = planareaMap.get(areaCodeForZip);
                    if (null == secondLowestForArea) {
                        // this should never happen
                        renderMessage("second lowest is null, for areaCode=" + areaCodeForZip);
                    } else {
                        zipToSlcspMap.put(zipCode, secondLowestForArea);
                    }
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
            slcspInputList.stream()
                    .map(x -> x + "," + zipToSlcspMap.get(x))
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

    private SilverPlanData makeSilverPlanObjectFromInput(String inputString) {
          /*
                    plan_id,state,metal_level,rate,rate_area
                    74449NR9870320,GA,Silver,298.62,7
                 */
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);
        String planType = inputString.substring(secondComma + 1, thirdComma);
        boolean isSilver = "Silver".equals(planType);

        return isSilver ? new SilverPlanData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                Float.parseFloat(inputString.substring(thirdComma + 1, fourthComma))) : null;
    }

    private ZipPlanAreaData makeZipToPlanObjectFromInput(String inputString) {
        /*
            zipcode,state,county_code,name,rate_area
            36749,AL,01001,Autauga,11
         */
        char delimiter = ',';
        int firstComma = inputString.indexOf(delimiter);
        int secondComma = inputString.indexOf(delimiter, firstComma + 1);
        int thirdComma = inputString.indexOf(delimiter, secondComma + 1);
        int fourthComma = inputString.indexOf(delimiter, thirdComma + 1);

        ZipPlanAreaData zipPlanAreaData = new ZipPlanAreaData(
                inputString.substring(firstComma + 1, secondComma) + inputString.substring(fourthComma + 1),
                inputString.substring(0, firstComma));
        return zipPlanAreaData;
    }


    /**
     * Class used both for the raw data, as well as the calculated Second-Lowest-Cost of all the plans for the area
     */
    class SilverPlanData implements Comparable {
        float planCost;
        String areaCode;

        SilverPlanData(String areaCode, float planCost) {
            this.areaCode = areaCode;
            this.planCost = planCost;
        }

        String getAreaCode() {
            return areaCode;
        }

        @Override
        public int compareTo(Object o) {
            SilverPlanData givenSpd = (SilverPlanData) o;
            return Float.compare(givenSpd.planCost, this.planCost);
        }
    }

    class ZipPlanAreaData {
        String zip;
        String areaCode;

        ZipPlanAreaData(String areaCode, String zip) {
            this.areaCode = areaCode;
            this.zip = zip;
        }

        String getZip() {
            return zip;
        }
    }


}
