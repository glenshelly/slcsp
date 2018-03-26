package com.glen.slcsp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;


public class SlcspFinderTest {

    private static final String DATA_DIR = "./data";
    private static final String ORIGINAL_FILE = DATA_DIR + "/slcsp-original.csv";
    private static final String BACKUP_FILE = DATA_DIR + "/slcsp-tempbackup.csv";
    private static final String INPUT_OUTPUT_FILE = DATA_DIR + "/slcsp.csv";
    private static final String EXPECTED_RESULT_FILE = DATA_DIR + "/slcsp-expected-result.csv";
    private SlcspFinder finder = new SlcspFinder();


    @Test
    public void verifyAfterAnyChangesToProcess() throws Exception {

        finder.process(DATA_DIR);

        final Path expectedPath = Paths.get(EXPECTED_RESULT_FILE);
        final Path inputOutputPath = Paths.get(INPUT_OUTPUT_FILE);
        byte[] expectedBytes = Files.readAllBytes(expectedPath);
        byte[] actualBytes = Files.readAllBytes(inputOutputPath);

        final boolean filesTheSame = Arrays.equals(expectedBytes, actualBytes);
        assertTrue("The actual results (in " + INPUT_OUTPUT_FILE + ") differ from the expected results (in " + EXPECTED_RESULT_FILE + ")", filesTheSame);
    }


    @Before
    public void setUp() throws Exception {
        System.out.println("Setting it up!");
        Files.copy(Paths.get(INPUT_OUTPUT_FILE), Paths.get(BACKUP_FILE), StandardCopyOption.REPLACE_EXISTING);

        final Path originalPath = Paths.get(ORIGINAL_FILE);
        final Path destinationPath = Paths.get(INPUT_OUTPUT_FILE);

        Files.copy(originalPath, destinationPath, StandardCopyOption.REPLACE_EXISTING);

    }

    @After
    public void tearDown() throws Exception {
        Files.copy(Paths.get(BACKUP_FILE), Paths.get(INPUT_OUTPUT_FILE), StandardCopyOption.REPLACE_EXISTING);
        System.out.println("Running: tearDown");
    }

}
