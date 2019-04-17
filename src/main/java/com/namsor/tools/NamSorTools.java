/*
 * To change this license inputHeaders, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.namsor.tools;

import com.namsor.sdk2.api.PersonalApi;
import com.namsor.sdk2.invoke.ApiClient;
import com.namsor.sdk2.invoke.ApiException;
import com.namsor.sdk2.model.BatchFirstLastNameDiasporaedOut;
import com.namsor.sdk2.model.BatchFirstLastNameGenderedOut;
import com.namsor.sdk2.model.BatchFirstLastNameGeoIn;
import com.namsor.sdk2.model.BatchFirstLastNameIn;
import com.namsor.sdk2.model.BatchFirstLastNameOriginedOut;
import com.namsor.sdk2.model.BatchFirstLastNameUSRaceEthnicityOut;
import com.namsor.sdk2.model.FirstLastNameDiasporaedOut;
import com.namsor.sdk2.model.FirstLastNameGenderedOut;
import com.namsor.sdk2.model.FirstLastNameGeoIn;
import com.namsor.sdk2.model.FirstLastNameIn;
import com.namsor.sdk2.model.FirstLastNameOriginedOut;
import com.namsor.sdk2.model.FirstLastNameUSRaceEthnicityOut;
import com.namsor.sdk2.model.PersonalNameGeoIn;
import com.namsor.sdk2.model.PersonalNameIn;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Command line tools for NamSorAPIv2
 *
 * @author elian
 */
public class NamSorTools {

    private final Set<String> done = Collections.synchronizedSet(new HashSet());

    private final String separatorOut = "|";
    private final String separatorIn = "|";

    private static final int BATCH_SIZE = 100;

    private static final String INPUT_DATA_FORMAT_FNLN = "fnln";
    private static final String INPUT_DATA_FORMAT_FNLNGEO = "fnlngeo";
    private static final String INPUT_DATA_FORMAT_FULLNAME = "name";
    private static final String INPUT_DATA_FORMAT_FULLNAMEGEO = "namegeo";

    private static final String[] INPUT_DATA_FORMAT = {
        INPUT_DATA_FORMAT_FNLN,
        INPUT_DATA_FORMAT_FNLNGEO,
        INPUT_DATA_FORMAT_FULLNAME,
        INPUT_DATA_FORMAT_FULLNAMEGEO
    };

    private static final String[][] INPUT_DATA_FORMAT_HEADER = {
        {"firstName", "lastName"},
        {"firstName", "lastName", "countryIso2"},
        {"fullName"},
        {"fullName", "countryIso2"}
    };

    private static final String SERVICE_NAME_PARSE = "parse";
    private static final String SERVICE_NAME_GENDER = "gender";
    private static final String SERVICE_NAME_ORIGIN = "origin";
    private static final String SERVICE_NAME_DIASPORA = "diaspora";
    private static final String SERVICE_NAME_USRACEETHNICITY = "usraceethnicity";

    private static final String[] SERVICES = {
        SERVICE_NAME_PARSE,
        SERVICE_NAME_GENDER,
        SERVICE_NAME_ORIGIN,
        SERVICE_NAME_DIASPORA,
        SERVICE_NAME_USRACEETHNICITY
    };

    private static final String[] OUTPUT_DATA_PARSE_HEADER = {"firstNameParsed", "lastNameParsed", "nameParserType", "nameParserTypeAlt", "nameParserTypeScore"};
    private static final String[] OUTPUT_DATA_GENDER_HEADER = {"likelyGender", "likelyGenderScore", "genderScale"};
    private static final String[] OUTPUT_DATA_ORIGIN_HEADER = {"countryOrigin", "countryOriginAlt", "countryOriginScore"};
    private static final String[] OUTPUT_DATA_DIASPORA_HEADER = {"ethnicity", "ethnicityAlt", "ethnicityScore"};
    private static final String[] OUTPUT_DATA_USRACEETHNICITY_HEADER = {"raceEthnicity", "raceEthnicityAlt", "raceEthnicityScore"};
    private static final String[][] OUTPUT_DATA_HEADERS = {
        OUTPUT_DATA_PARSE_HEADER,
        OUTPUT_DATA_GENDER_HEADER,
        OUTPUT_DATA_ORIGIN_HEADER,
        OUTPUT_DATA_DIASPORA_HEADER,
        OUTPUT_DATA_USRACEETHNICITY_HEADER
    };

    private final CommandLine commandLineOptions;
    private final PersonalApi api;
    private final int TIMEOUT = 30000;
    private final boolean withUID;
    private final boolean recover;

    public NamSorTools(CommandLine commandLineOptions) {
        this.commandLineOptions = commandLineOptions;
        String apiKey = commandLineOptions.getOptionValue("apiKey");
        if (apiKey == null || apiKey.isEmpty()) {
            throw new IllegalStateException("Missing API KEY");
        }
        ApiClient client = new ApiClient();
        client.setConnectTimeout(TIMEOUT);
        client.setReadTimeout(TIMEOUT);
        client.setWriteTimeout(TIMEOUT);
        client.setApiKey(apiKey);
        //client.setDebugging(false);
        //client.setBasePath("http://localhost:8080/NamSorAPIv2");
        api = new PersonalApi(client);

        withUID = commandLineOptions.hasOption("uid");
        recover = commandLineOptions.hasOption("recover");

    }

    private static List<String> readFile(String fileName) throws IOException {
        List<String> donePlace = new ArrayList();
        File exists = new File(fileName);
        if (!exists.exists()) {
            return donePlace;
        }
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String doneLine = br.readLine();
        int line = 0;
        while (doneLine != null) {
            donePlace.add(doneLine.trim());
            doneLine = br.readLine();
            if (line % 100000 == 0) {
                Logger.getLogger(IsDone.class.getName()).info("Loading " + fileName + ":" + line);
            }
            line++;
        }
        br.close();
        return donePlace;
    }

    public static void main(String[] args) {
        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {

            Option apiKey = Option.builder("apiKey").argName("apiKey")
                    .hasArg()
                    .desc("NamSor API Key")
                    .longOpt("apiKey")
                    .required()
                    .build();

            Option inputFile = Option.builder("i").argName("inputFile")
                    .hasArg()
                    .desc("input file name")
                    .longOpt("inputFile")
                    .required()
                    .build();

            Option countryIso2 = Option.builder("countryIso2").argName("countryIso2")
                    .hasArg()
                    .desc("countryIso2 default")
                    .longOpt("countryIso2")
                    .required(false)
                    .build();

            Option outputFile = Option.builder("o").argName("outputFile")
                    .hasArg()
                    .desc("output file name")
                    .longOpt("outputFile")
                    .required(false)
                    .build();

            Option outputFileOverwrite = Option.builder("w").argName("overwrite")
                    .hasArg(false)
                    .desc("overwrite existing output file")
                    .longOpt("overwrite")
                    .required(false)
                    .build();

            Option outputFileRecover = Option.builder("r").argName("recover")
                    .hasArg(false)
                    .desc("continue from a job (requires uid)")
                    .longOpt("recover")
                    .required(false)
                    .build();

            Option inputDataFormat = Option.builder("f").argName("inputDataFormat")
                    .hasArg()
                    .desc("input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
                    .longOpt("inputDataFormat")
                    .required(true)
                    .build();
            Option outputHeader = Option.builder("header").argName("header")
                    .hasArg(false)
                    .desc("output header")
                    .longOpt("header")
                    .required(false)
                    .build();

            Option inputDataHasId = Option.builder("uid").argName("inputDataHasId")
                    .hasArg(false)
                    .desc("input data has an ID prefix")
                    .longOpt("uid")
                    .required(false)
                    .build();

            Option help = Option.builder("h").argName("help")
                    .hasArg(false)
                    .desc("get help")
                    .longOpt("help")
                    .required(false)
                    .build();

            Option service = Option.builder("service").argName("service")
                    .hasArg(true)
                    .desc("service : parse / gender / origin / diaspora / usraceethnicity")
                    .longOpt("endpoint")
                    .required(true)
                    .build();

            Option encoding = Option.builder("e").argName("encoding")
                    .hasArg()
                    .desc("encoding : UTF-8 by default")
                    .longOpt("encoding")
                    .required(false)
                    .build();

            Options options = new Options();
            options.addOption(apiKey);
            options.addOption(inputFile);
            options.addOption(inputDataHasId);
            options.addOption(inputDataFormat);
            options.addOption(outputFile);
            options.addOption(outputHeader);
            options.addOption(service);
            options.addOption(encoding);
            options.addOption(outputFileOverwrite);
            options.addOption(countryIso2);
            options.addOption(outputFileRecover);
            options.addOption(help);

            Options helpOptions = new Options();
            helpOptions.addOption(help);

            CommandLine lineHelp = parser.parse(helpOptions, args, true);
            if (lineHelp.hasOption("help") || ((lineHelp.getOptions() == null || lineHelp.getOptions().length == 0)
                    && (lineHelp.getArgs() == null || lineHelp.getArgs().length == 0))) {
                final HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(NamSorTools.class.getSimpleName(), options, true);
                System.exit(0);
            }

            // parse the command line arguments
            CommandLine commandLineOptions = parser.parse(options, args);
            NamSorTools tools = new NamSorTools(commandLineOptions);
            tools.run();
        } catch (ParseException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NamSorToolException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @return the commandLineOptions
     */
    public CommandLine getCommandLineOptions() {
        return commandLineOptions;
    }

    private void run() throws NamSorToolException {
        String apiKey = getCommandLineOptions().getOptionValue("apiKey");
        if (apiKey == null || apiKey.isEmpty()) {
            throw new NamSorToolException("Missing API KEY");
        }
        BufferedReader reader = null;
        Writer writer = null;
        try {
            String service = getCommandLineOptions().getOptionValue("service");
            String inputFileName = getCommandLineOptions().getOptionValue("inputFile");
            if (inputFileName == null || inputFileName.isEmpty()) {
                throw new NamSorToolException("Missing inputFile");
            }
            File inputFile = new File(inputFileName);
            if (!inputFile.exists() || !inputFile.canRead()) {
                throw new NamSorToolException("Can't read inputFile " + inputFileName);
            }
            String outputFileName = getCommandLineOptions().getOptionValue("outputFile");
            if (outputFileName == null || outputFileName.isEmpty()) {
                outputFileName = inputFileName + "." + service + ".namsor";
                Logger.getLogger(getClass().getName()).info("Outputing to " + outputFileName);
            }
            File outputFile = new File(outputFileName);
            boolean outputFileOverwrite = getCommandLineOptions().hasOption("overwrite");
            if (outputFile.exists() && !outputFileOverwrite && !isRecover()) {
                throw new NamSorToolException("OutputFile " + inputFileName + " already exists, use -r to recover and continue job");
            }
            if (outputFileOverwrite && isRecover()) {
                throw new NamSorToolException("You can overwrite OR recover to " + inputFileName);
            }
            if (isRecover() && !isWithUID()) {
                throw new NamSorToolException("You can't recover without a uid " + inputFileName);
            }
            String encoding = getCommandLineOptions().getOptionValue("encoding");
            if (encoding == null || encoding.isEmpty()) {
                encoding = "UTF-8";
            }

            if (isRecover() && outputFile.exists()) {
                Logger.getLogger(NamSorTools.class.getName()).info("Recovering from existing " + outputFileName);
                BufferedReader readerDone = new BufferedReader(new InputStreamReader(new FileInputStream(outputFile), encoding));
                String doneLine = readerDone.readLine();
                int line = 0;
                int len = -1;
                while (doneLine != null) {
                    if (!doneLine.startsWith("#") && !doneLine.isEmpty()) {
                        String[] existingData = doneLine.split("\\|");
                        if( len < 0 ) {
                            len = existingData.length;
                        } else if( len != existingData.length ) {
                            Logger.getLogger(IsDone.class.getName()).warning("Line "+line+" doneLine="+doneLine+" len="+existingData.length+"!="+len);
                        }
                        done.add(existingData[0]);
                    }
                    doneLine = readerDone.readLine();
                    if (line % 100000 == 0) {
                        Logger.getLogger(IsDone.class.getName()).info("Loading from existing " + outputFileName + ":" + line);
                    }
                    line++;
                }
                readerDone.close();
            }
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), encoding));
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, isRecover()), encoding));

            process(service, reader, writer);
        } catch (IOException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ApiException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, ex.getResponseBody(), ex);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void process(String service, BufferedReader reader, Writer writer) throws IOException, NamSorToolException, ApiException {
        int lineId = 0;
        String inputDataFormat = getCommandLineOptions().getOptionValue("inputDataFormat");
        String[] inputHeaders = null;
        for (int i = 0; i < INPUT_DATA_FORMAT.length; i++) {
            if (INPUT_DATA_FORMAT[i].equals(inputDataFormat)) {
                inputHeaders = INPUT_DATA_FORMAT_HEADER[i];
                break;
            }
        }
        if (inputHeaders == null) {
            throw new NamSorToolException("Invalid inputFileFormat " + inputDataFormat);
        }
        String[] outputHeaders = null;
        for (int i = 0; i < SERVICES.length; i++) {
            if (SERVICES[i].equals(service)) {
                outputHeaders = OUTPUT_DATA_HEADERS[i];
                break;
            }
        }
        if (outputHeaders == null) {
            throw new NamSorToolException("Invalid service " + service);
        }
        boolean appendHeader = getCommandLineOptions().hasOption("header");
        if ( appendHeader && !isRecover() || (isRecover() && done.isEmpty()) ) {
            // don't append a header to an existing file
            appendHeader(writer, inputHeaders, outputHeaders);
        }
        int dataLenExpected = (isWithUID() ? inputHeaders.length + 1 : inputHeaders.length);
        StringWriter dataFormatExpected = new StringWriter();
        if (isWithUID()) {
            dataFormatExpected.append("uid" + separatorIn);
        }
        String countryIso2Default = getCommandLineOptions().getOptionValue("countryIso2");
        for (int i = 0; i < inputHeaders.length; i++) {
            dataFormatExpected.append(inputHeaders[i]);
            if (i < inputHeaders.length - 1) {
                dataFormatExpected.append(separatorIn);
            }
        }
        String line = reader.readLine();
        while (line != null) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                if (line.endsWith("|")) {
                    line = line + " ";
                }
                String[] lineData = line.split("\\|");
                if (lineData.length != dataLenExpected) {
                    throw new NamSorToolException("Line " + lineId + ", expected input with format : " + dataFormatExpected.toString());
                }
                String uid = null;
                int col = 0;
                if (isWithUID()) {
                    uid = lineData[col++];
                } else {
                    uid = "uid" + (uidGen++);
                }
                if (isRecover() && done.contains(uid)) {
                    // skip this, as it's already done
                } else {
                    if (inputDataFormat.equals(INPUT_DATA_FORMAT_FNLN)) {
                        String firstName = lineData[col++];
                        String lastName = lineData[col++];
                        FirstLastNameIn firstLastNameIn = new FirstLastNameIn();
                        firstLastNameIn.setId(uid);
                        firstLastNameIn.setFirstName(firstName);
                        firstLastNameIn.setLastName(lastName);
                        firstLastNamesIn.put(uid, firstLastNameIn);
                    } else if (inputDataFormat.equals(INPUT_DATA_FORMAT_FNLNGEO)) {
                        String firstName = lineData[col++];
                        String lastName = lineData[col++];
                        String countryIso2 = lineData[col++];
                        if ((countryIso2 == null || countryIso2.trim().isEmpty()) && countryIso2Default != null) {
                            countryIso2 = countryIso2Default;
                        }
                        FirstLastNameGeoIn firstLastNameGeoIn = new FirstLastNameGeoIn();
                        firstLastNameGeoIn.setId(uid);
                        firstLastNameGeoIn.setFirstName(firstName);
                        firstLastNameGeoIn.setLastName(lastName);
                        firstLastNameGeoIn.setCountryIso2(countryIso2);
                        firstLastNamesGeoIn.put(uid, firstLastNameGeoIn);
                    } else if (inputDataFormat.equals(INPUT_DATA_FORMAT_FULLNAME)) {
                        String fullName = lineData[col++];
                        PersonalNameIn personalNameIn = new PersonalNameIn();
                        personalNameIn.setId(uid);
                        personalNameIn.setName(fullName);
                        personalNamesIn.put(uid, personalNameIn);
                    } else if (inputDataFormat.equals(INPUT_DATA_FORMAT_FULLNAMEGEO)) {
                        String fullName = lineData[col++];
                        String countryIso2 = lineData[col++];
                        if ((countryIso2 == null || countryIso2.trim().isEmpty()) && countryIso2Default != null) {
                            countryIso2 = countryIso2Default;
                        }
                        PersonalNameGeoIn personalNameGeoIn = new PersonalNameGeoIn();
                        personalNameGeoIn.setId(uid);
                        personalNameGeoIn.setName(fullName);
                        personalNameGeoIn.setCountryIso2(countryIso2);
                        personalNamesGeoIn.put(uid, personalNameGeoIn);
                    }
                    processData(service, outputHeaders, writer, false);
                }
            }
            lineId++;
            line = reader.readLine();
        }
        processData(service, outputHeaders, writer, true);
        writer.flush();
    }

    private static long uidGen;
    private static long rowId;

    private void appendHeader(Writer writer, String[] inputHeaders, String[] outputHeaders) throws IOException {
        writer.append("#uid" + separatorOut);
        for (String inputHeader : inputHeaders) {
            writer.append(inputHeader + separatorOut);
        }
        for (String outputHeader : outputHeaders) {
            writer.append(outputHeader + separatorOut);
        }
        writer.append("rowId" + "\n");
        writer.flush();
    }

    private Map<String, FirstLastNameDiasporaedOut> processDiaspora(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameDiasporaedOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameDiasporaedOut origined = api.diasporaBatch(body);
        for (FirstLastNameDiasporaedOut personalName : origined.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameOriginedOut> processOriginGeo(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        List<FirstLastNameIn> namesNoGeo = new ArrayList();
        for (FirstLastNameGeoIn name : names) {
            FirstLastNameIn nameNoGeo = new FirstLastNameIn();
            nameNoGeo.setId(name.getId());
            nameNoGeo.setFirstName(name.getFirstName());
            nameNoGeo.setLastName(name.getLastName());
            namesNoGeo.add(nameNoGeo);
        }
        return processOrigin(namesNoGeo);
    }

    private Map<String, FirstLastNameOriginedOut> processOrigin(List<FirstLastNameIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameOriginedOut> result = new HashMap();
        BatchFirstLastNameIn body = new BatchFirstLastNameIn();
        body.setPersonalNames(names);
        BatchFirstLastNameOriginedOut origined = api.originBatch(body);
        for (FirstLastNameOriginedOut personalName : origined.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameGenderedOut> processGender(List<FirstLastNameIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameGenderedOut> result = new HashMap();
        BatchFirstLastNameIn body = new BatchFirstLastNameIn();
        body.setPersonalNames(names);
        BatchFirstLastNameGenderedOut gendered = api.genderBatch(body);
        for (FirstLastNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameGenderedOut> processGenderGeo(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameGenderedOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameGenderedOut gendered = api.genderGeoBatch(body);
        for (FirstLastNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameUSRaceEthnicityOut> processUSRaceEthnicity(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameUSRaceEthnicityOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameUSRaceEthnicityOut racedEthnicized = api.usRaceEthnicityBatch(body);
        for (FirstLastNameUSRaceEthnicityOut personalName : racedEthnicized.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private void processData(String service, String[] outputHeaders, Writer writer, boolean flushBuffers) throws ApiException, IOException {
        if (flushBuffers && !firstLastNamesIn.isEmpty() || firstLastNamesIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_ORIGIN)) {
                Map<String, FirstLastNameOriginedOut> origins = processOrigin(new ArrayList(firstLastNamesIn.values()));
                append(writer, outputHeaders, firstLastNamesIn, origins);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGender(new ArrayList(firstLastNamesIn.values()));
                append(writer, outputHeaders, firstLastNamesIn, genders);
            }
            firstLastNamesIn.clear();
        }
        if (flushBuffers && !firstLastNamesGeoIn.isEmpty() || firstLastNamesGeoIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_ORIGIN)) {
                Map<String, FirstLastNameOriginedOut> origins = processOriginGeo(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, origins);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGenderGeo(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, genders);
            } else if (service.equals(SERVICE_NAME_DIASPORA)) {
                Map<String, FirstLastNameDiasporaedOut> diasporas = processDiaspora(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, diasporas);
            } else if (service.equals(SERVICE_NAME_USRACEETHNICITY)) {
                Map<String, FirstLastNameUSRaceEthnicityOut> usRaceEthnicities = processUSRaceEthnicity(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, usRaceEthnicities);
            }
            firstLastNamesGeoIn.clear();
        }
    }

    private void append(Writer writer, String[] outputHeaders, Map input, Map output) throws IOException {
        Set<String> flushedUID = new HashSet();
        for (Object idObj : input.keySet()) {
            String uid = (String) idObj;
            flushedUID.add(uid);
            Object inputObj = input.get(uid);
            Object outputObj = output.get(uid);
            writer.append(uid + separatorOut);
            if (inputObj instanceof FirstLastNameIn) {
                FirstLastNameIn firstLastNameIn = (FirstLastNameIn) inputObj;
                writer.append(firstLastNameIn.getFirstName() + separatorOut + firstLastNameIn.getLastName() + separatorOut);
            } else if (inputObj instanceof FirstLastNameGeoIn) {
                FirstLastNameGeoIn firstLastNameGeoIn = (FirstLastNameGeoIn) inputObj;
                writer.append(firstLastNameGeoIn.getFirstName() + separatorOut + firstLastNameGeoIn.getLastName() + separatorOut + firstLastNameGeoIn.getCountryIso2() + separatorOut);
            } else if (inputObj instanceof PersonalNameIn) {
                PersonalNameIn personalNameIn = (PersonalNameIn) inputObj;
                writer.append(personalNameIn.getName() + separatorOut);
            } else if (inputObj instanceof PersonalNameGeoIn) {
                PersonalNameGeoIn personalNameGeoIn = (PersonalNameGeoIn) inputObj;
                writer.append(personalNameGeoIn.getName() + separatorOut + personalNameGeoIn.getCountryIso2() + separatorOut);
            } else {
                throw new IllegalArgumentException("Serialization of " + inputObj.getClass().getName() + " not supported");
            }
            if (outputObj == null) {
                for (String outputHeader : outputHeaders) {
                    writer.append("" + separatorOut);
                }
            } else if (outputObj instanceof FirstLastNameGenderedOut) {
                FirstLastNameGenderedOut firstLastNameGenderedOut = (FirstLastNameGenderedOut) outputObj;
                writer.append(firstLastNameGenderedOut.getLikelyGender().getValue() + separatorOut + firstLastNameGenderedOut.getScore() + separatorOut + firstLastNameGenderedOut.getGenderScale() + separatorOut);
            } else if (outputObj instanceof FirstLastNameOriginedOut) {
                FirstLastNameOriginedOut firstLastNameOriginedOut = (FirstLastNameOriginedOut) outputObj;
                writer.append(firstLastNameOriginedOut.getCountryOrigin() + separatorOut + firstLastNameOriginedOut.getCountryOriginAlt() + separatorOut + firstLastNameOriginedOut.getScore() + separatorOut);
            } else if (outputObj instanceof FirstLastNameDiasporaedOut) {
                FirstLastNameDiasporaedOut firstLastNameDiasporaedOut = (FirstLastNameDiasporaedOut) outputObj;
                writer.append(firstLastNameDiasporaedOut.getEthnicity() + separatorOut + firstLastNameDiasporaedOut.getEthnicityAlt() + separatorOut + firstLastNameDiasporaedOut.getScore() + separatorOut);
            } else if (outputObj instanceof FirstLastNameUSRaceEthnicityOut) {
                FirstLastNameUSRaceEthnicityOut firstLastNameUSRaceEthnicityOut = (FirstLastNameUSRaceEthnicityOut) outputObj;
                writer.append(firstLastNameUSRaceEthnicityOut.getRaceEthnicity() + separatorOut + firstLastNameUSRaceEthnicityOut.getRaceEthnicityAlt() + separatorOut + firstLastNameUSRaceEthnicityOut.getScore() + separatorOut);
            } else {
                throw new IllegalArgumentException("Serialization of " + outputObj.getClass().getName() + " not supported");
            }
            writer.append((rowId++) + "\n");
        }
        writer.flush();
        if (isRecover()) {
            done.addAll(flushedUID);
        }
        if (rowId % 100 == 0 && rowId < 1000
                || rowId % 1000 == 0 && rowId < 10000
                || rowId % 10000 == 0 && rowId < 100000
                || rowId % 100000 == 0) {
            Logger.getLogger(getClass().getName()).info("Processed " + rowId + " rows.");
        }
    }

    private final Map<String, FirstLastNameGeoIn> firstLastNamesGeoIn = new HashMap();
    private final Map<String, FirstLastNameIn> firstLastNamesIn = new HashMap();
    private final Map<String, PersonalNameIn> personalNamesIn = new HashMap();
    private final Map<String, PersonalNameGeoIn> personalNamesGeoIn = new HashMap();

    /**
     * @return the withUID
     */
    public boolean isWithUID() {
        return withUID;
    }

    /**
     * @return the recover
     */
    public boolean isRecover() {
        return recover;
    }

}
