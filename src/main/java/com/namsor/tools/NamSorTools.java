/*
 * To change this license inputHeaders, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.namsor.tools;

import com.namsor.sdk2.api.AdminApi;
import com.namsor.sdk2.api.GeneralApi;
import com.namsor.sdk2.api.PersonalApi;
import com.namsor.sdk2.api.IndianApi;
import com.namsor.sdk2.api.SocialApi;
import com.namsor.sdk2.invoke.ApiClient;
import com.namsor.sdk2.invoke.ApiException;
import com.namsor.sdk2.model.BatchFirstLastNameDiasporaedOut;
import com.namsor.sdk2.model.BatchFirstLastNameGenderedOut;
import com.namsor.sdk2.model.BatchFirstLastNameGeoIn;
import com.namsor.sdk2.model.BatchFirstLastNameGeoSubclassificationOut;
import com.namsor.sdk2.model.BatchFirstLastNameIn;
import com.namsor.sdk2.model.BatchFirstLastNameOriginedOut;
import com.namsor.sdk2.model.BatchFirstLastNamePhoneCodedOut;
import com.namsor.sdk2.model.BatchFirstLastNamePhoneNumberIn;
import com.namsor.sdk2.model.BatchFirstLastNameUSRaceEthnicityOut;
import com.namsor.sdk2.model.BatchNameGeoIn;
import com.namsor.sdk2.model.BatchNameIn;
import com.namsor.sdk2.model.BatchPersonalNameCastegroupOut;
import com.namsor.sdk2.model.BatchPersonalNameGenderedOut;
import com.namsor.sdk2.model.BatchPersonalNameGeoIn;
import com.namsor.sdk2.model.BatchPersonalNameGeoOut;
import com.namsor.sdk2.model.BatchPersonalNameGeoSubclassificationOut;
import com.namsor.sdk2.model.BatchPersonalNameGeoSubdivisionIn;
import com.namsor.sdk2.model.BatchPersonalNameIn;
import com.namsor.sdk2.model.BatchPersonalNameParsedOut;
import com.namsor.sdk2.model.BatchPersonalNameReligionedOut;
import com.namsor.sdk2.model.BatchPersonalNameSubdivisionIn;
import com.namsor.sdk2.model.BatchProperNounCategorizedOut;
import com.namsor.sdk2.model.FirstLastNameDiasporaedOut;
import com.namsor.sdk2.model.FirstLastNameGenderedOut;
import com.namsor.sdk2.model.FirstLastNameGeoIn;
import com.namsor.sdk2.model.FirstLastNameGeoSubclassificationOut;
import com.namsor.sdk2.model.FirstLastNameIn;
import com.namsor.sdk2.model.FirstLastNameOriginedOut;
import com.namsor.sdk2.model.FirstLastNamePhoneCodedOut;
import com.namsor.sdk2.model.FirstLastNamePhoneNumberIn;
import com.namsor.sdk2.model.FirstLastNameUSRaceEthnicityOut;
import com.namsor.sdk2.model.NameGeoIn;
import com.namsor.sdk2.model.NameIn;
import com.namsor.sdk2.model.PersonalNameCastegroupOut;
import com.namsor.sdk2.model.PersonalNameGenderedOut;
import com.namsor.sdk2.model.PersonalNameGeoIn;
import com.namsor.sdk2.model.PersonalNameGeoOut;
import com.namsor.sdk2.model.PersonalNameGeoSubclassificationOut;
import com.namsor.sdk2.model.PersonalNameGeoSubdivisionIn;
import com.namsor.sdk2.model.PersonalNameIn;
import com.namsor.sdk2.model.PersonalNameParsedOut;
import com.namsor.sdk2.model.PersonalNameReligionedOut;
import com.namsor.sdk2.model.PersonalNameSubdivisionIn;
import com.namsor.sdk2.model.ProperNounCategorizedOut;
import com.namsor.sdk2.model.ReligionStatOut;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    /**
     * From v2.0.27, output file is named .namsor_java.txt
     */
    private static final String NAMSOR_EXT = ".namsor_java.txt";

    private static final String DEFAULT_DIGEST_ALGO = "MD5";

    private final Set<String> done = Collections.synchronizedSet(new HashSet());

    private final String separatorOut = "|";
    private final String separatorIn = "|";

    private static final int BATCH_SIZE = 100;

    private static final String INPUT_DATA_FORMAT_FNLN = "fnln";
    private static final String INPUT_DATA_FORMAT_FNLNGEO = "fnlngeo";
    private static final String INPUT_DATA_FORMAT_FULLNAME = "name";
    private static final String INPUT_DATA_FORMAT_FULLNAMEGEO = "namegeo";
    private static final String INPUT_DATA_FORMAT_FULLNAMEGEOSUB = "namegeosub";
    private static final String INPUT_DATA_FORMAT_FNLNPHONE = "fnlnphone";

    private static final String[] INPUT_DATA_FORMAT = {
        INPUT_DATA_FORMAT_FNLN,
        INPUT_DATA_FORMAT_FNLNGEO,
        INPUT_DATA_FORMAT_FULLNAME,
        INPUT_DATA_FORMAT_FULLNAMEGEO,
        INPUT_DATA_FORMAT_FULLNAMEGEOSUB,
        INPUT_DATA_FORMAT_FNLNPHONE
    };

    private static final String[][] INPUT_DATA_FORMAT_HEADER = {
        {"firstName", "lastName"},
        {"firstName", "lastName", "countryIso2"},
        {"fullName"},
        {"fullName", "countryIso2"},
        {"fullName", "countryIso2","subDivisionIso31662"},
        {"firstName", "lastName", "phone"},};

    private static final String SERVICE_NAME_PARSE = "parse";
    private static final String SERVICE_NAME_GENDER = "gender";
    private static final String SERVICE_NAME_ORIGIN = "origin";
    private static final String SERVICE_NAME_COUNTRY = "country";
    private static final String SERVICE_NAME_RELIGION = "religion";
    private static final String SERVICE_NAME_CASTEGROUP = "castegroup";
    private static final String SERVICE_NAME_TYPE = "nametype";
    private static final String SERVICE_NAME_DIASPORA = "diaspora";
    private static final String SERVICE_NAME_PHONECODE = "phonecode";
    private static final String SERVICE_NAME_USRACEETHNICITY = "usraceethnicity";
    private static final String SERVICE_NAME_SUBDIVISION = "subdivision";

    private static final String[] SERVICES = {
        SERVICE_NAME_PARSE,
        SERVICE_NAME_GENDER,
        SERVICE_NAME_ORIGIN,
        SERVICE_NAME_COUNTRY,
        SERVICE_NAME_RELIGION,
        SERVICE_NAME_CASTEGROUP,
        SERVICE_NAME_TYPE,
        SERVICE_NAME_DIASPORA,
        SERVICE_NAME_USRACEETHNICITY,
        SERVICE_NAME_PHONECODE,
        SERVICE_NAME_SUBDIVISION
    };

  
    private static final String[] OUTPUT_DATA_PARSE_HEADER = {"firstNameParsed", "lastNameParsed", "nameParserType", "nameParserTypeAlt", "nameParserTypeScore", "script"};
    private static final String[] OUTPUT_DATA_GENDER_HEADER = {"likelyGender", "probabilityCalibrated", "likelyGenderScore", "genderScale", "script"};
    private static final String[] OUTPUT_DATA_ORIGIN_HEADER = {"region","topRegion","subRegion","countryOrigin", "countryOriginAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "countryOriginScore", "countryOriginTop", "script"};
    private static final String[] OUTPUT_DATA_COUNTRY_HEADER = {"region","topRegion","subRegion","country", "countryAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "countryScore", "countryTop", "script"};
    private static final String[] OUTPUT_DATA_RELIGION_HEADER = {"religion", "religionAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "religionScore", "religionsTop", "script"};
    private static final String[] OUTPUT_DATA_CASTEGROUP_HEADER = {"castegroup", "castegroupAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "castegroupScore", "castegroupsTop", "script"};
    private static final String[] OUTPUT_DATA_TYPE_HEADER = {"commonType", "commonTypeAlt", "commonTypeScore", "commonTypeTop", "script"};
    private static final String[] OUTPUT_DATA_DIASPORA_HEADER = {"ethnicity", "ethnicityAlt",  "probabilityCalibrated", "probabilityCalibratedAlt", "ethnicityScore", "ethnicityTop", "script"};
    private static final String[] OUTPUT_DATA_USRACEETHNICITY_HEADER = {"raceEthnicity", "raceEthnicityAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "raceEthnicityScore", "raceEthnicityTop", "script"};
    private static final String[] OUTPUT_DATA_PHONECODE_HEADER = {"internationalPhoneNumberVerified", "phoneCountryIso2Verified", "phoneCountryCode", "phoneCountryCodeAlt", "phoneCountryIso2", "phoneCountryIso2Alt", "originCountryIso2", "originCountryIso2Alt", "verified", "score", "script"};
    private static final String[] OUTPUT_DATA_SUBDIVISION_HEADER = {"subClassification", "subClassificationAlt", "probabilityCalibrated", "probabilityCalibratedAlt", "subclassificationScore", "subclassificationTop", "script"};
    private static final String[] OUTPUT_DATA_RELIGIONSTAT_HEADER = {"religion", "religionPct", "religionAlt", "religionAltPct"};

    private static final String[][] OUTPUT_DATA_HEADERS = {
        OUTPUT_DATA_PARSE_HEADER,
        OUTPUT_DATA_GENDER_HEADER,
        OUTPUT_DATA_ORIGIN_HEADER,
        OUTPUT_DATA_COUNTRY_HEADER,
        OUTPUT_DATA_RELIGION_HEADER,
        OUTPUT_DATA_CASTEGROUP_HEADER,
        OUTPUT_DATA_TYPE_HEADER,
        OUTPUT_DATA_DIASPORA_HEADER,
        OUTPUT_DATA_USRACEETHNICITY_HEADER,
        OUTPUT_DATA_PHONECODE_HEADER,
        OUTPUT_DATA_SUBDIVISION_HEADER
    };

    private final CommandLine commandLineOptions;

    private final PersonalApi personalApi;
    private final IndianApi indianApi;
    private final GeneralApi generalApi;
    private final AdminApi adminApi;
    private final SocialApi socialApi;    
    private final int TIMEOUT = 30000;
    
    private final boolean withUID;
    private final boolean recover;
    private final boolean skipErrors;
    private final MessageDigest digest;
    private final String usraceethnicityoption;
    private final boolean religionoption;

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
        usraceethnicityoption = commandLineOptions.getOptionValue("usraceethnicityoption");
        if (usraceethnicityoption != null && !usraceethnicityoption.isEmpty()) {
            Logger.getLogger(NamSorTools.class.getName()).info("Overriding usraceethnicityoption=" + usraceethnicityoption);
            client.addDefaultHeader(NAMSOR_OPTION_USRACEETHNICITY_TAXO, usraceethnicityoption);
        }
        religionoption = commandLineOptions.hasOption("religionoption");
        if (religionoption) {
            Logger.getLogger(NamSorTools.class.getName()).info("Overriding religionoption");
            client.addDefaultHeader(NAMSOR_OPTION_RELIGION_STATS, Boolean.TRUE.toString());
        }        
        String basePath = commandLineOptions.getOptionValue("basePath");
        if (basePath != null && !basePath.isEmpty()) {
            Logger.getLogger(NamSorTools.class.getName()).info("Overriding basePath=" + basePath);
            client.setBasePath(basePath);
        }
        //client.setDebugging(false);
        personalApi = new PersonalApi(client);
        generalApi = new GeneralApi(client);
        adminApi = new AdminApi(client);
        socialApi = new SocialApi(client);
        indianApi = new IndianApi(client);

        withUID = commandLineOptions.hasOption("uid");
        recover = commandLineOptions.hasOption("recover");
        skipErrors = commandLineOptions.hasOption("skip");

        MessageDigest digest_ = null;
        if (commandLineOptions.hasOption("digest")) {
            try {
                digest_ = MessageDigest.getInstance(DEFAULT_DIGEST_ALGO);
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, "Digest algo not found " + DEFAULT_DIGEST_ALGO, ex);
            }
        }
        digest = digest_;
    }

    private static String bytesToHex(byte[] hash) {
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public String digest(String inClear) {
        if (getDigest() == null || inClear == null || inClear.isEmpty()) {
            return inClear;
        } else {
            final byte[] hashbytes = getDigest().digest(
                    inClear.getBytes(StandardCharsets.UTF_8));
            String digested = bytesToHex(hashbytes);
            return digested;
        }
    }

    /** Deprecaded, now use server side value
     * public static String computeScriptFirst_(String someString) {
        for (int i = 0; i < someString.length(); i++) {
            Character c = someString.charAt(i);
            String script = Character.UnicodeScript.of(c).name();
            if (script.equals(Character.UnicodeScript.COMMON.name())) {
                continue;
            }
            return script;
        }
        return null;
    }*/
    public static final String NAMSOR_OPTION_USRACEETHNICITY_TAXO = "X-OPTION-USRACEETHNICITY-TAXONOMY";    
    public static final String NAMSOR_OPTION_USRACEETHNICITY_TAXO_4CLASSES = "USRACEETHNICITY-4CLASSES";
    public static final String NAMSOR_OPTION_USRACEETHNICITY_TAXO_4CLASSESCLASSIC = "USRACEETHNICITY-4CLASSES-CLASSIC";
    public static final String NAMSOR_OPTION_USRACEETHNICITY_TAXO_6CLASSES = "USRACEETHNICITY-6CLASSES";
    public static final String NAMSOR_OPTION_RELIGION_STATS = "X-OPTION-RELIGION-STATS";

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

            Option basePath = Option.builder("basePath").argName("basePath")
                    .hasArg()
                    .desc("Base Path, ex. https://v2.namsor.com/NamSorAPIv2")
                    .longOpt("basePath")
                    .required(false)
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

            Option skipErrors = Option.builder("s").argName("skip")
                    .hasArg(false)
                    .desc("skip errors")
                    .longOpt("skip")
                    .required(false)
                    .build();

            Option inputDataFormat = Option.builder("f").argName("inputDataFormat")
                    .hasArg()
                    .desc("input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) / full name, geo country iso2, country subdivision (namegeosub) ")
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

            Option digest = Option.builder("digest").argName("digest")
                    .hasArg(false)
                    .desc("SHA-256 digest names in output")
                    .longOpt("digest")
                    .required(false)
                    .build();

            Option help = Option.builder("h").argName("help")
                    .hasArg(false)
                    .desc("get help")
                    .longOpt("help")
                    .required(false)
                    .build();
            Option usraceethnicityoption = Option.builder("usraceethnicityoption").argName("usraceethnicityoption")
                                .hasArg(true)
                                .desc("extra usraceethnicity option "+NAMSOR_OPTION_USRACEETHNICITY_TAXO_4CLASSES+" "+NAMSOR_OPTION_USRACEETHNICITY_TAXO_4CLASSESCLASSIC+" "+NAMSOR_OPTION_USRACEETHNICITY_TAXO_6CLASSES)
                                .longOpt("usraceethnicityoption")
                                .required(false)
                                .build();
            
            Option repligionoption = Option.builder("religionoption").argName("religionoption")                    
                                .hasArg(false)
                                .desc("extra religion stats option "+NAMSOR_OPTION_RELIGION_STATS +" for country / origin / diaspora")
                                .longOpt("religionoption")
                                .required(false)
                                .build();

            
            Option service = Option.builder("service").argName("service")
                    .hasArg(true)
                    .desc("service : parse / gender / origin / country / diaspora / usraceethnicity / phoneCode / subclassification / religion / castegroup")
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
            options.addOption(basePath);
            options.addOption(inputFile);
            options.addOption(inputDataHasId);
            options.addOption(inputDataFormat);
            options.addOption(outputFile);
            options.addOption(outputHeader);
            options.addOption(service);
            options.addOption(skipErrors);
            options.addOption(encoding);
            options.addOption(outputFileOverwrite);
            options.addOption(countryIso2);
            options.addOption(outputFileRecover);
            options.addOption(digest);
            options.addOption(usraceethnicityoption);
            options.addOption(repligionoption);
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
            if (tools.getDigest() != null) {
                Logger.getLogger(NamSorTools.class.getName()).info("In output, all names will be digested ex. John Smith -> " + tools.digest("John Smith"));
            }
            tools.run();
        } catch (ParseException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (NamSorToolException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
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
        String softwareNameAndVersion = null;
        try {
            // get the software version
            softwareNameAndVersion = adminApi.softwareVersion().getSoftwareNameAndVersion();
        } catch (ApiException ex) {
            Logger.getLogger(NamSorTools.class.getName()).log(Level.SEVERE, null, ex);
            throw new NamSorToolException("Can't get the API version " + ex.getMessage());
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
                outputFileName = inputFileName + "." + service +(usraceethnicityoption==null?"":"_"+usraceethnicityoption) +(religionoption?"_religion":"") + (digest != null ? ".digest" : "") + NAMSOR_EXT;
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
                        if (len < 0) {
                            len = existingData.length;
                        } else if (len != existingData.length) {
                            Logger.getLogger(NamSorTools.class.getName()).warning("Line " + line + " doneLine=" + doneLine + " len=" + existingData.length + "!=" + len);
                        }
                        done.add(existingData[0]);
                    }
                    doneLine = readerDone.readLine();
                    if (line % 100000 == 0) {
                        Logger.getLogger(NamSorTools.class.getName()).info("Loading from existing " + outputFileName + ":" + line);
                    }
                    line++;
                }
                readerDone.close();
            }
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), encoding));
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, isRecover()), encoding));
            process(service, reader, writer, softwareNameAndVersion);
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

    private void process(String service, BufferedReader reader, Writer writer, String softwareNameAndVersion) throws IOException, NamSorToolException, ApiException {
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
        if (appendHeader && !isRecover() || (isRecover() && done.isEmpty())) {
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
                    if (skipErrors) {
                        Logger.getLogger(getClass().getName()).warning("Line " + lineId + ", expected input with format : " + dataFormatExpected.toString() + " line = " + line);
                        lineId++;
                        line = reader.readLine();
                        continue;
                    } else {
                        throw new NamSorToolException("Line " + lineId + ", expected input with format : " + dataFormatExpected.toString() + " line = " + line);
                    }
                }
                String uid = null;
                int col = 0;
                if (isWithUID()) {
                    uid = lineData[col++];
                } else {
                    UUID uuid = UUID.randomUUID();
                    uid = uuid.toString();
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
                    } else if (inputDataFormat.equals(INPUT_DATA_FORMAT_FULLNAMEGEOSUB)) {
                        String fullName = lineData[col++];
                        String countryIso2 = lineData[col++];
                        if ((countryIso2 == null || countryIso2.trim().isEmpty()) && countryIso2Default != null) {
                            countryIso2 = countryIso2Default;
                        }
                        String subDivisionIso31662 = lineData[col++];                        
                        PersonalNameGeoSubdivisionIn personalNameGeoIn = new PersonalNameGeoSubdivisionIn();
                        personalNameGeoIn.setId(uid);
                        personalNameGeoIn.setName(fullName);
                        personalNameGeoIn.setCountryIso2(countryIso2);
                        personalNameGeoIn.setSubdivisionIso(subDivisionIso31662);
                        personalNameGeoSubIn.put(uid, personalNameGeoIn);
                    } else if (inputDataFormat.equals(INPUT_DATA_FORMAT_FNLNPHONE)) {
                        String firstName = lineData[col++];
                        String lastName = lineData[col++];
                        String phoneNumber = lineData[col++];

                        FirstLastNamePhoneNumberIn firstLastNamePhoneNumberIn = new FirstLastNamePhoneNumberIn();
                        firstLastNamePhoneNumberIn.setId(uid);
                        firstLastNamePhoneNumberIn.setFirstName(firstName);
                        firstLastNamePhoneNumberIn.setLastName(lastName);
                        firstLastNamePhoneNumberIn.setPhoneNumber(phoneNumber);
                        firstLastNamesPhoneNumberIn.put(uid, firstLastNamePhoneNumberIn);
                    }
                    processData(service, outputHeaders, writer, false, softwareNameAndVersion);
                }
            }
            lineId++;
            line = reader.readLine();
        }
        processData(service, outputHeaders, writer, true, softwareNameAndVersion);
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
        if(religionoption) {
            for (String outputHeader : OUTPUT_DATA_RELIGIONSTAT_HEADER) {
                writer.append(outputHeader + separatorOut);
            }            
        }
        writer.append("version" + separatorOut);
        writer.append("rowId" + "\n");
        writer.flush();
    }

    private Map<String, FirstLastNameGeoSubclassificationOut> processSubclassification(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameGeoSubclassificationOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameGeoSubclassificationOut origined = personalApi.subclassificationBatch(body);
        for (FirstLastNameGeoSubclassificationOut personalName : origined.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }
    
    private Map<String, PersonalNameGeoSubclassificationOut> processSubclassificationFull(List<PersonalNameGeoIn> names) throws ApiException, IOException {
        Map<String, PersonalNameGeoSubclassificationOut> result = new HashMap();
        BatchPersonalNameGeoIn body = new BatchPersonalNameGeoIn();
        body.setPersonalNames(names);
        BatchPersonalNameGeoSubclassificationOut origined = personalApi.subclassificationFullBatch(body);
        for (PersonalNameGeoSubclassificationOut personalName : origined.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }
    
    private Map<String, FirstLastNameDiasporaedOut> processDiaspora(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameDiasporaedOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameDiasporaedOut origined = personalApi.diasporaBatch(body);
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
        BatchFirstLastNameOriginedOut origined = personalApi.originBatch(body);
        for (FirstLastNameOriginedOut personalName : origined.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameGenderedOut> processGender(List<FirstLastNameIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameGenderedOut> result = new HashMap();
        BatchFirstLastNameIn body = new BatchFirstLastNameIn();
        body.setPersonalNames(names);
        BatchFirstLastNameGenderedOut gendered = personalApi.genderBatch(body);
        for (FirstLastNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, PersonalNameGenderedOut> processGenderFull(List<PersonalNameIn> names) throws ApiException, IOException {
        Map<String, PersonalNameGenderedOut> result = new HashMap();
        BatchPersonalNameIn body = new BatchPersonalNameIn();
        body.setPersonalNames(names);
        BatchPersonalNameGenderedOut gendered = personalApi.genderFullBatch(body);
        for (PersonalNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, PersonalNameGeoOut> processCountry(List<PersonalNameIn> names) throws ApiException, IOException {
        Map<String, PersonalNameGeoOut> result = new HashMap();
        BatchPersonalNameIn body = new BatchPersonalNameIn();
        body.setPersonalNames(names);
        BatchPersonalNameGeoOut countried = personalApi.countryBatch(body);
        for (PersonalNameGeoOut personalName : countried.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, PersonalNameGeoOut> processCountryGeo(List<PersonalNameGeoIn> names) throws ApiException, IOException {
        Map<String, PersonalNameGeoOut> result = new HashMap();
        BatchPersonalNameIn body = new BatchPersonalNameIn();
        List<PersonalNameIn> names_ = new ArrayList();
        for (PersonalNameGeoIn name : names) {
            PersonalNameIn noGeo = new PersonalNameIn();
            noGeo.setId(name.getId());
            noGeo.setName(name.getName());
            names_.add(noGeo);
        }
        body.setPersonalNames(names_);
        BatchPersonalNameGeoOut countried = personalApi.countryBatch(body);
        for (PersonalNameGeoOut personalName : countried.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }
    
    private Map<String, PersonalNameReligionedOut> processReligion(List<PersonalNameGeoSubdivisionIn> names) throws ApiException, IOException {
        Map<String, PersonalNameReligionedOut> result = new HashMap();
        BatchPersonalNameGeoSubdivisionIn body = new BatchPersonalNameGeoSubdivisionIn();
        body.setPersonalNames(names);
        BatchPersonalNameReligionedOut countried = personalApi.religionFullBatch(body);
        for (PersonalNameReligionedOut personalName : countried.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }        

    private List<PersonalNameSubdivisionIn> adaptPersonalNameGeoSubdivisionIn(List<PersonalNameGeoSubdivisionIn> names) {
        List<PersonalNameSubdivisionIn> res= new ArrayList();
        for (PersonalNameGeoSubdivisionIn re : names) {
            PersonalNameSubdivisionIn o = new PersonalNameSubdivisionIn();
            o.setId(re.getId());
            o.setName(re.getName());
            o.setSubdivisionIso(re.getSubdivisionIso());
            res.add(o);
        }
        return res;
    }
    
    private Map<String, PersonalNameCastegroupOut> processCastegoup(List<PersonalNameGeoSubdivisionIn> names) throws ApiException, IOException {
        Map<String, PersonalNameCastegroupOut> result = new HashMap();
        BatchPersonalNameSubdivisionIn body = new BatchPersonalNameSubdivisionIn();
        body.setPersonalNames(adaptPersonalNameGeoSubdivisionIn(names));
        BatchPersonalNameCastegroupOut countried = indianApi.castegroupIndianFullBatch(body);
        for (PersonalNameCastegroupOut personalName : countried.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }        
    

    private Map<String, ProperNounCategorizedOut> processNameType(List<PersonalNameIn> names_) throws ApiException, IOException {
        List<NameIn> names = new ArrayList();
        for (PersonalNameIn personalNameIn : names_) {
            NameIn name = new NameIn();
            name.setId(personalNameIn.getId());
            name.setName(personalNameIn.getName());
            names.add(name);
        }
        Map<String, ProperNounCategorizedOut> result = new HashMap();
        BatchNameIn body = new BatchNameIn();
        body.setProperNouns(names);
        BatchProperNounCategorizedOut nameTypedOut = generalApi.nameTypeBatch(body);
        for (ProperNounCategorizedOut nameTyped : nameTypedOut.getProperNouns()) {
            result.put(nameTyped.getId(), nameTyped);
        }
        return result;
    }

    private Map<String, ProperNounCategorizedOut> processNameTypeGeo(List<PersonalNameGeoIn> names_) throws ApiException, IOException {
        List<NameGeoIn> names = new ArrayList();
        for (PersonalNameGeoIn personalNameIn : names_) {
            NameGeoIn name = new NameGeoIn();
            name.setId(personalNameIn.getId());
            name.setName(personalNameIn.getName());
            name.setCountryIso2(personalNameIn.getCountryIso2());
            names.add(name);
        }
        Map<String, ProperNounCategorizedOut> result = new HashMap();
        BatchNameGeoIn body = new BatchNameGeoIn();
        body.setProperNouns(names);
        BatchProperNounCategorizedOut nameTypedOut = generalApi.nameTypeGeoBatch(body);
        for (ProperNounCategorizedOut nameTyped : nameTypedOut.getProperNouns()) {
            result.put(nameTyped.getId(), nameTyped);
        }
        return result;
    }
    
        
    private Map<String, PersonalNameGeoOut> processCountryAdapted(List<FirstLastNameIn> names_) throws ApiException, IOException {
        List<PersonalNameIn> names = new ArrayList();
        for (FirstLastNameIn name : names_) {
            PersonalNameIn adapted = new PersonalNameIn();
            adapted.setId(name.getId());
            adapted.setName(name.getFirstName() + " " + name.getLastName());
            names.add(adapted);
        }
        return processCountry(names);
    }

    private Map<String, PersonalNameGenderedOut> processGenderFullGeo(List<PersonalNameGeoIn> names) throws ApiException, IOException {
        Map<String, PersonalNameGenderedOut> result = new HashMap();
        BatchPersonalNameGeoIn body = new BatchPersonalNameGeoIn();
        body.setPersonalNames(names);
        BatchPersonalNameGenderedOut gendered = personalApi.genderFullGeoBatch(body);
        for (PersonalNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, PersonalNameParsedOut> processParse(List<PersonalNameIn> names) throws ApiException, IOException {
        Map<String, PersonalNameParsedOut> result = new HashMap();
        BatchPersonalNameIn body = new BatchPersonalNameIn();
        body.setPersonalNames(names);
        BatchPersonalNameParsedOut parsed = personalApi.parseNameBatch(body);
        for (PersonalNameParsedOut personalName : parsed.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameGenderedOut> processGenderGeo(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameGenderedOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameGenderedOut gendered = personalApi.genderGeoBatch(body);
        for (FirstLastNameGenderedOut personalName : gendered.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, PersonalNameParsedOut> processParseGeo(List<PersonalNameGeoIn> names) throws ApiException, IOException {
        Map<String, PersonalNameParsedOut> result = new HashMap();
        BatchPersonalNameGeoIn body = new BatchPersonalNameGeoIn();
        body.setPersonalNames(names);
        BatchPersonalNameParsedOut parsed = personalApi.parseNameGeoBatch(body);
        for (PersonalNameParsedOut personalName : parsed.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNameUSRaceEthnicityOut> processUSRaceEthnicity(List<FirstLastNameGeoIn> names) throws ApiException, IOException {
        Map<String, FirstLastNameUSRaceEthnicityOut> result = new HashMap();
        BatchFirstLastNameGeoIn body = new BatchFirstLastNameGeoIn();
        body.setPersonalNames(names);
        BatchFirstLastNameUSRaceEthnicityOut racedEthnicized = personalApi.usRaceEthnicityBatch(body);
        for (FirstLastNameUSRaceEthnicityOut personalName : racedEthnicized.getPersonalNames()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private Map<String, FirstLastNamePhoneCodedOut> processPhoneCode(ArrayList<FirstLastNamePhoneNumberIn> names) throws ApiException {
        Map<String, FirstLastNamePhoneCodedOut> result = new HashMap();
        BatchFirstLastNamePhoneNumberIn body = new BatchFirstLastNamePhoneNumberIn();
        body.setPersonalNamesWithPhoneNumbers(names);
        BatchFirstLastNamePhoneCodedOut phoneCoded = socialApi.phoneCodeBatch(body);
        for (FirstLastNamePhoneCodedOut personalName : phoneCoded.getPersonalNamesWithPhoneNumbers()) {
            result.put(personalName.getId(), personalName);
        }
        return result;
    }

    private void processData(String service, String[] outputHeaders, Writer writer, boolean flushBuffers, String softwareNameAndVersion) throws ApiException, IOException {
        if (flushBuffers && !firstLastNamesIn.isEmpty() || firstLastNamesIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_ORIGIN)) {
                Map<String, FirstLastNameOriginedOut> origins = processOrigin(new ArrayList(firstLastNamesIn.values()));
                append(writer, outputHeaders, firstLastNamesIn, origins, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGender(new ArrayList(firstLastNamesIn.values()));
                append(writer, outputHeaders, firstLastNamesIn, genders, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_COUNTRY)) {
                Map<String, PersonalNameGeoOut> countrieds = processCountryAdapted(new ArrayList(firstLastNamesIn.values()));
                append(writer, outputHeaders, firstLastNamesIn, countrieds, softwareNameAndVersion);
            }
            firstLastNamesIn.clear();
        }
        if (flushBuffers && !firstLastNamesGeoIn.isEmpty() || firstLastNamesGeoIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_ORIGIN)) {
                Map<String, FirstLastNameOriginedOut> origins = processOriginGeo(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, origins, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGenderGeo(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, genders, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_DIASPORA)) {
                Map<String, FirstLastNameDiasporaedOut> diasporas = processDiaspora(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, diasporas, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_USRACEETHNICITY)) {
                Map<String, FirstLastNameUSRaceEthnicityOut> usRaceEthnicities = processUSRaceEthnicity(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, usRaceEthnicities, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_SUBDIVISION)) {
                Map<String, FirstLastNameGeoSubclassificationOut> subclassifications = processSubclassification(new ArrayList(firstLastNamesGeoIn.values()));
                append(writer, outputHeaders, firstLastNamesGeoIn, subclassifications, softwareNameAndVersion);
            }
            firstLastNamesGeoIn.clear();
        }
        if (flushBuffers && !personalNamesIn.isEmpty() || personalNamesIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_PARSE)) {
                Map<String, PersonalNameParsedOut> parseds = processParse(new ArrayList(personalNamesIn.values()));
                append(writer, outputHeaders, personalNamesIn, parseds, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGenderFull(new ArrayList(personalNamesIn.values()));
                append(writer, outputHeaders, personalNamesIn, genders, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_COUNTRY)) {
                Map<String, PersonalNameGeoOut> countrieds = processCountry(new ArrayList(personalNamesIn.values()));
                append(writer, outputHeaders, personalNamesIn, countrieds, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_TYPE)) {
                Map<String, ProperNounCategorizedOut> nameTypeds = processNameType(new ArrayList(personalNamesIn.values()));
                append(writer, outputHeaders, personalNamesIn, nameTypeds, softwareNameAndVersion);
            }
            personalNamesIn.clear();
        }
        if (flushBuffers && !personalNamesGeoIn.isEmpty() || personalNamesGeoIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_PARSE)) {
                Map<String, PersonalNameParsedOut> parseds = processParseGeo(new ArrayList(personalNamesGeoIn.values()));
                append(writer, outputHeaders, personalNamesGeoIn, parseds, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_GENDER)) {
                Map<String, FirstLastNameGenderedOut> genders = processGenderFullGeo(new ArrayList(personalNamesGeoIn.values()));
                append(writer, outputHeaders, personalNamesGeoIn, genders, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_COUNTRY)) {
                Map<String, PersonalNameGeoOut> countrieds = processCountryGeo(new ArrayList(personalNamesGeoIn.values()));
                append(writer, outputHeaders, personalNamesGeoIn, countrieds, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_TYPE)) {
                Map<String, ProperNounCategorizedOut> nameTypeds = processNameTypeGeo(new ArrayList(personalNamesGeoIn.values()));
                append(writer, outputHeaders, personalNamesGeoIn, nameTypeds, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_SUBDIVISION)) {
                Map<String, PersonalNameGeoSubclassificationOut> subclassifications = processSubclassificationFull(new ArrayList(personalNamesGeoIn.values()));
                append(writer, outputHeaders, personalNamesGeoIn, subclassifications, softwareNameAndVersion);
            }
            
            personalNamesGeoIn.clear();
        }
        if (flushBuffers && !personalNameGeoSubIn.isEmpty() || personalNameGeoSubIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_RELIGION)) {
                Map<String, PersonalNameReligionedOut> religioned = processReligion(new ArrayList(personalNameGeoSubIn.values()));
                append(writer, outputHeaders, personalNameGeoSubIn, religioned, softwareNameAndVersion);
            } else if (service.equals(SERVICE_NAME_CASTEGROUP)) {
                Map<String, PersonalNameCastegroupOut> castegrouped = processCastegoup(new ArrayList(personalNameGeoSubIn.values()));
                append(writer, outputHeaders, personalNameGeoSubIn, castegrouped, softwareNameAndVersion);
            } 
            personalNameGeoSubIn.clear();
        }
        
        if (flushBuffers && !firstLastNamesPhoneNumberIn.isEmpty() || firstLastNamesPhoneNumberIn.size() >= BATCH_SIZE) {
            if (service.equals(SERVICE_NAME_PHONECODE)) {
                Map<String, FirstLastNamePhoneCodedOut> phoneCodes = processPhoneCode(new ArrayList(firstLastNamesPhoneNumberIn.values()));
                append(writer, outputHeaders, firstLastNamesPhoneNumberIn, phoneCodes, softwareNameAndVersion);
            }
            firstLastNamesPhoneNumberIn.clear();
        }
    }

    private void append(Writer writer, String[] outputHeaders, Map input, Map output, String softwareNameAndVersion) throws IOException {
        Set<String> flushedUID = new HashSet();
        for (Object idObj : input.keySet()) {
            String uid = (String) idObj;
            flushedUID.add(uid);
            Object inputObj = input.get(uid);
            Object outputObj = output.get(uid);
            writer.append(uid + separatorOut);
            if (inputObj instanceof FirstLastNameIn) {
                FirstLastNameIn firstLastNameIn = (FirstLastNameIn) inputObj;
                writer.append(digest(firstLastNameIn.getFirstName()) + separatorOut + digest(firstLastNameIn.getLastName()) + separatorOut);
            } else if (inputObj instanceof FirstLastNameGeoIn) {
                FirstLastNameGeoIn firstLastNameGeoIn = (FirstLastNameGeoIn) inputObj;
                writer.append(digest(firstLastNameGeoIn.getFirstName()) + separatorOut + digest(firstLastNameGeoIn.getLastName()) + separatorOut + firstLastNameGeoIn.getCountryIso2() + separatorOut);
            } else if (inputObj instanceof PersonalNameIn) {
                PersonalNameIn personalNameIn = (PersonalNameIn) inputObj;
                writer.append(digest(personalNameIn.getName()) + separatorOut);
            } else if (inputObj instanceof PersonalNameGeoIn) {
                PersonalNameGeoIn personalNameGeoIn = (PersonalNameGeoIn) inputObj;
                writer.append(digest(personalNameGeoIn.getName()) + separatorOut + personalNameGeoIn.getCountryIso2() + separatorOut);
            } else if (inputObj instanceof PersonalNameGeoSubdivisionIn) {
                PersonalNameGeoSubdivisionIn personalNameGeoIn = (PersonalNameGeoSubdivisionIn) inputObj;
                writer.append(digest(personalNameGeoIn.getName()) + separatorOut + personalNameGeoIn.getCountryIso2() + separatorOut+ personalNameGeoIn.getSubdivisionIso() + separatorOut);
            } else if (inputObj instanceof NameIn) {
                NameIn personalNameIn = (NameIn) inputObj;
                writer.append(digest(personalNameIn.getName()) + separatorOut);
            } else if (inputObj instanceof NameGeoIn) {
                NameGeoIn personalNameGeoIn = (NameGeoIn) inputObj;
                writer.append(digest(personalNameGeoIn.getName()) + separatorOut + personalNameGeoIn.getCountryIso2() + separatorOut);
            } else if (inputObj instanceof FirstLastNamePhoneNumberIn) {
                FirstLastNamePhoneNumberIn firstLastNamePhoneNumberIn = (FirstLastNamePhoneNumberIn) inputObj;
                writer.append(digest(firstLastNamePhoneNumberIn.getFirstName()) + separatorOut + digest(firstLastNamePhoneNumberIn.getLastName())+ separatorOut+ digest(firstLastNamePhoneNumberIn.getPhoneNumber())+ separatorOut);
                //

            } else {
                throw new IllegalArgumentException("Serialization of " + inputObj.getClass().getName() + " not supported");
            }
            if (outputObj == null) {
                for (String outputHeader : outputHeaders) {
                    writer.append("" + separatorOut);
                }
            } else if (outputObj instanceof FirstLastNameGenderedOut) {
                FirstLastNameGenderedOut firstLastNameGenderedOut = (FirstLastNameGenderedOut) outputObj;
                String scriptName = firstLastNameGenderedOut.getScript(); //NamSorTools.computeScriptFirst(firstLastNameGenderedOut.getLastName());
                writer.append(firstLastNameGenderedOut.getLikelyGender().getValue() + separatorOut + firstLastNameGenderedOut.getProbabilityCalibrated() + separatorOut + firstLastNameGenderedOut.getScore() + separatorOut + firstLastNameGenderedOut.getGenderScale() + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof FirstLastNameOriginedOut) {
                FirstLastNameOriginedOut firstLastNameOriginedOut = (FirstLastNameOriginedOut) outputObj;
                String scriptName = firstLastNameOriginedOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameOriginedOut.getLastName());
                //"region","topRegion","subRegion"
                writer.append(firstLastNameOriginedOut.getRegionOrigin() + separatorOut + firstLastNameOriginedOut.getTopRegionOrigin() + separatorOut + firstLastNameOriginedOut.getSubRegionOrigin() + separatorOut + firstLastNameOriginedOut.getCountryOrigin() + separatorOut + firstLastNameOriginedOut.getCountryOriginAlt() + separatorOut + firstLastNameOriginedOut.getProbabilityCalibrated() + separatorOut + firstLastNameOriginedOut.getProbabilityAltCalibrated() + separatorOut + firstLastNameOriginedOut.getScore() + separatorOut + toCSV(firstLastNameOriginedOut.getCountriesOriginTop()) + separatorOut + scriptName + separatorOut);
                if( religionoption ) {
                    appendReligionStat(writer, firstLastNameOriginedOut.getReligionStats(),firstLastNameOriginedOut.getReligionStatsAlt());
                }
            } else if (outputObj instanceof ProperNounCategorizedOut) {
                ProperNounCategorizedOut properNounCategorizedOut = (ProperNounCategorizedOut) outputObj;
                String scriptName = properNounCategorizedOut.getScript();//NamSorTools.computeScriptFirst(properNounCategorizedOut.getName());
                writer.append(properNounCategorizedOut.getCommonType() + separatorOut + properNounCategorizedOut.getCommonTypeAlt() + separatorOut + properNounCategorizedOut.getScore() + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof FirstLastNameDiasporaedOut) {
                FirstLastNameDiasporaedOut firstLastNameDiasporaedOut = (FirstLastNameDiasporaedOut) outputObj;
                String scriptName = firstLastNameDiasporaedOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameDiasporaedOut.getLastName());
                writer.append(firstLastNameDiasporaedOut.getEthnicity() + separatorOut + firstLastNameDiasporaedOut.getEthnicityAlt() + separatorOut + firstLastNameDiasporaedOut.getProbabilityCalibrated() + separatorOut + firstLastNameDiasporaedOut.getProbabilityAltCalibrated() + separatorOut + firstLastNameDiasporaedOut.getScore() + separatorOut + toCSV(firstLastNameDiasporaedOut.getEthnicitiesTop()) + separatorOut + scriptName + separatorOut);
                if( religionoption ) {
                    appendReligionStat(writer, firstLastNameDiasporaedOut.getReligionStats(),firstLastNameDiasporaedOut.getReligionStatsAlt());
                }
            } else if (outputObj instanceof FirstLastNameGeoSubclassificationOut) {
                FirstLastNameGeoSubclassificationOut firstLastNameGeoSubclassificationOut = (FirstLastNameGeoSubclassificationOut) outputObj;
                String scriptName = firstLastNameGeoSubclassificationOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameDiasporaedOut.getLastName());
                writer.append(firstLastNameGeoSubclassificationOut.getSubClassification() + separatorOut + firstLastNameGeoSubclassificationOut.getSubClassificationAlt() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityCalibrated() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityAltCalibrated()  + separatorOut + firstLastNameGeoSubclassificationOut.getScore() + separatorOut + toCSV(firstLastNameGeoSubclassificationOut.getSubclassificationTop()) + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof PersonalNameReligionedOut) {
                PersonalNameReligionedOut firstLastNameGeoSubclassificationOut = (PersonalNameReligionedOut) outputObj;
                String scriptName = firstLastNameGeoSubclassificationOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameDiasporaedOut.getLastName());
                writer.append(firstLastNameGeoSubclassificationOut.getReligion() + separatorOut + firstLastNameGeoSubclassificationOut.getReligionAlt() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityCalibrated() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityAltCalibrated()  + separatorOut + firstLastNameGeoSubclassificationOut.getScore() + separatorOut + toCSV(firstLastNameGeoSubclassificationOut.getReligionsTop()) + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof PersonalNameCastegroupOut) {
                PersonalNameCastegroupOut firstLastNameGeoSubclassificationOut = (PersonalNameCastegroupOut) outputObj;
                String scriptName = firstLastNameGeoSubclassificationOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameDiasporaedOut.getLastName());
                writer.append(firstLastNameGeoSubclassificationOut.getCastegroup() + separatorOut + firstLastNameGeoSubclassificationOut.getCastegroupAlt() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityCalibrated() + separatorOut + firstLastNameGeoSubclassificationOut.getProbabilityAltCalibrated()  + separatorOut + firstLastNameGeoSubclassificationOut.getScore() + separatorOut + toCSV(firstLastNameGeoSubclassificationOut.getCastegroupTop()) + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof FirstLastNameUSRaceEthnicityOut) {
                FirstLastNameUSRaceEthnicityOut firstLastNameUSRaceEthnicityOut = (FirstLastNameUSRaceEthnicityOut) outputObj;
                String scriptName = firstLastNameUSRaceEthnicityOut.getScript();//NamSorTools.computeScriptFirst(firstLastNameUSRaceEthnicityOut.getLastName());
                writer.append(firstLastNameUSRaceEthnicityOut.getRaceEthnicity() + separatorOut + firstLastNameUSRaceEthnicityOut.getRaceEthnicityAlt() + separatorOut  + firstLastNameUSRaceEthnicityOut.getProbabilityCalibrated() + separatorOut + firstLastNameUSRaceEthnicityOut.getProbabilityAltCalibrated() + separatorOut + firstLastNameUSRaceEthnicityOut.getScore() + separatorOut + toCSV(firstLastNameUSRaceEthnicityOut.getRaceEthnicitiesTop()) + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof PersonalNameGenderedOut) {
                PersonalNameGenderedOut personalNameGenderedOut = (PersonalNameGenderedOut) outputObj;
                String scriptName = personalNameGenderedOut.getScript();//NamSorTools.computeScriptFirst(personalNameGenderedOut.getName());
                writer.append(personalNameGenderedOut.getLikelyGender().getValue() + separatorOut + personalNameGenderedOut.getScore() + separatorOut + personalNameGenderedOut.getProbabilityCalibrated() + separatorOut + personalNameGenderedOut.getGenderScale() + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof PersonalNameGeoOut) {
                PersonalNameGeoOut personalNameGeoOut = (PersonalNameGeoOut) outputObj;
                String scriptName = personalNameGeoOut.getScript();//NamSorTools.computeScriptFirst(personalNameGeoOut.getName());
                //"region","topRegion","subRegion"
                writer.append(personalNameGeoOut.getRegion() + separatorOut + personalNameGeoOut.getTopRegion() + separatorOut + personalNameGeoOut.getSubRegion() + separatorOut + personalNameGeoOut.getCountry() + separatorOut + personalNameGeoOut.getCountryAlt()  + separatorOut + personalNameGeoOut.getProbabilityCalibrated() + separatorOut + personalNameGeoOut.getProbabilityAltCalibrated() + separatorOut +personalNameGeoOut.getScore() + separatorOut + toCSV(personalNameGeoOut.getCountriesTop()) +  separatorOut + scriptName + separatorOut);
                if( religionoption ) {
                    appendReligionStat(writer, personalNameGeoOut.getReligionStats(),personalNameGeoOut.getReligionStatsAlt());
                }
            } else if (outputObj instanceof PersonalNameParsedOut) {
                PersonalNameParsedOut personalNameParsedOut = (PersonalNameParsedOut) outputObj;
                //  {"firstNameParsed", "lastNameParsed", "nameParserType", "nameParserTypeAlt", "nameParserTypeScore"};
                String firstNameParsed = (personalNameParsedOut.getFirstLastName() != null ? personalNameParsedOut.getFirstLastName().getFirstName() : "");
                String lastNameParsed = (personalNameParsedOut.getFirstLastName() != null ? personalNameParsedOut.getFirstLastName().getLastName() : "");
                String scriptName = personalNameParsedOut.getScript();//NamSorTools.computeScriptFirst(personalNameParsedOut.getName());
                writer.append(firstNameParsed + separatorOut + lastNameParsed + separatorOut + personalNameParsedOut.getNameParserType() + separatorOut + personalNameParsedOut.getNameParserTypeAlt() + separatorOut + personalNameParsedOut.getScore() + separatorOut + scriptName + separatorOut);
            } else if (outputObj instanceof FirstLastNamePhoneCodedOut) {
                FirstLastNamePhoneCodedOut firstLastNamePhoneCodedOut = (FirstLastNamePhoneCodedOut) outputObj;
                String scriptName = firstLastNamePhoneCodedOut.getScript();//NamSorTools.computeScriptFirst(firstLastNamePhoneCodedOut.getLastName());
                writer.append(firstLastNamePhoneCodedOut.getInternationalPhoneNumberVerified() + separatorOut
                        + firstLastNamePhoneCodedOut.getPhoneCountryIso2Verified() + separatorOut
                        + firstLastNamePhoneCodedOut.getPhoneCountryCode() + separatorOut
                        + firstLastNamePhoneCodedOut.getPhoneCountryCodeAlt() + separatorOut
                        + firstLastNamePhoneCodedOut.getPhoneCountryIso2() + separatorOut
                        + firstLastNamePhoneCodedOut.getPhoneCountryIso2Alt() + separatorOut
                        + firstLastNamePhoneCodedOut.getOriginCountryIso2() + separatorOut
                        + firstLastNamePhoneCodedOut.getOriginCountryIso2Alt() + separatorOut
                        + firstLastNamePhoneCodedOut.getVerified() + separatorOut
                        + firstLastNamePhoneCodedOut.getScore() + separatorOut
                        + scriptName);
            } else {
                throw new IllegalArgumentException("Serialization of " + outputObj.getClass().getName() + " not supported");
            }
            writer.append(softwareNameAndVersion + separatorOut);
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
    private final Map<String, PersonalNameGeoSubdivisionIn> personalNameGeoSubIn = new HashMap();
    private final Map<String, FirstLastNamePhoneNumberIn> firstLastNamesPhoneNumberIn = new HashMap();

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

    /**
     * @return the digest
     */
    public MessageDigest getDigest() {
        return digest;
    }

    public String toCSV(List<String> topClasses) {
        StringWriter sw = new StringWriter();
        for (String topClass : topClasses) {
            sw.append(topClass+";");
        }
        return sw.toString();
    }

    private void appendReligionStat(Writer writer, List<ReligionStatOut> religionStats, List<ReligionStatOut> religionStatsAlt) throws IOException {        
        if( religionStats != null && religionStats.size()>0) {
            writer.append(religionStats.get(0).getReligion() + separatorOut + religionStats.get(0).getPct() + separatorOut);
        } else {
            writer.append("" + separatorOut + "" + separatorOut);
        }
        if( religionStatsAlt != null && religionStatsAlt.size()>0) {
            writer.append(religionStatsAlt.get(0).getReligion() + separatorOut + religionStatsAlt.get(0).getPct() + separatorOut);
        } else {
            writer.append("" + separatorOut + "" + separatorOut);
        }        
    }
}
