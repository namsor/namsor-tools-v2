# namsor-tools-v2
NamSor command line tools, to append gender, origin, diaspora or us 'race'/ethnicity to a CSV file. The CSV file should in UTF-8 encoding, pipe-| demimited. It can be very large. Check out also the Python CLT (https://github.com/namsor/namsor-python-tools-v2)

## Installation

Please install Maven first, then build the executable with command
```bash
mvn clean package
```
NB: we use Unix conventions for file paths, ex. samples/some_fnln.txt but on MS Windows that would be samples\some_fnln.txt

## Usage

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar
usage: NamSorTools -apiKey <apiKey> [-basePath <basePath>] [-countryIso2
       <countryIso2>] [-digest] [-e <encoding>] -f <inputDataFormat> [-h]
       [-header] -i <inputFile> [-o <outputFile>] [-r] [-religionoption]
       [-s] -service <service> [-uid] [-usraceethnicityoption
       <usraceethnicityoption>] [-w]

       
 -apiKey,--apiKey <apiKey>
 NamSor API Key
 -basePath,--basePath <basePath>
 Base Path, ex. https://v2.namsor.com/NamSorAPIv2
 -countryIso2,--countryIso2 <countryIso2>
 countryIso2 default
 -digest,--digest
 SHA-256 digest names in output
 -e,--encoding <encoding>
 encoding : UTF-8 by default
 -f,--inputDataFormat <inputDataFormat>
 input data format : first name, last name (fnln) / first name, last name,
 geo country iso2 (fnlngeo) / full name (name) / full name, geo country
 iso2 (namegeo) / full name, geo country iso2, country subdivision
 (namegeosub)
 -h,--help
 get help
 -header,--header
 output header
 -i,--inputFile <inputFile>
 input file name
 -o,--outputFile <outputFile>
 output file name
 -r,--recover
 continue from a job (requires uid)
 -religionoption,--religionoption
 extra religion stats option X-OPTION-RELIGION-STATS for country / origin
 / diaspora
 -s,--skip
 skip errors
 -service,--endpoint <service>
 service : parse / gender / origin / country / diaspora / usraceethnicity
 / phoneCode / subclassification / religion / castegroup
 -uid,--uid
 input data has an ID prefix
 -usraceethnicityoption,--usraceethnicityoption <usraceethnicityoption>
 extra usraceethnicity option USRACEETHNICITY-4CLASSES
 USRACEETHNICITY-4CLASSES-CLASSIC USRACEETHNICITY-6CLASSES
 -w,--overwrite
 overwrite existing output file
```

## Examples

To append the likely name gender to a list of first and last names : John|Smith

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -f fnln -i samples/some_fnln.txt -service gender
```

To append the likely name origin to a list of first and last names : John|Smith

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -f fnln -i samples/some_fnln.txt -service origin
```

To append the likely country of residence to a list of full names : John Smith

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -f name -i samples/some_name.txt -service country
```


To parse names into first and last name components (John Smith or Smith, John -> John|Smith)

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -f name -i samples/some_name.txt -service parse
```

The recommended input format is to specify a unique ID and a geographic context (if known) as a countryIso2 code. 

To append gender to a list of id, first and last names, geographic context : id12|John|Smith|US

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -uid -f fnlngeo -i samples/some_idfnlngeo.txt -service gender
```

To append the ethnicity (in the sense of cultural heritage / country of origin of ascendents) from a list of id, first and last names, geographic context : id12|John|Smith|US

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -uid -f fnlngeo -i samples/some_idfnlngeo.txt -service diaspora
```

To parse name into first and last name components, a geographic context is recommended (esp. for Latam names) : id12|John Smith|US

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -w -header -uid -f namegeo -i samples/some_idnamegeo.txt -service parse
```
On large input files with a unique ID, it is possible to recover from where the process crashed and append to the existint output file, for example :

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey> -r -header -uid -f fnlngeo -i samples/some_idfnlngeo.txt -service gender
```

For Indian names (for now), you can infer the likely india state or union territory (ie. a subdivision of the country as per ISO 3166-2:IN)

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey>  -r -header -uid -f fnlngeo -i samples/some_indian_idfnlngeo.txt -service subdivision
```

For Indian names (for now), you can infer the likely religion (provided the IN country code and state/union territory as per ISO 3166-2:IN)

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey>  -r -header -uid -f namegeosub -i samples/some_indian_idnamegeosub.txt -service religion
```

For Indian names (for now), you can infer the likely caste group (provided the IN country code and state/union territory as per ISO 3166-2:IN)

```bash
java -jar target/NamSorToolsV2-0.26-SNAPSHOT.jar -apiKey <yourAPIKey>  -r -header -uid -f namegeosub -i samples/some_indian_idnamegeosub.txt -service castegroup
```


## Anonymizing output data
The -digest option will digest personal names in file outpus, using a non reversible MD-5 hash. For example, John Smith will become 6117323d2cabbc17d44c2b44587f682c.
Please note that this doesn't apply to the PARSE output. 

## Understanding output
Please read and contribute to the WIKI
https://github.com/namsor/namsor-tools-v2/wiki/NamSor-Tools-V2

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.



