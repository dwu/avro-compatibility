package com.hotels.avro.compatibility;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaParseException;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Cli {

    private static Schema parseSchema(String schemaString) {
        try {
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(schemaString);
        } catch (SchemaParseException e) {
            return null;
        }
    }

    public void checkCompatibility(String sourceSchema, String targetSchema, Compatibility.CheckType checkType) {
        Schema source = parseSchema(sourceSchema);
        Schema target = parseSchema(targetSchema);

        CompatibilityCheckResult result = null;
        switch (checkType) {
            case CAN_BE_READ_BY:
                result = Compatibility.checkThat(source).canBeReadBy(target);
                if (result.isCompatible()) {
                    System.out.println("Result: Source schema CAN be read with target schema");
                } else {
                    System.out.println("Result:\nSource schema CANNOT be read with target schema.\n\nReason(s):");
                    printIncompatibilities(result.getResult().getIncompatibilities());
                }
                break;
            case MUTUAL_READ:
                result = Compatibility.checkThat(source).mutualReadWith(target);
                if (result.isCompatible()) {
                    System.out.println("Result: Source schema CAN be mutually read with target schema");
                } else {
                    System.out.println("Result:\nSource schema CANNOT be mutually read with target schema.\n\nReason(s):");
                    printIncompatibilities(result.getResult().getIncompatibilities());
                }
                break;
        }
    }

    private void printIncompatibilities(List<SchemaCompatibility.Incompatibility> incompatibilities) {
        for (SchemaCompatibility.Incompatibility incompatibility : incompatibilities) {
            System.out.println(String.format("- Type: %s\n  Location: %s\n  Source: %s\n  Target: %s", incompatibility.getType(), incompatibility.getLocation(), incompatibility.getWriterFragment(), incompatibility.getReaderFragment()));
        }
    }

    public static void main(String[] args) {
        Cli cli = new Cli();

        Options options = new Options();
        options.addOption(Option.builder("s").longOpt("source").hasArg().desc("Source schema for compatibility check").required().build());
        options.addOption(Option.builder("t").longOpt("target").hasArg().desc("Target schema for compatibility check").required().build());
        options.addOption(Option.builder("m").longOpt("mutual").desc("Check for mutual compatibility.\nDefault: Check that source can be read by target.").build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            String source = line.getOptionValue("s");
            String target = line.getOptionValue("t");

            String sourceSchema = Files.asCharSource(new File(source), Charsets.UTF_8).read();
            String targetSchema = Files.asCharSource(new File(target), Charsets.UTF_8).read();

            if (line.hasOption("m")) {
                cli.checkCompatibility(sourceSchema, targetSchema, Compatibility.CheckType.MUTUAL_READ);
            } else {
                cli.checkCompatibility(sourceSchema, targetSchema, Compatibility.CheckType.CAN_BE_READ_BY);
            }
        } catch (ParseException e) {
            System.err.println("Invalid command line arguments. " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("avro-compatibility", options);
        } catch (IOException e) {
            System.err.println("Cannot read input file(s). " + e.getMessage());
        }
    }

}
