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

    public void checkCompatibility(String writerSchema, String readerSchema, Compatibility.CheckType checkType) {
        Schema writer = parseSchema(writerSchema);
        Schema reader = parseSchema(readerSchema);

        CompatibilityCheckResult result = null;
        switch (checkType) {
            case CAN_BE_READ_BY:
                result = Compatibility.checkThat(writer).canBeReadBy(reader);
                if (result.isCompatible()) {
                    System.out.println("Result: Writer schema CAN be read with reader schema");
                } else {
                    System.out.println("Result:\nWriter schema CANNOT be read with reader schema.\n\nReason(s):");
                    printIncompatibilities(result.getResult().getIncompatibilities());
                }
                break;
            case MUTUAL_READ:
                result = Compatibility.checkThat(writer).mutualReadWith(reader);
                if (result.isCompatible()) {
                    System.out.println("Result: Writer schema CAN be mutually read with reader schema");
                } else {
                    System.out.println("Result:\nWriter schema CANNOT be mutually read with reader schema.\n\nReason(s):");
                    printIncompatibilities(result.getResult().getIncompatibilities());
                }
                break;
        }
    }

    private void printIncompatibilities(List<SchemaCompatibility.Incompatibility> incompatibilities) {
        for (SchemaCompatibility.Incompatibility incompatibility : incompatibilities) {
            System.out.println(String.format("- Type: %s\n  Location: %s\n  Writer: %s\n  Reader: %s", incompatibility.getType(), incompatibility.getLocation(), incompatibility.getWriterFragment(), incompatibility.getReaderFragment()));
        }
    }

    public static void main(String[] args) {
        Cli cli = new Cli();

        Options options = new Options();
        options.addOption(Option.builder("w").longOpt("writer").hasArg().desc("Writer schema for compatibility check").required().build());
        options.addOption(Option.builder("r").longOpt("reader").hasArg().desc("Reader schema for compatibility check").required().build());
        options.addOption(Option.builder("m").longOpt("mutual").desc("Check for mutual compatibility.\nDefault: Check that reader can read output produced by writer.").build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            String writer = line.getOptionValue("w");
            String reader = line.getOptionValue("r");

            String writerSchema = Files.asCharSource(new File(writer), Charsets.UTF_8).read();
            String readerSchema = Files.asCharSource(new File(reader), Charsets.UTF_8).read();

            if (line.hasOption("m")) {
                cli.checkCompatibility(writerSchema, readerSchema, Compatibility.CheckType.MUTUAL_READ);
            } else {
                cli.checkCompatibility(writerSchema, readerSchema, Compatibility.CheckType.CAN_BE_READ_BY);
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
