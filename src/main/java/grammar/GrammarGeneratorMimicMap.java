package grammar;

import grammar.utils.CSVAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GrammarGeneratorMimicMap {

    private static final Logger logger = LoggerFactory.getLogger(GrammarGeneratorMimicMap.class);

    public static void main(String[] args) throws IOException {
        final String csvPath = args.length > 0
                ? args[0]
                : "target/mimic-quality-smoke-sample.csv";
        final String grammarPath = args.length > 1
                ? args[1]
                : "src/main/resources/grammars/mimic/mimic_generated-grammar-map.bnf";
        final String keyColumn = "";

        List<String> excludedColumns = new ArrayList<>(List.of("timestamp", "time", "ID"));
        if (keyColumn != null && !keyColumn.isEmpty()) {
            excludedColumns.add(keyColumn);
        }

        List<String> attributes = CSVAnalyzer.extractAttributes(csvPath, excludedColumns);
        File parentDirectory = new File(grammarPath).getParentFile();
        if (parentDirectory != null && !parentDirectory.exists() && !parentDirectory.mkdirs()) {
            throw new IOException("Could not create grammar directory: " + parentDirectory);
        }

        GrammarGeneratorMap.generateGrammar(attributes, grammarPath);
        logger.info("MIMIC map grammar generated from {} with {} attributes", csvPath, attributes.size());
    }
}
