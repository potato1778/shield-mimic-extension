package problem;

import event.GenericEvent;
import mappers.QueryRepresentation;
import problem.utils.PrivacyMetricChoice;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.SequencedMap;
import java.util.zip.GZIPInputStream;

public class MimicQualitySmokeTest {

    public static void main(String[] args) throws Exception {
        String experiment = args.length > 0 ? args[0] : "filter-noise-001";
        String inputCsvPath = args.length > 1
                ? args[1]
                : createSampleCsv(
                        Path.of(System.getenv().getOrDefault(
                                "MIMIC_GZ", "data/mimic-numeric.csv.gz")),
                        Path.of("target", "mimic-quality-smoke-sample.csv"),
                        5000
                ).toString();
        System.out.println("[QUALITY] Input: " + inputCsvPath);
        System.out.println("[QUALITY] Experiment: " + experiment);

        QueryRepresentation.OperatorNode filter89Node = new QueryRepresentation.OperatorNode(
                QueryRepresentation.Operator.FILTER,
                new QueryRepresentation.FilterArgs(
                        "btbHR_bpm",
                        QueryRepresentation.Condition.GREATER_THAN,
                        89.0
                )
        );

        QueryRepresentation.OperatorNode filter100Node = new QueryRepresentation.OperatorNode(
                QueryRepresentation.Operator.FILTER,
                new QueryRepresentation.FilterArgs(
                        "btbHR_bpm",
                        QueryRepresentation.Condition.GREATER_THAN,
                        100.0
                )
        );

        QueryRepresentation.OperatorNode noise001Node = new QueryRepresentation.OperatorNode(
                QueryRepresentation.Operator.MAP_NOISE,
                new QueryRepresentation.MapNoiseArgs(
                        "btbHR_bpm",
                        0.01
                )
        );

        QueryRepresentation.OperatorNode noise005Node = new QueryRepresentation.OperatorNode(
                QueryRepresentation.Operator.MAP_NOISE,
                new QueryRepresentation.MapNoiseArgs(
                        "btbHR_bpm",
                        0.05
                )
        );

        QueryRepresentation pipeline = switch (experiment) {
            case "empty" -> new QueryRepresentation(List.of());
            case "filter-89" -> new QueryRepresentation(List.of(filter89Node));
            case "filter-100" -> new QueryRepresentation(List.of(filter100Node));
            case "noise-001" -> new QueryRepresentation(List.of(noise001Node));
            case "noise-005" -> new QueryRepresentation(List.of(noise005Node));
            case "filter-noise-001" -> new QueryRepresentation(List.of(filter89Node, noise001Node));
            case "filter-noise-005" -> new QueryRepresentation(List.of(filter89Node, noise005Node));
            default -> throw new IllegalArgumentException(
                    "Unknown experiment: " + experiment
                            + ". Use empty, filter-89, filter-100, noise-001, noise-005, filter-noise-001, or filter-noise-005."
            );
        };

        System.out.println("[QUALITY] Build problem");
        StreamAnonymizationProblem problem = new StreamAnonymizationProblem(
                inputCsvPath,
                "",
                PrivacyMetricChoice.K_ANONYMITY_CARDINALITY,
                true
        );

        System.out.println("[QUALITY] Evaluate pipeline");
        SequencedMap<String, Double> qualities = problem.qualityFunction().apply(pipeline);

        System.out.println("[QUALITY] Pipeline: " + pipeline);
        qualities.forEach((name, value) ->
                System.out.printf("[QUALITY] %s = %.6f%n", name, value));
        System.out.printf(
                "[QUALITY_SUMMARY] %s performance=%.6f privacy=%.6f results=%.6f%n",
                experiment,
                qualities.get("performance-similarity"),
                qualities.get("privacy"),
                qualities.get("results-similarity")
        );
        System.exit(0);
    }

    private static Path createSampleCsv(Path sourceGzip, Path targetCsv, int dataRows) throws IOException {
        Files.createDirectories(targetCsv.getParent());
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(Files.newInputStream(sourceGzip));
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8));
             BufferedWriter writer = Files.newBufferedWriter(targetCsv, StandardCharsets.UTF_8)) {

            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Input CSV file is empty: " + sourceGzip);
            }
            writer.write(header);
            writer.newLine();

            for (int i = 0; i < dataRows; i++) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                writer.write(line);
                writer.newLine();
            }
        }
        return targetCsv;
    }
}
