# Integration points

Two files in the Shield framework need to know about MIMIC. Both changes are small and local, and
this document describes them in prose so that the framework keeps living in one place.

## 1. `src/main/java/problem/StreamAnonymizationProblem.java`

- A `MIMIC` value is added to the `DatasetType` enum.
- The dataset dispatch selects `DatasetType.MIMIC` when the input path contains `mimic`.
- The results-similarity metric for that case is an `F1Score` with a 0.15 relative tolerance and an
  empty numeric attribute list, because the MIMIC query compares event sets rather than
  per-attribute values. Matching is done on timestamp and partitioning key.

## 2. `src/main/java/query/LiebreAnonymizationQuery.java`

- The MIMIC stream is loaded from the file named by the `MIMIC_GZ` environment variable, and the
  loader decompresses the gzip archive and reads the numeric columns.
- The scoring loop also runs `MainQueryMimic.process(...)` on the modified stream and reports the
  event count, so the console line shows both the generic counters and how many anomaly events the
  clinical query still produced.

## Notes

The `StreamAnonymizationProblem_2ObjectivesRes` and `StreamAnonymizationProblem_2ObjectivesPerf`
problem definitions did not need changes, since they reuse the dispatch from the base class.
