# Shield + MIMIC-IV: streaming anonymization for ICU vital signs

This repository holds the MIMIC-IV integration I built on top of the **Shield** framework during a
research project at Chalmers University of Technology.

The Shield framework itself is not part of this repository. It lives at
[vincenzo-gulisano/shield](https://github.com/vincenzo-gulisano/shield), and the streaming engine it
runs on is [vincenzo-gulisano/Liebre](https://github.com/vincenzo-gulisano/Liebre). The files here
are meant to be dropped into a checkout of that framework.

## The idea

Shield searches for **data modifiers**. A data modifier is a streaming query that turns an original
stream `S` into a modified stream `S'`, and the search looks for the best compromises between
privacy, results similarity and performance similarity. The search is a multi-objective evolutionary
algorithm driven by the JGEA framework, and the candidate pipelines are assembled from four operator
families, `filter`, `map_noise`, `map_duplicate` and `map_aggregate`.

My part extended that search to **live ICU vital-sign streams from MIMIC-IV**, so a pipeline has to
suppress identifying structure in physiological time series while keeping a downstream clinical
query intact.

## What I added

| File | Role |
|---|---|
| `src/main/java/query/MainQueryMimic.java` | The analysis query `Q` for the MIMIC stream. It flags tachycardia (`btbHR_bpm > 100` or `HR_bpm > 100`), hypoxemia (`SpO2_pct < 95`) and tachypnea (`RR_rpm > 20`) over a sliding window, and it records the performance profile that the similarity metrics compare against. |
| `src/main/java/grammar/GrammarGeneratorMimicMap.java` | Builds the search grammar for the MIMIC attribute set, with `timestamp`, `time` and `ID` kept out of the operator space. |
| `src/main/resources/grammars/mimic/mimic_generated-grammar-aggregate.bnf` | The generated grammar that defines the search space for the MIMIC stream. |
| `src/main/java/problem/MimicQualitySmokeTest.java` | A smoke test that evaluates a few hand-written pipelines, empty, filter only, noise only and filter plus noise, and prints the three quality values. It catches a broken setup long before a full evolutionary run finishes. |
| `experiment_mimic.txt` | The three-objective experiment definition for the MIMIC stream. |
| `experiment_mimic_smoke.txt` | A short run of the same configuration, used to check the setup end to end. |

`INTEGRATION.md` lists the two places where the framework itself needed to know about MIMIC.

## Data

No patient data is in this repository. MIMIC-IV is released by PhysioNet under a credentialed data
use agreement, so the recordings used in the experiments stay outside version control. The code
expects your own local copy, and you point it there with the `MIMIC_GZ` environment variable.

## Running it

1. Clone and build the streaming engine on the `rich_agg` branch, then install it into your local
   Maven repository.

        git clone -b rich_agg https://github.com/vincenzo-gulisano/Liebre.git
        cd Liebre && mvn clean install

2. Clone the Shield framework and copy the files from this repository into the matching paths.
3. Apply the two integration points described in `INTEGRATION.md`.
4. Build the framework, then run the smoke test before starting the full search.

        mvn clean install
        export MIMIC_GZ=/path/to/your/mimic-numeric.csv.gz
        java -jar target/Shield-1.0-SNAPSHOT-jar-with-dependencies.jar -v -nt 10 -f experiment_mimic.txt

Java 21 is required.

## Credit

The Shield framework and the Liebre streaming engine are the work of
[Vincenzo Gulisano](https://github.com/vincenzo-gulisano) and collaborators. Everything in this
repository is my own contribution to that project.

## Contact

Ruize Liu · ruizeliu.heu@gmail.com · [LinkedIn](https://www.linkedin.com/in/ruize-liu/)
