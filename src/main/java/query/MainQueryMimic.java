package query;

import common.util.Util;
import component.operator.Operator;
import component.operator.in1.map.MapFunction;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import event.GenericEvent;
import metrics.performance.utils.StreamStatsWindow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MainQueryMimic {

    // Q: detect any vital sign anomaly
    // Tachycardia: btbHR > 100 OR HR > 100
    // Hypoxemia:   SpO2 < 95
    // Tachypnea:   RR > 20
    private static final double TACHYCARDIA_THRESHOLD = 100.0;
    private static final double HYPOXEMIA_THRESHOLD   = 95.0;
    private static final double TACHYPNEA_THRESHOLD   = 20.0;

    public record QueryResult(List<GenericEvent> events, StreamStatsWindow statsWindow) implements MainQueryResult {}

    public static QueryResult process(List<GenericEvent> inputStream, String queryId, long minTs, long maxTs) {
        final long resolution = 60000L;

        StreamStatsWindow statsWindow = new StreamStatsWindow(
                Set.of("sourceStream", "outputStream"),
                minTs, maxTs, resolution);

        final List<GenericEvent> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        Query query = new Query();

        SourceFunction<GenericEvent> collectionSource = createCollectionSource(inputStream);
        Source<GenericEvent> inputSource = query.addBaseSource("mimic_I1_" + queryId, collectionSource);

        Operator<GenericEvent, GenericEvent> highHeartRateFilter = query.addFilterOperator(
                "mimic_anomaly_" + queryId,
                event -> {
                    double btbHR = event.getAttribute("btbHR_bpm");
                    double hr    = event.getAttribute("HR_bpm");
                    double spo2  = event.getAttribute("SpO2_pct");
                    double rr    = event.getAttribute("RR_rpm");
                    // retain event if ANY vital sign is anomalous
                    return (!Double.isNaN(btbHR) && btbHR > TACHYCARDIA_THRESHOLD)
                        || (!Double.isNaN(hr)    && hr    > TACHYCARDIA_THRESHOLD)
                        || (!Double.isNaN(spo2)  && spo2  < HYPOXEMIA_THRESHOLD)
                        || (!Double.isNaN(rr)    && rr    > TACHYPNEA_THRESHOLD);
                });

        class PerformanceRecorder implements MapFunction<GenericEvent, GenericEvent> {
            private final HashSet<String> keysSet = new HashSet<>();
            private final String streamId;
            private final StreamStatsWindow statsWindowLocal;
            private long currentBucketIndex = -1L;

            public PerformanceRecorder(String streamId, StreamStatsWindow statsWindowLocal) {
                this.streamId = streamId;
                this.statsWindowLocal = statsWindowLocal;
            }

            @Override
            public GenericEvent apply(GenericEvent event) {
                if (event != null) {
                    long bucketIndex = (event.getTimestamp() - statsWindowLocal.minTimestamp())
                            / statsWindowLocal.getResolutionMillis();
                    if (currentBucketIndex != -1 && currentBucketIndex != bucketIndex) {
                        keysSet.clear();
                    }
                    currentBucketIndex = bucketIndex;

                    long alignedTs = statsWindowLocal.minTimestamp()
                            + bucketIndex * statsWindowLocal.getResolutionMillis();
                    if (alignedTs < statsWindowLocal.minTimestamp()) {
                        alignedTs = statsWindowLocal.minTimestamp();
                    }
                    if (alignedTs > statsWindowLocal.maxTimestamp()) {
                        alignedTs = statsWindowLocal.maxTimestamp();
                    }

                    if (!keysSet.contains(event.getKey())
                            && event.getEventType() != GenericEvent.EventType.EMPTY_WINDOW) {
                        keysSet.add(event.getKey());
                        statsWindowLocal.addKeys(streamId, alignedTs, 1);
                    }
                    statsWindowLocal.addTuples(streamId, alignedTs, 1);
                }
                return event;
            }
        }

        Operator<GenericEvent, GenericEvent> recorderAfterSource = query.addMapOperator(
                "mimic_rec_as_" + queryId,
                new PerformanceRecorder("sourceStream", statsWindow));
        Operator<GenericEvent, GenericEvent> recorderAfterFilter = query.addMapOperator(
                "mimic_rec_af_" + queryId,
                new PerformanceRecorder("outputStream", statsWindow));

        Sink<GenericEvent> sink = query.addBaseSink("mimic_o1_" + queryId, event -> {
            if (event != null && event.getEventType() != GenericEvent.EventType.EMPTY_WINDOW) {
                collectedEvents.add(event);
            }
        });

        query.connect(inputSource, recorderAfterSource)
                .connect(recorderAfterSource, highHeartRateFilter)
                .connect(highHeartRateFilter, recorderAfterFilter)
                .connect(recorderAfterFilter, sink);

        query.activate();

        while (sink.isEnabled()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        query.deActivate();

        return new QueryResult(collectedEvents, statsWindow);
    }

    private static <T> SourceFunction<T> createCollectionSource(final List<T> list) {
        return new SourceFunction<>() {
            private int currentIndex = 0;
            private boolean isFinished = false;
            private static final long IDLE_SLEEP = 10;
            private boolean enabled;

            @Override
            public T get() {
                if (isFinished) {
                    Util.sleep(IDLE_SLEEP);
                    return null;
                }
                if (currentIndex < list.size()) {
                    T item = list.get(currentIndex);
                    currentIndex++;
                    return item;
                }
                isFinished = true;
                return null;
            }

            @Override public boolean isInputFinished() { return isFinished; }
            @Override public void enable() { this.enabled = true; }
            @Override public boolean isEnabled() { return enabled; }
            @Override public void disable() { this.enabled = false; }
            @Override public boolean canRun() { return !isFinished; }
        };
    }
}
