package Samples;

import java.util.Arrays;
import java.util.List;
import options.SampleOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class JoinDatasetsWithNativeSdkCode {

    public static void main(String[] args) {
        SampleOptions options = TestPipeline.testingPipelineOptions().as(SampleOptions.class);
        options.setOutput("C:\\Users\\axc353\\Documents\\Git\\indian-premier-league-csv-dataset\\Joins");
        Pipeline pipeline = Pipeline.create(options);

        final List<KV<String, String>> emailsList =
            Arrays.asList(
                KV.of("amy", "amy@example.com"),
                KV.of("carl", "carl@example.com"),
                KV.of("julia", "julia@example.com"),
                KV.of("carl", "carl@email.com"));

        final List<KV<String, String>> phonesList =
            Arrays.asList(
                KV.of("amy", "111-222-3333"),
                KV.of("james", "222-333-4444"),
                KV.of("amy", "333-444-5555"),
                KV.of("carl", "444-555-6666"));

        PCollection<KV<String, String>> emails = pipeline.apply("CreateEmails", Create.of(emailsList));
        PCollection<KV<String, String>> phones = pipeline.apply("CreatePhones", Create.of(phonesList));

        final TupleTag<String> emailsTag = new TupleTag<>();
        final TupleTag<String> phonesTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> results =
            KeyedPCollectionTuple.of(emailsTag, emails)
                .and(phonesTag, phones)
                .apply(CoGroupByKey.create());

        results.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        String name = e.getKey();
                        Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                        Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                        StringBuilder sb = new StringBuilder();
                        sb.append(name + "-");
                        emailsIter.forEach(email -> sb.append(email + ","));
                        phonesIter.forEach(phone -> sb.append(phone + ","));
                        c.output(sb.append('\n').toString());
                    }
                }))
            .apply(TextIO.write().to(options.getOutput()).withSuffix(".txt").withoutSharding());
        pipeline.run().waitUntilFinish();
    }
}

