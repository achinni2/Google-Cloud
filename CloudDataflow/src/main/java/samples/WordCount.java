package Samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class WordCount {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        String input = "C:\\Users\\axc353\\Documents\\test.txt";
        String outputPrefix = "C:\\Users\\axc353\\Documents\\output";

        final String searchTerm = "Pipeline";

        p.apply("GetJava", TextIO.read().from(input)) //
            .apply("Grep", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    String line = c.element();
                    if (line.contains(searchTerm)) {
                        c.output(line);
                    }
                }
            }))
            .apply(TextIO.write().to(outputPrefix).withSuffix(".txt").withoutSharding());

        p.run();
    }
}
