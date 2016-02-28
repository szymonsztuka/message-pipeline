package messagepipeline.pipeline.node;

public interface Node {
    void signalBatchEnd();
    String getName();
}
