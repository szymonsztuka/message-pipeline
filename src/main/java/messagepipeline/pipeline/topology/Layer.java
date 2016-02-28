package messagepipeline.pipeline.topology;

public interface Layer {
    boolean step(String stepName);
    void nodesStart();
     String getName();
}
