package messagepipeline.content;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

//TODO class name should be pull message encoder
public interface PullMessageReceiver {
	 boolean read(ByteBuffer	bis, boolean firstMessage, ByteArrayOutputStream baos);
	 String get(ByteArrayOutputStream baos) ;
}
