package messagepipeline.message;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public interface PullMessageReceiver {
	 boolean read(ByteBuffer	bis, boolean firstMessage, ByteArrayOutputStream baos);
	 String get(ByteArrayOutputStream baos) ;
}
