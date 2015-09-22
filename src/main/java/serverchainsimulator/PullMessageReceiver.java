package serverchainsimulator;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public interface PullMessageReceiver {
	 public boolean read(ByteBuffer	bis, boolean firstMessage, ByteArrayOutputStream baos);
	 public String get(ByteArrayOutputStream baos) ;
}
