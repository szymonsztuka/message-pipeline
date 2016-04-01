package radar.experimental;

import radar.message.PullMessageReceiver;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class SimplePullMessageReceiver implements PullMessageReceiver {
    public static byte STX = (byte)0x02;
    public static  byte ETX = (byte)0x03;
    public static  byte US = (byte)0x30;
    int msgLength = 0;
    int lengthConsumed = 0;
    public byte[] msg ;

    @Override
    public boolean read(ByteBuffer bis, boolean firstMessage, ByteArrayOutputStream baos) {
        if(firstMessage) {
            msgLength = 0;
            lengthConsumed = 0;
            bis.get();//first byte is STX
            byte x ;
            int i = 0;
            while( (x=bis.get() )!= US && i < bis.limit()) {
                int shift = (4 - 1 - i) * 8;
                msgLength += (x & 0x000000FF) << shift;
                i++;
            }
            msg = new byte[msgLength];
            if (msgLength <= (bis.limit() - bis.position())) {
                bis.get(msg, 0, msgLength);
                lengthConsumed = msgLength;
                return false;
            } else {
                lengthConsumed = bis.limit() - bis.position() - 1; //skip  byte is ETX
                bis.get(msg, 0, lengthConsumed);
                return true;
            }
        }else{
            if(msgLength- lengthConsumed <=bis.limit() - bis.position() ){
                bis.get(msg, lengthConsumed, msgLength- lengthConsumed);
                lengthConsumed = msgLength;
                return false;
            } else {
                bis.get(msg, lengthConsumed, bis.limit() - bis.position());
                lengthConsumed = bis.limit() - bis.position();
                return true;
            }
        }
    }

    @Override
    public String get(ByteArrayOutputStream baos) {
        return msg.toString();
    }

    public static void main(String [] args) {
        /*ByteBuffer bis = ByteBuffer.allocate(20);
        bis.put(STX);
        bis.putInt(1);
        bis.put(US);
        bis.put((byte)0x41);
        bis.put(ETX);
        bis.flip();
        for(int i=0; i < bis.limit(); i++) {
            System.out.print(bis.get());
        }
        SimplePullMessageReceiver rec = new SimplePullMessageReceiver();
        bis.flip();
        rec.read(bis, true, null);
        System.out.println();
        for(int i=0; i < rec.msg.length; i++) {
            System.out.print(rec.msg[i]);
        }*/


        ByteBuffer bis = ByteBuffer.allocate(20);
        bis.put(STX);
        bis.putInt(4);
        bis.put(US);
        bis.put((byte)0x41);
        bis.put((byte)0x4F);
        bis.put(ETX);
        bis.flip();
        for(int i=0; i < bis.limit(); i++) {
            System.out.print(bis.get());
        }
        SimplePullMessageReceiver rec = new SimplePullMessageReceiver();
        bis.flip();
        rec.read(bis, true, null);
        System.out.println();
        for(int i=0; i < rec.msg.length; i++) {
            System.out.print(rec.msg[i]);
        }
        bis.flip();
        bis.put((byte) 0x42);
        bis.put((byte)0x43);
        bis.flip();
        rec.read(bis, false, null);
        System.out.println();
        for(int i=0; i < rec.msg.length; i++) {
            System.out.print(rec.msg[i]);
        }
    }
}
