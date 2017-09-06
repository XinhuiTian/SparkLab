package sparklab.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class MessageEncoder extends MessageToMessageEncoder<Message> {
	@Override
	public void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) {
		long bodyLength = 0;
		ByteBuf body = null;
		if (msg.body() != null) try {
			System.out.println("Non-empty message");
			bodyLength = msg.body().size();
			body = (ByteBuf) msg.body().convertToNetty();
		} catch (Exception e){

		}

		Message.Type msgType = msg.type();

		/*
		int headerLength = 8 + msgType.encodedLength() + msg.encodedLength();
		ByteBuf header = ctx.alloc().heapBuffer(headerLength);
		msgType.encode(header);
		msg.encode(header);
		*/

		long msgLength = msgType.encodedLength() + bodyLength;
		ByteBuf bytes = ctx.alloc().heapBuffer((int) msgLength);
		msgType.encode(bytes);
		msg.encode(bytes);

		if (body != null) {
			bytes.writeBytes(body);

		} else {
			System.out.println("body is null");
			// out.add(header);
		}
		out.add(bytes);
	}
}
