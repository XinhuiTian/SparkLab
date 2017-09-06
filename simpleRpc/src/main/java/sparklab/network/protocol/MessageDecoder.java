package sparklab.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
	@Override
	public void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
		Message.Type msgType = Message.Type.decode(msg);
		Message decoded = decode(msgType, msg);
		// OneWayMessage decoded = OneWayMessage.decode(msg);
		out.add(decoded);
	}

	private Message decode(Message.Type msgType, ByteBuf in) {
		switch (msgType) {
			case RpcRequest:
				return RpcRequest.decode(in);

			case RpcResponse:
				return RpcResponse.decode(in);

			case RpcFailure:
				return RpcFailure.decode(in);

			case OneWayMessage:
				return OneWayMessage.decode(in);

			default:
				throw new IllegalArgumentException("Unexpected message type: " + msgType);
		}
	}
}
