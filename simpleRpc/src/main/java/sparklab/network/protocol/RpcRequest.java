package sparklab.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import sparklab.network.buffer.ManagedBuffer;
import sparklab.network.buffer.NettyManagedBuffer;

public final class RpcRequest extends AbstractMessage implements RequestMessage {
	public final long requestId;

	public RpcRequest(long requestId, ManagedBuffer message) {
		super(message, true);
		this.requestId = requestId;
	}

	@Override
	public Type type() { return Type.RpcRequest; }

	@Override
	public int hashCode() {
		return Objects.hashCode(requestId, body());
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof RpcRequest) {
			RpcRequest o = (RpcRequest) other;
			return requestId == o.requestId && super.equals(o);
		}
		return false;
	}


	@Override
	public int encodedLength() {
		return 8 + 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(requestId);
		// See comment in encodedLength().
		buf.writeInt((int) body().size());
	}

	public static RpcRequest decode(ByteBuf buf) {
		long requestId = buf.readLong();
		// See comment in encodedLength().
		buf.readInt();
		return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
	}
}
