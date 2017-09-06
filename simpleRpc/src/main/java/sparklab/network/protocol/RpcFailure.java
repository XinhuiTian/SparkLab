package sparklab.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public final class RpcFailure extends AbstractMessage implements ResponseMessage {
	public final long requestId;
	public final String errorString;

	public RpcFailure(long requestId, String errorString) {
		this.requestId = requestId;
		this.errorString = errorString;
	}

	@Override
	public Type type() { return Type.RpcFailure; }

	@Override
	public int encodedLength() {
		return 8 + Encoders.Strings.encodedLength(errorString);
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(requestId);
		Encoders.Strings.encode(buf, errorString);
	}

	public static RpcFailure decode(ByteBuf buf) {
		long requestId = buf.readLong();
		String errorString = Encoders.Strings.decode(buf);
		return new RpcFailure(requestId, errorString);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(requestId, errorString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof RpcFailure) {
			RpcFailure o = (RpcFailure) other;
			return requestId == o.requestId && errorString.equals(o.errorString);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
					  .add("requestId", requestId)
					  .add("errorString", errorString)
					  .toString();
	}
}
