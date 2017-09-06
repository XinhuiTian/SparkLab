package sparklab.network.protocol;

import sparklab.network.buffer.ManagedBuffer;

public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage {

	protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
		super(body, isBodyInFrame);
	}

	public abstract ResponseMessage createFailureResponse(String error);
}
