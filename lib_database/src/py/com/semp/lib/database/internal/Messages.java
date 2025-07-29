package py.com.semp.lib.database.internal;

import py.com.semp.lib.utilidades.messages.MessageKey;

/**
 * Messages for the utilities library.
 * 
 * @author Sergio Morel
 */
public enum Messages implements MessageKey
{
	DATABASE_NOT_CONFIGURED_ERROR,
	DATABASE_DRIVER_NOT_FOUND_ERROR,
	CONNECTING_TO_DATABASE,
	SET_AUTO_COMMIT_ERROR,
	SET_AUTO_COMMIT,
	DONT_INSTANTIATE,
	SHUTTING_DOWN,
	NOT_SUPPORTED,
	DRIVER_INFO;
	
	@Override
	public String getMessageKey()
	{
		return this.name();
	}
}