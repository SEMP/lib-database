package py.com.semp.lib.database.internal;

import java.util.Locale;
import java.util.ResourceBundle;

import py.com.semp.lib.database.configuration.Values;
import py.com.semp.lib.utilidades.messages.MessageKey;
import py.com.semp.lib.utilidades.messages.MessageManager;
import py.com.semp.lib.utilidades.messages.MessageRetriever;

/**
 * A utility class providing centralized access to message retrieval within the library.
 * It is designed to work internally and should not be accessed outside the library.
 * The class ensures consistent use of the predefined {@link MessageManager} instance
 * for all message retrieval operations.
 * 
 * @author Sergio Morel
 */
public final class MessageUtil
{
	/**
	 * The resource bundle base name.
	 */
	private static final String RESOURCE = Values.Resources.MESSAGES_BASE_NAME;
	
	/**
	 * The path pointing to the location of the resource bundles.
	 */
	private static final String PATH = Values.Constants.MESSAGES_PATH;
	
	/**
	 * Shared instance of {@link MessageRetriever} for message retrieval.
	 */
	private static final MessageRetriever MESSAGE_RETRIEVER = new MessageRetriever()
	{
		@Override
		protected MessageManager getNewMessageManager(Locale locale)
		{
			ResourceBundle resourceBundle = ResourceBundle.getBundle(PATH + RESOURCE, locale);
			
			return new MessageManager(resourceBundle);
		}
	};
	
	private MessageUtil()
	{
		super();
		
		String errorMessage = MessageUtil.getMessage(Messages.DONT_INSTANTIATE, this.getClass().getName());
		
		throw new AssertionError(errorMessage);
	}
	
	/**
	 * Retrieves a message corresponding to the provided key from the shared resource bundle.
	 * 
	 * @param messageKey
	 * - The key of the desired message.
	 * @param arguments
	 * - Arguments to build the string.
	 * @return
	 * - The message string associated with the key.
	 */
	public static String getMessage(String messageKey, Object... arguments)
	{
		return MESSAGE_RETRIEVER.getMessage(messageKey, arguments);
	}
	
	/**
	 * Retrieves a message corresponding to the provided {@link MessageKey} from the shared resource bundle.
	 * 
	 * @param messageKey
	 * - The MessageKey enum representation of the desired message key.
	 * @param arguments
	 * - Arguments to build the string.
	 * @return
	 * - The message string associated with the key.
	 */
	public static String getMessage(MessageKey messageKey, Object... arguments)
	{
		return MESSAGE_RETRIEVER.getMessage(messageKey, arguments);
	}
	
	/**
	 * Sets the locale for retrieving messages, allowing for a change in the language of the returned messages.
	 * 
	 * @param locale
	 * - the new locale to set for retrieving messages.
	 */
	public static void setLocale(Locale locale)
	{
		MESSAGE_RETRIEVER.setLocale(locale);
	}
}