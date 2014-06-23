/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.math.BigDecimal;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.Session;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.Timestamp;

//This class handles the JMS Map message type 
class MapMessageHandler extends JMSMessageHandlerImpl {
	// constructor
	public MapMessageHandler(List<NativeSchema> nativeSchemaObjects) {
		// call the base class constructor to initialize the native schema
		// attributes.
		super(nativeSchemaObjects);
	}

	// constructor
	public MapMessageHandler(List<NativeSchema> nativeSchemaObjects,
			Metric nTruncatedInserts) {
		super(nativeSchemaObjects, nTruncatedInserts);
	}

	// For JMSSink operator, convert the incoming tuple to a JMS MapMessage
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException {
		// create a new mapMessage
		MapMessage message;
		synchronized (session) {
			message = (MapMessage) session.createMapMessage();
		}
		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;

		// get the attributes from the native schema
		for (NativeSchema currentObject : nativeSchemaObjects) {
			// iterate through the native schema elements
			// extract the name, type and length
			final String name = currentObject.getName();
			final NativeTypes type = currentObject.getType();
			final int length = currentObject.getLength();
			// handle based on the data-type
			switch (type) {

			// For all cases, IllegalArgumentException and NPE(for setBytes) is
			// not caught since name is always verified and is not null or not
			// empty string.
			case Bytes: {
				// extract the blob from the tuple
				// get its size
				Blob bl = tuple.getBlob(name);
				long size = bl.getLength();

				// check for length in native schema
				// if the length of the blob is greater than the length
				// specified in native schema
				// set the isTruncated to true
				// truncate the blob
				if (size > length && length != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {
					isTruncated = true;
					size = length;
				}
				byte[] blobdata = new byte[(int) size];
				bl.getByteBuffer(0, (int) size).get(blobdata);
				// Since name is always verified and never null or empty string,
				// we dont need to catch NPE

				// set the bytes into the messaage
				message.setBytes(name, blobdata);
			}
				break;
			case String: {
				switch (tuple.getStreamSchema().getAttribute(name).getType()
						.getMetaType()) {
				case RSTRING:
				case USTRING:
					// extract the String
					// get its length
					String rdata = tuple.getString(name);
					int size = rdata.length();
					// If no length was specified in native schema or
					// if the length of the String rdata is less than the length
					// specified in native schema
					if (length == LENGTH_ABSENT_IN_NATIVE_SCHEMA
							|| size <= length) {
						message.setString(name, rdata);
					}
					// if the length of rdate is greater than the length
					// specified in native schema
					// set the isTruncated to true
					// truncate the String
					else if (size > length) {
						isTruncated = true;
						String stringdata = rdata.substring(0, length);
						message.setString(name, stringdata);
					}

					break;
				// spl types decimal32, decimal64,decimal128, timestamp are
				// mapped to String.
				case DECIMAL32:
				case DECIMAL64:
				case DECIMAL128:
					message.setString(name, tuple.getBigDecimal(name)
							.toString());
					break;
				case TIMESTAMP:
					message.setString(name, (tuple.getTimestamp(name)
							.getTimeAsSeconds()).toString());
					break;
				}
			}
				break;
			case Byte:
				message.setByte(name, tuple.getByte(name));
				break;
			case Short:
				message.setShort(name, tuple.getShort(name));
				break;
			case Int:
				message.setInt(name, tuple.getInt(name));
				break;
			case Long:
				message.setLong(name, tuple.getLong(name));
				break;
			case Float:
				message.setFloat(name, tuple.getFloat(name));
				break;
			case Double:
				message.setDouble(name, tuple.getDouble(name));
				break;
			case Boolean:
				message.setBoolean(name, tuple.getBoolean(name));
				break;
			}
		}
		// if the isTruncated boolean is set, increment the metric
		// nTruncatedInserts
		if (isTruncated) {
			nTruncatedInserts.incrementValue(1);
		}
		// return the message
		return message;
	}

	// For JMSSource operator, convert the incoming JMS MapMessage to tuple
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException {
		if (!(message instanceof MapMessage)) {
			// We got a wrong message type so throw an error
			return MessageAction.DISCARD_MESSAGE_WRONG_TYPE;
		} else {
			MapMessage mapMessage = (MapMessage) message;
			// Iterate through the native schema attributes
			for (NativeSchema currentObject : nativeSchemaObjects) {

				try {
					// extract the name, type and length
					final String name = currentObject.getName();
					final NativeTypes type = currentObject.getType();
					// handle based on data-tye
					// extract the data from the message for each native schema
					// attrbute and set into the tuple
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema())
						switch (type) {
						case Byte:
							tuple.setByte(name, mapMessage.getByte(name));
							break;
						case Short:
							tuple.setShort(name, mapMessage.getShort(name));
							break;
						case Int:
							tuple.setInt(name, mapMessage.getInt(name));
							break;
						case Long:
							tuple.setLong(name, mapMessage.getLong(name));
							break;
						case Float:
							tuple.setFloat(name, mapMessage.getFloat(name));
							break;
						case Double:
							tuple.setDouble(name, mapMessage.getDouble(name));
							break;
						case Boolean:
							tuple.setBoolean(name, mapMessage.getBoolean(name));
							break;
						case String:
							switch (tuple.getStreamSchema().getAttribute(name)
									.getType().getMetaType()) {
							case RSTRING:
							case USTRING:
								tuple.setString(name,
										mapMessage.getString(name));
								break;
							case DECIMAL32:
							case DECIMAL64:
							case DECIMAL128: {

								BigDecimal bigDecValue = new BigDecimal(
										mapMessage.getString(name));
								tuple.setBigDecimal(name, bigDecValue);
							}
								break;
							case TIMESTAMP: {
								BigDecimal bigDecValue = new BigDecimal(
										mapMessage.getString(name));
								tuple.setTimestamp(name,
										Timestamp.getTimestamp(bigDecValue));
							}
								break;
							}
							break;

						}
				} catch (MessageFormatException mfEx) {
					// if MessageFormatException is thrown
					return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
				}

			}
			// Messsage was successfully read
			return MessageAction.SUCCESSFUL_MESSAGE;
		}
	}
}
