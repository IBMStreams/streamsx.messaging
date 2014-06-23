/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.math.BigDecimal;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.Session;

import javax.jms.StreamMessage;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.Timestamp;

//This class handles the JMS Stream message type 
class StreamMessageHandler extends JMSMessageHandlerImpl {
	
	// constructor
	public StreamMessageHandler(List<NativeSchema> nativeSchemaObjects) {
		super(nativeSchemaObjects);
	}

	// constructor
	public StreamMessageHandler(List<NativeSchema> nativeSchemaObjects,
			Metric nTruncatedInserts) {
		super(nativeSchemaObjects, nTruncatedInserts);
	}

	// For JMSSink operator, convert the incoming tuple to a JMS StreamMessage
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException {
		// create a new StreamMessage
		StreamMessage message;
		synchronized (session) {
			message = session.createStreamMessage();
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
				// set the bytes in the message
				byte[] blobdata = new byte[(int) size];
				bl.getByteBuffer(0, (int) size).get(blobdata);
				message.writeBytes(blobdata);
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

						message.writeString(rdata);
					}
					// if the length of rdate is greater than the length
					// specified in native schema
					// set the isTruncated to true
					// truncate the String
					else if (size > length) {
						isTruncated = true;
						String stringdata = rdata.substring(0, length);
						message.writeString(stringdata);
					}
					break;
				// spl types decimal32, decimal64,decimal128, timestamp are
				// mapped to String.
				case DECIMAL32:
				case DECIMAL64:
				case DECIMAL128:
					message.writeString(tuple.getBigDecimal(name).toString());
					break;
				case TIMESTAMP:
					message.writeString((tuple.getTimestamp(name)
							.getTimeAsSeconds()).toString());
					break;
				}
			}
				break;

			case Byte:
				message.writeByte(tuple.getByte(name));
				break;

			case Short:
				message.writeShort(tuple.getShort(name));
				break;

			case Int:
				message.writeInt(tuple.getInt(name));
				break;

			case Long:
				message.writeLong(tuple.getLong(name));
				break;

			case Float:
				message.writeFloat(tuple.getFloat(name));
				break;
			case Double:
				message.writeDouble(tuple.getDouble(name));
				break;
			case Boolean:
				message.writeBoolean(tuple.getBoolean(name));
				break;
			}
		}
		// if the isTruncated boolean is set, increment the metric
		// nTruncatedInserts
		if (isTruncated) {
			nTruncatedInserts.incrementValue(1);
		}
		return message;
	}

	// For JMSSource operator, convert the incoming JMS MapMessage to tuple
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException {
		// We got a wrong message type so throw an error
		if (!(message instanceof StreamMessage)) {
			return MessageAction.DISCARD_MESSAGE_WRONG_TYPE;
		}
		StreamMessage streamMessage = (StreamMessage) message;
		// Iterate through the native schema attributes
		for (NativeSchema currentObject : nativeSchemaObjects) {
			// Added the try catch block to catch the MessageEOFException
			// This exception must be thrown when an unexpected end of stream
			// has been reached when a StreamMessage is being read.
			try {
				// extract the name and type
				final String name = currentObject.getName();
				final NativeTypes type = currentObject.getType();
				// handle based on data-tye
				// extract the data from the message for each native schema
				// attrbute and set into the tuple
				switch (type) {
				case Byte:
					byte byteData = streamMessage.readByte();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setByte(name, byteData);
					}
					break;
				case Short:

					short shortData = streamMessage.readShort();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setShort(name, shortData);
					}
					break;
				case Int:

					int intData = streamMessage.readInt();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setInt(name, intData);
					}
					break;
				case Long:

					long longData = streamMessage.readLong();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setLong(name, longData);
					}
					break;
				case Float:

					float floatData = streamMessage.readFloat();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setFloat(name, floatData);
					}
					break;
				case Double:

					double doubleData = streamMessage.readDouble();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setDouble(name, doubleData);
					}
					break;
				case Boolean:

					boolean booleanData = streamMessage.readBoolean();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setBoolean(name, booleanData);
					}
					break;
				case String:

					switch (tuple.getStreamSchema().getAttribute(name)
							.getType().getMetaType()) {
					case RSTRING:
					case USTRING:
						String stringData = streamMessage.readString();
						// we are interested in this currentObject only if it is
						// present in streams schema
						if (currentObject.getIsPresentInStreamSchema()) {
							tuple.setString(name, stringData);
						}
						break;
					case DECIMAL32:
					case DECIMAL64:
					case DECIMAL128: {
						BigDecimal bigDecValue = new BigDecimal(
								streamMessage.readString());
						// we are interested in this currentObject only if it is
						// present in streams schema
						if (currentObject.getIsPresentInStreamSchema()) {
							tuple.setBigDecimal(name, bigDecValue);
						}
					}
						break;
					case TIMESTAMP: {
						BigDecimal bigDecValue = new BigDecimal(
								streamMessage.readString());
						// we are interested in this currentObject only if it is
						// present in streams schema
						if (currentObject.getIsPresentInStreamSchema()) {
							tuple.setTimestamp(name,
									Timestamp.getTimestamp(bigDecValue));
						}
					}
						break;
					}

					break;
				}
			} catch (MessageEOFException meofEx) {
				return MessageAction.DISCARD_MESSAGE_EOF_REACHED;
			} catch (MessageNotReadableException mnrEx) {
				return MessageAction.DISCARD_MESSAGE_UNREADABLE;
			} catch (MessageFormatException mfEx) {
				return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
			}

		}// end for
			// Messsage was successfully read
		return MessageAction.SUCCESSFUL_MESSAGE;

	}
}
