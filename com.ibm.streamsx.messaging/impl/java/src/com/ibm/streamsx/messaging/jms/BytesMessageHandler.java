/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.messaging.jms;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotReadableException;
import javax.jms.Session;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;

//This class handles the JMS Bytes message type 
class BytesMessageHandler extends JMSMessageHandlerImpl {

	// variable to set the codepage parameter, defaults to UTF-8
	private final String codepage;

	// constructor
	BytesMessageHandler(List<NativeSchema> nativeSchemaObjects, String cpage) {
		// call the base class constructor to initialize the native schema
		// attributes.
		super(nativeSchemaObjects);
		// set the codepage parameter if one is specified in the operator model,
		// if not a default value of UTF-8 is assumed
		codepage = cpage;
	}

	BytesMessageHandler(List<NativeSchema> nativeSchemaObjects, String cpage, Metric nTruncatedInserts) {
		super(nativeSchemaObjects, nTruncatedInserts);
		// set the codepage parameter if one is specified in the operator model,
		// if not a default value of UTF-8 is assumed
		codepage = cpage;
	}

	// For JMSSink operator, convert the incoming tuple to a JMS BytesMessage
	public Message convertTupleToMessage(Tuple tuple, Session session) throws JMSException,
			UnsupportedEncodingException {
		// create a new BytesMessage
		BytesMessage message;
		synchronized (session) {
			message = (BytesMessage) session.createBytesMessage();
		}

		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;

		for (NativeSchema currentObject : nativeSchemaObjects) {
			// Iterate through the native schema attributes
			// extract the name, type and length
			final String name = currentObject.getName();
			final NativeTypes type = currentObject.getType();
			final int length = currentObject.getLength();
			// handle based on the data-type
			switch (type) {

			case Bytes: {
				// variable to hold the bytes data
				byte[] bytedata = null;
				int size;
				MetaType metaType = tuple.getStreamSchema().getAttribute(name).getType().getMetaType();
				switch (metaType) {

				case DECIMAL32:
				case DECIMAL64:
				case DECIMAL128: {
					bytedata = tuple.getBigDecimal(name).toString().getBytes(codepage);
				}
					break;
				case TIMESTAMP: {

					bytedata = ((tuple.getTimestamp(name).getTimeAsSeconds()).toString()).getBytes(codepage);
				}
					break;

				case BLOB:
					Blob bl = tuple.getBlob(name);

					bytedata = new byte[(int) bl.getLength()];
					bl.getByteBuffer(0, (int) bl.getLength()).get(bytedata);

					break;

				case RSTRING:

					bytedata = ((RString) tuple.getObject(name)).getData();
					break;

				case USTRING:

					bytedata = tuple.getString(name).getBytes(codepage);
					break;
				}
				size = bytedata.length;

				// check for length in native schema
				// spl types decimal32, decimal64,decimal128, timestamp have a
				// default length of -4
				if (length == -2) {
					// truncate
					if (size > java.lang.Short.MAX_VALUE) {
						size = java.lang.Short.MAX_VALUE;
						isTruncated = true;
					}

					message.writeShort((short) size);
				} else if (length == -4) {
					message.writeInt(size);

				} else {
					// if the length of the bytedata is greater than the length
					// specified in native schema
					// set the isTruncated to true
					// truncate the bytedata
					if (size > length) {
						isTruncated = true;
						size = length;
					}
				}
				// Need to truncate or pad as required
				message.writeBytes(bytedata, 0, size);
				// variable required to pad the bytedata
				// for RSTRING and USTRING we pad with space and for BLOB we pad
				// with null

				byte topad = ' ';
				if (tuple.getStreamSchema().getAttribute(name).getType().getMetaType() == Type.MetaType.BLOB)
					topad = 0;
				if (length > 0) {

					for (int i = size; i < length; i++)
						message.writeByte(topad);
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
			default:
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

	// For JMSSource operator, convert the incoming JMS BytesMessage to tuple
	public MessageAction convertMessageToTuple(Message message, OutputTuple tuple) throws JMSException,
			UnsupportedEncodingException {
		if (!(message instanceof BytesMessage)) {
			// We got a wrong message type so throw an error
			return MessageAction.DISCARD_MESSAGE_WRONG_TYPE;
		}
		BytesMessage bytesMessage = (BytesMessage) message;
		boolean discard = false;
		// Iterate through the native schema attributes
		for (NativeSchema currentObject : nativeSchemaObjects) {
			// Added the try catch block to catch the MessageEOFException
			// This exception must be thrown when an unexpected end of stream
			// has been reached when a BytesMessage is being read.
			try {
				// extract the name, type and length
				final String name = currentObject.getName();
				final NativeTypes type = currentObject.getType();
				final int length = currentObject.getLength();
				// handle based on data-tye
				// extract the data from the message for each native schema
				// attrbute and set into the tuple
				switch (type) {

				case Byte:
					byte bt = bytesMessage.readByte();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setByte(name, bt);
					}
					break;
				case Short:
					short s = bytesMessage.readShort();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setShort(name, s);
					}
					break;
				case Int:
					int i = bytesMessage.readInt();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setInt(name, i);
					}
					break;
				case Long:
					long l = bytesMessage.readLong();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setLong(name, l);
					}
					break;
				case Float:
					float f = bytesMessage.readFloat();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setFloat(name, f);
					}
					break;
				case Double:
					double d = bytesMessage.readDouble();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setDouble(name, d);
					}
					break;
				case Boolean:
					boolean bl = bytesMessage.readBoolean();
					// we are interested in this currentObject only if it is
					// present in streams schema
					if (currentObject.getIsPresentInStreamSchema()) {
						tuple.setBoolean(name, bl);
					}
					break;
				case Bytes: {
					int size;
					// check for length in native schema
					// if -2 read a short from the message which will have the
					// length of the attribute
					if (length == -2) {
						size = (int) bytesMessage.readShort();
					} else if (length == -4) {
						// if -2 read an int from the message which will have
						// the length of the attribute
						size = bytesMessage.readInt();
					} else {
						size = length;
					}

					byte b[] = new byte[size];
					// read that many bytes from the message
					int lenRead = bytesMessage.readBytes(b, size);
					if (lenRead < size) {
						// When the lenRead is less than size.
						// Discard message saying message too
						// short

						discard = true;
					} else {
						// we are interested in this currentObject only if it is
						// present in streams schema
						if (currentObject.getIsPresentInStreamSchema()) {
							switch (tuple.getStreamSchema().getAttribute(name).getType().getMetaType()) {

							case DECIMAL32:
							case DECIMAL64:
							case DECIMAL128: {

								String stringdata = new String(b, 0, lenRead, codepage);
								BigDecimal bigDecValue = new BigDecimal(stringdata);
								tuple.setBigDecimal(name, bigDecValue);

							}
								break;
							case TIMESTAMP: {
								String timeseriesData = new String(b, 0, lenRead, codepage);
								BigDecimal bigDecValue = new BigDecimal(timeseriesData);
								tuple.setTimestamp(name, Timestamp.getTimestamp(bigDecValue));

							}
								break;
							case RSTRING: {

								RString rstringData = new RString(b);
								tuple.setObject(name, rstringData);
							}
								break;
							case USTRING: {
								String rstringData = new String(b, codepage);
								tuple.setString(name, rstringData);
							}
								break;
							}

						}
					}
				}
					break;

				}

				if (discard == true) {
					// The message is too short
					return MessageAction.DISCARD_MESSAGE_EOF_REACHED;
				}
			} catch (MessageEOFException meofEx) {
				return MessageAction.DISCARD_MESSAGE_EOF_REACHED;
			}

			catch (MessageNotReadableException mnrEx) {
				return MessageAction.DISCARD_MESSAGE_UNREADABLE;
			}

		}
		// the message was read successfully
		return MessageAction.SUCCESSFUL_MESSAGE;
	}
}
