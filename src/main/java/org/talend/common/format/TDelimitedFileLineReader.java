package org.talend.common.format;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class TDelimitedFileLineReader {
	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;
	// The line delimiter
	private final byte[] recordDelimiterBytes;

	public TDelimitedFileLineReader(InputStream in, int bufferSize,
			byte[] recordDelimiterBytes) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
		this.recordDelimiterBytes = recordDelimiterBytes;
	}

	public void close() throws IOException {
		in.close();
	}

	public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
			throws IOException {
		str.clear();
		int txtLength = 0; // tracks str.getLength(), as an optimization
		long bytesConsumed = 0;
		int delPosn = 0;
		int ambiguousByteCount = 0; // To capture the ambiguous
									// characters count
		do {
			int startPosn = bufferPosn; // Start from previous end
										// position
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				bufferLength = in.read(buffer);
				if (bufferLength <= 0) {
					str.append(recordDelimiterBytes, 0, ambiguousByteCount);
					break; // EOF
				}
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) {
				if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
					delPosn++;
					if (delPosn >= recordDelimiterBytes.length) {
						bufferPosn++;
						break;
					}
				} else if (delPosn != 0) {
					bufferPosn--;
					delPosn = 0;
				}
			}
			int readLength = bufferPosn - startPosn;
			bytesConsumed += readLength;
			int appendLength = readLength - delPosn;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				if (ambiguousByteCount > 0) {
					str.append(recordDelimiterBytes, 0, ambiguousByteCount);
					bytesConsumed += ambiguousByteCount;
					ambiguousByteCount = 0;
				}
				str.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
			if (bufferPosn >= bufferLength) {
				if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
					ambiguousByteCount = delPosn;
					bytesConsumed -= ambiguousByteCount; // to be
															// consumed
															// in next
				}
			}
		} while (delPosn < recordDelimiterBytes.length
				&& bytesConsumed < maxBytesToConsume);
		if (bytesConsumed > (long) Integer.MAX_VALUE) {
			throw new IOException("Too many bytes before delimiter: "
					+ bytesConsumed);
		}
		return (int) bytesConsumed;
	}

	public int readLine(Text str, int maxLineLength) throws IOException {
		return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	public int readLine(Text str) throws IOException {
		return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
}
