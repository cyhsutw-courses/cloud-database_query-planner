package org.vanilladb.core.storage.file;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDB;


/**
 * The VanillaDB file manager. The database system stores its data as files
 * within a specified directory. The file manager provides methods for reading
 * the contents of a file block to a Java byte buffer, writing the contents of a
 * byte buffer to a file block, and appending the contents of a byte buffer to
 * the end of a file. These methods are called exclusively by the class
 * {@link org.vanilladb.core.storage.file.Page Page}, and are thus
 * package-private. The class also contains two public methods: Method
 * {@link #isNew() isNew} is called during system initialization by
 * {@link VanillaDB#init}. Method {@link #size(String) size} is called by the
 * log manager and transaction manager to determine the end of the file.
 */
public class FileMgr {
	private static Logger logger = Logger.getLogger(FileMgr.class.getName());
	private File dbDirectory;
	private boolean isNew;
	private Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

	/**
	 * Creates a file manager for the specified database. The database will be
	 * stored in a folder of that name in the user's home directory. If the
	 * folder does not exist, then a folder containing an empty database is
	 * created automatically. Files for all temporary tables (i.e. tables
	 * beginning with "temp") are deleted.
	 * 
	 * @param dbName
	 *            the name of the directory that holds the database
	 */
	public FileMgr(String dbName) {
		String homedir = System.getProperty("user.home");
		dbDirectory = new File(homedir, dbName);
		isNew = !dbDirectory.exists();

		// create the directory if the database is new
		if (isNew && !dbDirectory.mkdir())
			throw new RuntimeException("cannot create " + dbName);

		// remove any leftover temporary tables
		for (String filename : dbDirectory.list())
			if (filename.startsWith("_temp"))
				new File(dbDirectory, filename).delete();

		if (logger.isLoggable(Level.INFO))
			logger.info("block size " + Page.BLOCK_SIZE);
	}

	/**
	 * Reads the contents of a disk block into a bytebuffer.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the bytebuffer
	 */
	void read(BlockId blk, ByteBuffer bb) {
		try {
			FileChannel fc = getFile(blk.fileName());
			synchronized (fc) {
				bb.clear();
				fc.read(bb, blk.number() * BLOCK_SIZE);
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	/**
	 * Writes the contents of a bytebuffer into a disk block.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the bytebuffer
	 */
	void write(BlockId blk, ByteBuffer bb) {
		try {
			FileChannel fc = getFile(blk.fileName());
			synchronized (fc) {
				bb.rewind();
				fc.write(bb, blk.number() * BLOCK_SIZE);
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	/**
	 * Appends the contents of a bytebuffer to the end of the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param bb
	 *            the bytebuffer
	 * @return a block ID refers to the newly-created block.
	 */
	BlockId append(String fileName, ByteBuffer bb) {
		try {
			FileChannel fc = getFile(fileName);
			synchronized (fc) {
				long newblknum = fc.size() / BLOCK_SIZE;
				BlockId blk = new BlockId(fileName, newblknum);
				bb.rewind();
				fc.write(bb, blk.number() * BLOCK_SIZE);
				return blk;
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot access " + fileName);
		}
	}

	/**
	 * Returns the number of blocks in the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @return the number of blocks in the file
	 */
	public long size(String fileName) {
		try {
			FileChannel fc = getFile(fileName);
			synchronized (fc) {
				return fc.size() / BLOCK_SIZE;
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot access " + fileName);
		}
	}

	/**
	 * Returns a boolean indicating whether the file manager had to create a new
	 * database directory.
	 * 
	 * @return true if the database is new
	 */
	public boolean isNew() {
		return isNew;
	}

	/**
	 * Returns the file channel for the specified filename. The file channel is
	 * stored in a map keyed on the filename. If the file is not open, then it
	 * is opened and the file channel is added to the map.
	 * 
	 * @param fileName
	 *            the specified filename
	 * @return the file channel associated with the open file.
	 * @throws IOException
	 */
	private FileChannel getFile(String fileName) throws IOException {
		synchronized (openFiles) {
			FileChannel fc = openFiles.get(fileName);
			if (fc == null) {
				File dbTable = new File(dbDirectory, fileName);
				RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
				fc = f.getChannel();
				openFiles.put(fileName, fc);
			}
			return fc;
		}
	}
}
