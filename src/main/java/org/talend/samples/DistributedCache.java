package org.talend.samples;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DistributedCache {

    private static final Log LOG = LogFactory.getLog(DistributedCache.class);

    public static final String CACHE_FILES = "tmpfiles";

    public static final String CACHE_ARCHIVES = "tmparchives";

    public static void addCacheFile(URI uri, Configuration conf) {
        try {
            String files = conf.get(CACHE_FILES);
            String newFile = validateFile(uri, conf).toString();
            conf.set(CACHE_FILES, files == null ? newFile : files + "," + newFile);
        } catch (Exception ex) {
            LOG.warn("options parsing failed: " + ex.getMessage());
        }
    }

    public static void addCacheArchive(URI uri, Configuration conf) {
        try {
            String files = conf.get(CACHE_ARCHIVES);
            String newFile = validateFile(uri, conf).toString();
            conf.set(CACHE_ARCHIVES, files == null ? newFile : files + "," + newFile);
        } catch (Exception ex) {
            LOG.warn("options parsing failed: " + ex.getMessage());
        }
    }

    static private String validateFile(URI file, Configuration conf) throws IOException {
        if (file == null) {
            return null;
        }
        String finalPath;
        Path path = new Path(file);
        FileSystem localFs = FileSystem.getLocal(conf);
        if (file.getScheme() == null) {
            // default to the local file system
            // check if the file exists or not first
            if (!localFs.exists(path)) {
                throw new FileNotFoundException("File " + file + " does not exist.");
            }
            finalPath = path.makeQualified(localFs).toString();
        } else {
            // check if the file exists in this file system
            // we need to recreate this filesystem object to copy
            // these files to the file system jobtracker is running
            // on.
            FileSystem fs = path.getFileSystem(conf);
            if (!fs.exists(path)) {
                throw new FileNotFoundException("File " + file + " does not exist.");
            }
            finalPath = path.makeQualified(fs).toString();
        }
        return finalPath;
    }
}