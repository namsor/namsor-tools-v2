/*
 * Copyright NamSor - All Rights Reserved.
 */
package com.namsor.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author elian
 */
public class IsDone {

    private final Set done = Collections.synchronizedSet(new HashSet());

    public synchronized boolean isDone(String key) {
        return getDone().contains(key.trim().toLowerCase());
    }

    public synchronized void done(String key) throws IOException {
        //if (!key_k.contains("page")) {
        Logger.getLogger(getClass().getName()).info(key + " done.");
        //}
        getDone().add(key.trim().toLowerCase());
        getDoneWriter().append(key.trim().toLowerCase() + "\n");
        getDoneWriter().flush();
    }

    /**
     * @return the doneWriter
     */
    Writer getDoneWriter() {
        return doneWriter;
    }

    /**
     * @param doneWriter the doneWriter to set
     */
    void setDoneWriter(Writer doneWriter) {
        this.doneWriter = doneWriter;
    }

    private Writer doneWriter;

    public static List<String> readFile(String fileName) throws IOException {
        List<String> donePlace = new ArrayList();
        File exists = new File(fileName);
        if (!exists.exists()) {
            exists.createNewFile();
            return donePlace;
        }
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String doneLine = br.readLine();
        int line = 0;
        while (doneLine != null) {
            donePlace.add(doneLine.trim());
            doneLine = br.readLine();
            if (line % 100000 == 0) {
                Logger.getLogger(IsDone.class.getName()).info("Loading " + fileName + ":" + line);
            }
            line++;
        }
        br.close();
        return donePlace;
    }

    public static IsDone createIsDoneStatic(String taskName) {
        try {
            return createIsDone(taskName);
        } catch (IOException ex) {
            Logger.getLogger(IsDone.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static IsDone createIsDone(String taskName) throws IOException {
        IsDone isDone = new IsDone();
        String fileName = "./data/done_" + taskName + ".txt";
        Logger.getLogger(IsDone.class.getName()).info("Loading DONE " + taskName);
        List<String> doneStr = readFile(fileName);
        if (doneStr != null) {
            isDone.getDone().addAll(doneStr);
        }
        Logger.getLogger(IsDone.class.getName()).info("Loaded DONE " + taskName + " " + doneStr.size() + " items.");
        isDone.setDoneWriter(new FileWriter(fileName, true));
        return isDone;
    }

    /**
     * @return the done
     */
    public Set getDone() {
        return done;
    }

}
