/*
 * Copyright 2021 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.internal.database;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;

/**
 *
 * @author gregorygraham
 */
public class DatabaseList implements Serializable {

  private static final long serialVersionUID = 1L;

	private final static Random RANDOM = ThreadLocalRandom.current();
  
  private final HashMap<String, EntryValue> map = new HashMap<>(0);

  public DatabaseList() {
  }

  public DatabaseList(DBDatabase... databases) {
    for (var db : databases) {
      add(db);
    }
  }

  public synchronized int size() {
    return map.size();
  }

  public synchronized boolean isEmpty() {
    return map.isEmpty();
  }

  public synchronized boolean contains(Object o) {
    if (o instanceof DBDatabase) {
      DBDatabase db = (DBDatabase) o;
      return map.containsKey(EntryValue.getKey(db));
    } else {
      return false;
    }
  }

  public synchronized Iterator<DBDatabase> iterator() {
    return stream().map((v)->v.database).iterator();
  }

  public Stream<EntryValue> stream() {
    return map.values().stream();
  }

  /**
   * Adds the database to the DatabaseList as an unsynchronised member.
   *
   * @param database the database to be added.
   * @return TRUE if the database is new to the list, FALSE if the database has
   * already been added (the database is still added)
   */
  public synchronized final boolean add(DBDatabase database) {
    final EntryValue entry = new EntryValue(database);
    EntryValue put = map.put(entry.key, entry);
    return put == null;
  }

  /**
   * Removes the database from the DatabaseList.
   *
   * @param e the database to be removed.
   * @return TRUE if the database was in the list, FALSE if the database was
   * unknown.
   */
  public synchronized boolean remove(DBDatabase e) {
    EntryValue remove = map.remove(EntryValue.getKey(e));
    return remove == null;
  }

  public synchronized boolean containsAll(Collection<DBDatabase> c) {
    boolean allAreInTheMap = c
            .stream()
            .allMatch(t -> map.containsKey(EntryValue.getKey(t))
            );
    return allAreInTheMap;
  }

  /**
   * Adds all the databases to the DatabaseList as unsynchronised members.
   *
   * @param collectionOfDatabases the databases to be added.
   * @return TRUE if ALL the databases are new to the list, FALSE if ANY of the
   * database has already been added (all databases are still added)
   */
  public synchronized boolean addAll(Collection<? extends DBDatabase> collectionOfDatabases) {
    boolean add = true;
    for (DBDatabase dBDatabase : collectionOfDatabases) {
      add &= this.add(dBDatabase);
    }
    return add;
  }

  /**
   * Removes all the databases to the DatabaseList.
   *
   * @param collectionOfDatabases the databases to be removed.
   * @return TRUE if ALL the databases were known to the list, FALSE if ANY of
   * the databases were unknown (all databases are still removed)
   */
  public synchronized boolean removeAll(Collection<DBDatabase> collectionOfDatabases) {
    boolean removed = true;
    for (DBDatabase db : collectionOfDatabases) {
      removed &= remove(db);
    }
    return removed;
  }

  private synchronized void set(DBDatabase db, DBDatabaseCluster.Status newStatus) {
    if(db!=null && newStatus != null) {
      final String key = EntryValue.getKey(db);
      EntryValue val = map.get(key);
      boolean fresh = false;
      boolean changed = false;
      if (val == null) {
        val = new EntryValue(db);
        fresh = true;
      }
      if (val.status != newStatus) {
        changed = true;
      }
      val.status = newStatus;
      map.put(key, val);
      if (!fresh) {
        if (newStatus.anyOf(UNSYNCHRONISED, QUARANTINED, DEAD)) {
          val.escalationCounter++;
        }
        if (exceedsUnsynchronisedLimit(val)) {
          val.escalationCounter = 0;
          setQuarantined(db);
        }
        if (exceedsQuarantineLimit(val)) {
          val.escalationCounter = 0;
          setDead(db);
        }
        if (READY.equals(newStatus)) {
          val.escalationCounter = 0;
        }
      }
    }
  }

  public synchronized void setReady(DBDatabase db) {
    set(db, READY);
  }

  public synchronized void setUnsynchronised(DBDatabase db) {
    set(db, UNSYNCHRONISED);
  }

  public synchronized void setPaused(DBDatabase db) {
    set(db, PAUSED);
  }

  public synchronized void setDead(DBDatabase db) {
    set(db, DEAD);
  }

  public synchronized void setQuarantined(DBDatabase db) {
    set(db, QUARANTINED);
  }

  public synchronized void setUnknown(DBDatabase db) {
    set(db, UNKNOWN);
  }

  public synchronized void setProcessing(DBDatabase db) {
    set(db, PROCESSING);
  }

  public synchronized void setSynchronising(DBDatabase db) {
    set(db, SYNCHRONIZING);
  }

  public synchronized DBDatabase[] getDatabases() {
    final List<DBDatabase> list = toList();
    return list.toArray(new DBDatabase[]{});
  }

  public List<DBDatabase> toList() {
    return stream().map((v)->v.database).collect(Collectors.toList());
  }

  public List<DBDatabase> toList(DBDatabaseCluster.Status... statuses) {
    return stream().filter((v)->v.status.anyOf(statuses)).map((v)->v.database).collect(Collectors.toList());
  }

  public synchronized DBDatabaseCluster.Status getStatusOf(DBDatabase statusOfThisDatabase) {
    return map.getOrDefault(EntryValue.getKey(statusOfThisDatabase), EntryValue.UNKNOWN).status;
  }

  public synchronized boolean isReady(DBDatabase database) {
    EntryValue val = map.get(EntryValue.getKey(database));
    return val != null && READY.equals(val.status);
  }

  public synchronized DBDatabase[] getDatabases(DBDatabaseCluster.Status... statuses) {
    List<DBDatabase> found = new ArrayList<>(0);
    for (EntryValue entry : map.values()) {
      for (DBDatabaseCluster.Status status : statuses) {
        if (entry.status.equals(status)) {
          found.add(entry.database);
        }
      }
    }
    DBDatabase[] array = found.toArray(new DBDatabase[]{});
    return array;
  }

  public synchronized long countReadyDatabases() {
    return countDatabases(READY);
  }

  public synchronized long countPausedDatabases() {
    return toList(PAUSED).size();
  }

  public synchronized long countDatabases(DBDatabaseCluster.Status... statuses) {
    return getDatabases(statuses).length;
  }

  public synchronized void clear() {
    map.clear();
  }

  public synchronized boolean areAllReady() {
    return countDatabases(DBDatabaseCluster.Status.READY) == map.size();
  }

  public synchronized boolean isDead(DBDatabase db) {
    return DEAD.equals(getStatusOf(db));
  }

  private long getEscalationLimit(DBDatabaseCluster.Status status) {
    return 6;
  }

  private boolean exceedsQuarantineLimit(EntryValue val) {
    return val.status.equals(QUARANTINED)
                && val.escalationCounter > getEscalationLimit(QUARANTINED);
  }

  private boolean exceedsUnsynchronisedLimit(EntryValue val) {
    return val.status.equals(UNSYNCHRONISED)
                && val.escalationCounter > getEscalationLimit(UNSYNCHRONISED);
  }

  public synchronized void replace(DBDatabase databaseToAdd) {
    if (!contains(databaseToAdd)){
      add(databaseToAdd);
    }else{
      final EntryValue oldVal = map.get(EntryValue.getKey(databaseToAdd));
      final EntryValue newVal = new EntryValue(databaseToAdd);
      newVal.copy(oldVal);
      map.replace(newVal.key, newVal);
    }
  }

  public DBDatabase getRandomDatabase() {
    if (size() < 1) {
      return null;
    }
    if (size() == 1){
      return getEntryValueByIndex(0).database;
    }
    final int randNumber = RANDOM.nextInt(map.size());
    DBDatabase randomElement = getEntryValueByIndex(randNumber).database;
    return randomElement;
  }

  private EntryValue getEntryValueByIndex(int index) {
    return map.values().toArray(EntryValue.EMPTY_ARRAY)[index];
  }
  

  public static class EntryValue extends Object {
    
    public static final EntryValue UNKNOWN = new EntryValue(null);
    public static final EntryValue[] EMPTY_ARRAY = new EntryValue[0];
    private static final String UNKNOWN_KEY = "~~UNKNOWN_KEY~~";
    
    final String key;
    final DBDatabase database;
    DBDatabaseCluster.Status status;
    long escalationCounter = 0;

    private static String getKey(DBDatabase db) {
      return db==null?UNKNOWN_KEY:db.getSettings().encode();
    }

    public EntryValue(DBDatabase database1) {
      if(database1 == null) {
        this.database = null;
        this.key = UNKNOWN_KEY;
        this.status = DBDatabaseCluster.Status.UNKNOWN;
        this.escalationCounter = Long.MIN_VALUE;
      } else {
        this.database = database1;
        this.key = getKey(database1);
        this.status = UNSYNCHRONISED;
        this.escalationCounter = 0;
      }
    }
    
    public void copy(EntryValue copyValues){
      escalationCounter = copyValues.escalationCounter;
      status = copyValues.status;
    }
  }
}
