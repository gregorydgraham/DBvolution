/*
 * Copyright 2018 gregorygraham.
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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import nz.co.gregs.dbvolution.utility.TableSet;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.actions.DBAction;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster;
import nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status;
import static nz.co.gregs.dbvolution.databases.DBDatabaseCluster.Status.*;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.reflection.DataModel;
import nz.co.gregs.dbvolution.utility.StringCheck;
import nz.co.gregs.dbvolution.utility.PreferencesImproved;
import nz.co.gregs.dbvolution.utility.Timeout;
import nz.co.gregs.dbvolution.utility.encryption.Encryption_Internal;
import nz.co.gregs.separatedstring.Builder;
import nz.co.gregs.separatedstring.Decoder;
import nz.co.gregs.separatedstring.Encoder;

/**
 *
 * @author gregorygraham
 */
public class ClusterDetails implements Serializable {

  private final static long serialVersionUID = 1l;

  private static final Logger LOG = Logger.getLogger(ClusterDetails.class.getName());

  private final DatabaseList members = new DatabaseList();

  private transient final Set<DBRow> requiredTables = Collections.synchronizedSet(DataModel.getRequiredTables());
  private transient final Set<DBRow> trackedTables = Collections.synchronizedSet(new HashSet<>());
  private transient final Map<DBDatabase, Queue<DBAction>> queuedActions = Collections.synchronizedMap(new HashMap<>(0));

  private transient final PreferencesImproved prefs = PreferencesImproved.userNodeForPackage(this.getClass());
  private String clusterLabel = "NotDefined";
  private boolean supportsDifferenceBetweenNullAndEmptyString = true;
  private boolean quietExceptions = false;
  private DBDatabaseCluster.Configuration configuration = DBDatabaseCluster.Configuration.fullyManual();

  private transient final Lock synchronisingLock = new ReentrantLock();
  private transient final Condition aDatabaseHasBeenSynchronised = synchronisingLock.newCondition();
  private transient final Condition allDatabasesAreSynchronised = synchronisingLock.newCondition();
  private transient final Condition someDatabasesNeedSynchronizing = synchronisingLock.newCondition();
  private transient final Condition readyDatabaseIsAvailable = synchronisingLock.newCondition();
  private DatabaseConnectionSettings clusterSettings;
  private DBDatabase preferredDatabase;

  private final static Random RANDOM = new Random();
  private boolean preferredDatabaseRequired;
  private boolean stillRunning = true;
  private final PropertyChangeSupport propertyChangeSupport;
  private transient final List<LoggedException> loggedExceptions = new ArrayList<>(0);

  public ClusterDetails(String label) {
    this.clusterLabel = label;
    propertyChangeSupport = new PropertyChangeSupport(this);
  }

  public void addPropertyChangeListener(PropertyChangeListener pcl) {
    propertyChangeSupport.addPropertyChangeListener(pcl);
  }

  public void removePropertyChangeListener(PropertyChangeListener pcl) {
    propertyChangeSupport.removePropertyChangeListener(pcl);
  }

  public final boolean add(DBDatabase databaseToAdd) {
    if (databaseToAdd != null) {
      propertyChangeSupport.firePropertyChange("new member", null, databaseToAdd);
      DBDatabase database = databaseToAdd;
      final boolean clusterSupportsDifferenceBetweenNullAndEmptyString = getSupportsDifferenceBetweenNullAndEmptyString();
      boolean databaseSupportsDifferenceBetweenNullAndEmptyString = database.supportsDifferenceBetweenNullAndEmptyString();
      if (clusterSupportsDifferenceBetweenNullAndEmptyString) {
        if (databaseSupportsDifferenceBetweenNullAndEmptyString) {
          // both support the diference so there is no conflict
        } else {
          // the cluster needs to change to handle Oracle-like behaviour
          setSupportsDifferenceBetweenNullAndEmptyString(false);
        }
      } else {
        if (databaseSupportsDifferenceBetweenNullAndEmptyString) {
          // currently the cluster and query should avoid any need to change the database behaviour
        }
      }

      if (clusterContains(database)) {
        members.setUnsynchronised(database);
      } else {
        addDatabaseAsUnsynchronized(database);
        saveClusterSettingsToPrefs();
        return true;
      }
    }
    return false;
  }

  public final void replace(DBDatabase databaseToAdd) {
    if (!members.contains(databaseToAdd)) {
      // it's not actually in the cluster yet so put it thru the add process
      // instead.
      add(databaseToAdd);
    } else {
      members.replace(databaseToAdd);
    }
  }

  private boolean addDatabaseAsUnsynchronized(DBDatabase database) {
    members.add(database);
    signalSomeDatabasesNeedSynchronising();
    return true;
  }

  private void signalSomeDatabasesNeedSynchronising() {
    synchronisingLock.lock();
    try {
      someDatabasesNeedSynchronizing.signalAll();
    } finally {
      synchronisingLock.unlock();
    }
  }

  public DBDatabase[] getAllDatabases() {
    synchronisingLock.lock();
    try {
      return members.getDatabases();
    } finally {
      synchronisingLock.unlock();
    }
  }

  public void quarantineDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
    if (clusterContains(database)) {
      if (hasTooFewReadyDatabases() && members.isReady(database)) {
        // Unable to quarantine the only remaining database
        propertyChangeSupport.firePropertyChange("failed to quarantine member", null, database);
        throw new UnableToRemoveLastDatabaseFromClusterException();
      }

      if (quietExceptions) {
      } else {
        logException(
                except,
                Level.WARNING,
                "QUARANTINING Database \"{0}\" from cluster {1} due to exception {2} with message \"{3}\"",
                database.getLabel(), clusterLabel, except.getClass().getSimpleName(), except.getMessage()
        );
      }
      database.setLastException(except);
      members.setQuarantined(database);
      queuedActions.remove(database);
      propertyChangeSupport.firePropertyChange("quarantined member", null, database);
      setAuthoritativeDatabase();
      if (database instanceof DBDatabaseCluster) {
        DBDatabaseCluster cluster = (DBDatabaseCluster) database;
        cluster.setHasQuarantined(true);
      }
    }
  }

  public void deadDatabase(DBDatabase database, Throwable except) throws UnableToRemoveLastDatabaseFromClusterException {
    if (clusterContains(database)) {
      if (hasTooFewReadyDatabases() && members.isReady(database)) {
        // Unable to quarantine the only remaining database
        propertyChangeSupport.firePropertyChange("last member can not die", null, database);
        throw new UnableToRemoveLastDatabaseFromClusterException();
      }

      if (quietExceptions) {
      } else {
        logException(
                except,
                Level.WARNING,
                "DEAD Database \"{0}\" removed from cluster {1} due to exception {2} with message \"{3}\"",
                new Object[]{database.getLabel(), clusterLabel, except.getClass().getSimpleName(), except.getMessage()}
        );
      }
      database.setLastException(except);
      members.setDead(database);
      queuedActions.remove(database);
      propertyChangeSupport.firePropertyChange("member has died", null, database);
      setAuthoritativeDatabase();
    }
  }

  public synchronized boolean removeDatabase(DBDatabase databaseToRemove) {
    DBDatabase database = databaseToRemove;
    if (hasTooFewReadyDatabases() && members.isReady(database)) {
      propertyChangeSupport.firePropertyChange("unable to remove last member", null, database);
      throw new UnableToRemoveLastDatabaseFromClusterException();
    } else {
      members.remove(database);
      propertyChangeSupport.firePropertyChange("removed database", null, database);
      setAuthoritativeDatabase();
      saveClusterSettingsToPrefs();
      checkSupportForDifferenceBetweenNullAndEmptyString();
      return true;
    }
  }

  protected boolean hasTooFewReadyDatabases() {
    return members.countReadyDatabases() < 2;
  }

  public DBDatabase[] getUnsynchronizedDatabases() {
    return members.getDatabases(DBDatabaseCluster.Status.UNSYNCHRONISED);
  }

  public Queue<DBAction> getActionQueue(DBDatabase db) {
    synchronized (queuedActions) {
      Queue<DBAction> queue = queuedActions.get(db);
      if (queue == null) {
        queue = new LinkedBlockingQueue<>();
        queuedActions.put(db, queue);
      }
      return queue;
    }
  }

  public DBRow[] getRequiredAndTrackedTables() {
    var tables = new TableSet();

    tables.addAll(requiredTables);
    tables.addAll(trackedTables);
    return tables.toArray(new DBRow[]{});
  }

  public void setTrackedTables(Collection<DBRow> rows) {
    ArrayList<DBRow> oldValue = new ArrayList<>(trackedTables);
    trackedTables.clear();
    propertyChangeSupport.firePropertyChange("cleared tracked tables", oldValue, trackedTables);
    for (DBRow row : rows) {
      addTrackedTable(row, false);
    }
    saveTrackedTables();
  }

  public void addTrackedTable(DBRow row) {
    addTrackedTable(row, true);
  }

  private void addTrackedTable(DBRow row, boolean saveTablesAutomatically) {
    synchronized (trackedTables) {
      trackedTables.add(DBRow.getDBRow(row.getClass()));
      propertyChangeSupport.firePropertyChange("added tracked table", null, row);
    }
    if (saveTablesAutomatically) {
      saveTrackedTables();
    }
  }

  public void addTrackedTables(Collection<DBRow> rows) {
    for (DBRow row : rows) {
      addTrackedTable(row, false);
    }
    saveTrackedTables();
  }

  public void removeTrackedTable(DBRow row) {
    removeTrackedTable(row, true);
  }

  private void removeTrackedTable(DBRow row, boolean andSave) {
    synchronized (trackedTables) {
      trackedTables.remove(row);
      propertyChangeSupport.firePropertyChange("removed tracked table", null, row);
    }
    if (andSave) {
      saveTrackedTables();
    }
  }

  public void removeTrackedTables(Collection<DBRow> rows) {
    for (DBRow row : rows) {
      removeTrackedTable(row, false);
    }
    saveTrackedTables();
  }

  private void readyDatabase(DBDatabase databaseToReady) {
    members.setReady(databaseToReady);
    setAuthoritativeDatabase();
    signalThatADatabaseHasBeenSynchronised();
    signalReadyDatabaseIsAvailable();
  }

  private void signalReadyDatabaseIsAvailable() {
    synchronisingLock.lock();
    try {
      readyDatabaseIsAvailable.signalAll();
    } finally {
      synchronisingLock.unlock();
    }
  }

  protected boolean hasReadyDatabases() {
    return members.countReadyDatabases() > 0;
  }

  public DBDatabase[] getReadyDatabases() {
    return members.getDatabases(DBDatabaseCluster.Status.READY);
  }

  /**
   * Returns an array of all the ready databases that are available in random
   * order.
   *
   * <p>
   * If there is a preferred database it is placed at the beginning of the
   * array. If the preferred database is required then the method blocks until
   * it is ready or the cluster signals that it has failed.</p>
   *
   * @return an array of DBDatabase that are ready for this cluster
   */
  public DBDatabase[] getRandomReadyDatabaseArray() {
    DBDatabase preferredDB = getPreferredDatabase();
    final DatabaseList databaseList = new DatabaseList(members.getDatabases(DBDatabaseCluster.Status.READY));
    int index = 0;
    DBDatabase[] dbArray = new DBDatabase[databaseList.size()];
    if (preferredDB != null) {
      databaseList.remove(preferredDB);
      dbArray[0] = preferredDB;
      index++;
    }
    while (databaseList.size() > 0) {
      final DBDatabase randomDatabase = databaseList.getRandomDatabase();
      if (randomDatabase != null) {
        databaseList.remove(randomDatabase);
        dbArray[index] = randomDatabase;
        index++;
      }
    }
    return dbArray;
  }

  public DBDatabase getPausedDatabase() {
    DBDatabase template = getRandomReadyDatabase();
    members.setPaused(template);
    return template;
  }

  public DBDatabase getPreferredDatabase() {
    if (hasPreferredDatabase() && preferredDatabaseIsReady()) {
      return preferredDatabase;
    } else if (hasPreferredDatabase() && preferredDatabaseRequired) {
      try {
        waitUntilDatabaseHasSynchronised(preferredDatabase, 0L);
        return preferredDatabase;
      } catch (UnableToSynchronizeDatabase ex) {
        logException(
                ex,
                Level.SEVERE,
                "Cluster {0} could not synchronise preferred database {1} but preferred database is required: {2}",
                clusterLabel, preferredDatabase.getLabel(), ex.getLocalizedMessage()
        );
        return null;
      }
    } else {
      return null;
    }
  }

  public DBDatabase getReadyDatabase() {
    DBDatabase result = getPreferredDatabase();
    if (result == null) {
      result = getRandomReadyDatabase();
    }
    return result;
  }

  private DBDatabase getRandomReadyDatabase() {
    DBDatabase[] dbs = getReadyDatabases();
    int tries = 0;
    while (dbs.length < 1 && members.countPausedDatabases() > 0 && tries <= 10) {
      awaitReadyDatabase();
      dbs = getReadyDatabases();
      tries++;
    }
    if (dbs.length > 0) {
      final int randNumber = RANDOM.nextInt(dbs.length);
      DBDatabase randomElement = dbs[randNumber];
      return randomElement;
    }
    return null;
  }

  private void awaitReadyDatabase() {
    synchronisingLock.lock();
    try {
      readyDatabaseIsAvailable.await(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      logException(ex, Level.SEVERE, "Cluster {0} interrupted while waiting for ready database: {1}", getClusterLabel(), ex.getLocalizedMessage());
      Thread.currentThread().interrupt();
    } finally {
      synchronisingLock.unlock();
    }
  }

  public void addAll(DBDatabase[] databases) throws SQLException {
    for (DBDatabase database : databases) {
      add(database);
    }
  }

  public void addAll(Collection<DBDatabase> databases) throws SQLException {
    for (DBDatabase database : databases) {
      add(database);
    }
  }

  public synchronized DBDatabase getTemplateDatabase() {
    if (members.size() == 1 && configuration.isUseAutoRebuild()) {
      return getAuthoritativeDatabase();
    } else {
      if (members.countReadyDatabases() == 0 && members.countPausedDatabases() == 0) {
        return null;
      }
      return getPausedDatabase();
    }
  }

  private DBDatabase getAuthoritativeDatabase() {
    final DatabaseConnectionSettings authoritativeDCS = getAuthoritativeDatabaseConnectionSettings();
    if (authoritativeDCS != null) {
      try {
        return authoritativeDCS.createDBDatabase();
      } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
        logException(ex, Level.SEVERE, "Unable to retrieve authoritative database for cluster {0}: {1}", getClusterLabel(), ex.getLocalizedMessage());
        return null;
      }
    } else {
      return null;
    }
  }

  private void removedTrackedTablesFromPrefs() {
    prefs.remove(getTrackedTablesPrefsIdentifier());
  }

  private synchronized void saveTrackedTables() {
    if (configuration.isUseAutoRebuild()) {
      Set<Class<?>> previousClasses = new HashSet<>(0);
      Encoder rowClasses = getTrackedTablesSeparatedStringTemplate();
      for (DBRow trackedTable : trackedTables) {
        if (!previousClasses.contains(trackedTable.getClass())) {
          previousClasses.add(trackedTable.getClass());
          rowClasses.add(trackedTable.getClass().getName());
        }
      }
      String encodedTablenames = rowClasses.encode();
      try {
        final String encryptedText = Encryption_Internal.encrypt(encodedTablenames);
        final String name = getTrackedTablesPrefsIdentifier();
        prefs.put(name, encryptedText);
      } catch (CannotEncryptInputException ex) {
        logException(ex, Level.SEVERE, "Unable to encrypt tracked tables for cluster {0}: {1}", getClusterLabel(), ex.getLocalizedMessage());
      }
    }
  }

  public List<String> getSavedTrackedTables() {

    String encodedSettings = "";
    final String rawPrefsValue = prefs.get(getTrackedTablesPrefsIdentifier(), null);
    if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
      try {
        encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
      } catch (UnableToDecryptInput ex) {
        logException(ex, Level.SEVERE, "Unable to decrypt saved tracked tables for cluster {0}: {1}", getClusterLabel(), ex.getLocalizedMessage());
      }
    }
    Decoder seps = getTrackedTablesSeparatedStringTemplate().decoder();
    List<String> decodedRowClasses = seps.decode(encodedSettings);
    return decodedRowClasses;
  }

  public synchronized void loadTrackedTables() {
    Set<Class<DBRow>> previousClasses = new HashSet<>(0);
    if (configuration.isUseAutoRebuild()) {
      List<String> savedTrackedTables = getSavedTrackedTables();
      for (String savedTrackedTable : savedTrackedTables) {
        try {
          @SuppressWarnings("unchecked")
          Class<DBRow> trackedTableClass = (Class<DBRow>) Class.forName(savedTrackedTable);
          if (!previousClasses.contains(trackedTableClass)) {
            previousClasses.add(trackedTableClass);
            DBRow dbRow = DBRow.getDBRow(trackedTableClass);
            trackedTables.add(dbRow);
          }
        } catch (ClassNotFoundException ex) {
          logException(ex, Level.SEVERE, "Tracked Table {0} requested but not found while trying to rebuild cluster {1}", savedTrackedTable, getClusterLabel());
        }
      }
    }
  }

  public void logException(Throwable exception, Level level, String message, Object... messageItems) {
    loggedExceptions.add(
            new LoggedException(exception, level, message, messageItems)
    );
    if (!quietExceptions) {
      LOG.log(
              level,
              message,
              messageItems
      );
    }
  }

  private String getTrackedTablesPrefsIdentifier() {
    return getClusterLabel() + "_trackedtables";
  }

  private Encoder getTrackedTablesSeparatedStringTemplate() {
    return Builder.commaSeparated().encoder();
  }

  private synchronized void removeAuthoritativeDatabaseFromPrefs() {
    prefs.remove(getClusterLabel());
  }

  private synchronized void setAuthoritativeDatabase() {
    if (configuration.isUseAutoRebuild()) {
      for (DBDatabase db : members.getDatabases(DBDatabaseCluster.Status.READY)) {
        final String name = getClusterLabel();
        if (!db.isMemoryDatabase() && StringCheck.isNotEmptyNorNull(name)) {
          final String encode = db.getSettings().encode();
          try {
            prefs.put(name, Encryption_Internal.encrypt(encode));
          } catch (CannotEncryptInputException ex) {
            logException(
                    ex,
                    Level.SEVERE,
                    "Cluster {0} unable to encrypt connection settings for authoritative database {1}: {2}",
                    getClusterLabel(), db.getLabel(), ex.getLocalizedMessage()
            );
            prefs.put(name, encode);
          }
          return;
        }
      }
    }
  }

  public synchronized DatabaseConnectionSettings getAuthoritativeDatabaseConnectionSettings() {
    if (configuration.isUseAutoRebuild()) {
      String encodedSettings = "";
      final String rawPrefsValue = prefs.get(getClusterLabel(), null);
      if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
        try {
          encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
        } catch (UnableToDecryptInput ex) {
          logException(
                  ex,
                  Level.SEVERE,
                  "Cluster {0} unable to decrypt connection settings for authoritative database \"{1}\": {2}",
                  getClusterLabel(), rawPrefsValue, ex.getLocalizedMessage()
          );
          encodedSettings = rawPrefsValue;
        }
      }
      if (StringCheck.isNotEmptyNorNull(encodedSettings)) {
        DatabaseConnectionSettings settings = DatabaseConnectionSettings.decode(encodedSettings);
        return settings;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public boolean clusterContains(DBDatabase database) {
    return members.contains(database);
  }

  /**
   * @return the clusterLabel
   */
  public String getClusterLabel() {
    return clusterLabel;
  }

  /**
   * @param clusterLabel the clusterLabel to set
   */
  public void setClusterLabel(String clusterLabel) {
    this.clusterLabel = clusterLabel;
    setAuthoritativeDatabase();
  }

  public DBDatabase[] getQuarantinedDatabases() {
    return members.getDatabases(DBDatabaseCluster.Status.QUARANTINED);
  }

  public void removeAllDatabases() throws SQLException {
    members.clear();
    preferredDatabase = null;
  }

  public synchronized void dismantle() throws SQLException {
    try {
      removeAllDatabases();
    } catch (SQLException ex) {
      LOG.warning(ex.getLocalizedMessage());
    }
    try {
      removeAuthoritativeDatabaseFromPrefs();
    } catch (Exception ex) {
      LOG.warning(ex.getLocalizedMessage());
    }
    try {
      removeAddedDatabasesFromPrefs();
    } catch (Exception ex) {
      LOG.warning(ex.getLocalizedMessage());
    }
    try {
      removedTrackedTablesFromPrefs();
    } catch (Exception ex) {
      LOG.warning(ex.getLocalizedMessage());
    }
  }

  public boolean getAutoReconnect() {
    return configuration.isUseAutoReconnect();
  }

  public boolean getAutoRebuild() {
    return configuration.isUseAutoRebuild();
  }

  public boolean hasAuthoritativeDatabase() {
    return this.getAuthoritativeDatabaseConnectionSettings() != null;
  }

  public synchronized void setSupportsDifferenceBetweenNullAndEmptyString(boolean result) {
    supportsDifferenceBetweenNullAndEmptyString = result;
  }

  public boolean getSupportsDifferenceBetweenNullAndEmptyString() {
    checkSupportForDifferenceBetweenNullAndEmptyString();
    return supportsDifferenceBetweenNullAndEmptyString;
  }

  private void checkSupportForDifferenceBetweenNullAndEmptyString() {
    boolean supportsDifference = true;
    for (DBDatabase database : getAllDatabases()) {
      supportsDifference = supportsDifference && database.supportsDifferenceBetweenNullAndEmptyString();
    }
    setSupportsDifferenceBetweenNullAndEmptyString(supportsDifference);
  }

  public void setQuietExceptionsPreference(boolean bln) {
    this.quietExceptions = bln;
  }

  public void setConfiguration(DBDatabaseCluster.Configuration config) {
    this.configuration = config;
  }

  private synchronized void removeAddedDatabasesFromPrefs() {
    prefs.remove(getPrefsClusterSettingsKey());
  }

  private synchronized void saveClusterSettingsToPrefs() {
    if (configuration.isUseAutoConnect()) {
      final String name = getPrefsClusterSettingsKey();
      try {
        final String encode = clusterSettings.encode();
        final String encrypt = Encryption_Internal.encrypt(encode);
        prefs.put(name, encrypt);
      } catch (CannotEncryptInputException ex) {
        logException(
                ex,
                Level.SEVERE,
                "Cluster {0} unable to encrypt connection settings: {1}",
                getClusterLabel(), ex.getLocalizedMessage()
        );
      }
    }
  }

  private String getPrefsClusterSettingsKey() {
    return getClusterLabel() + "_settings";
  }

  public synchronized List<DBDatabase> getClusterHostsFromPrefs() {
    List<DBDatabase> databases = new ArrayList<>();
    if (configuration.isUseAutoConnect()) {
      String encodedSettings = "";
      final String rawPrefsValue = prefs.get(getPrefsClusterSettingsKey(), null);
      if (StringCheck.isNotEmptyNorNull(rawPrefsValue)) {
        try {
          encodedSettings = Encryption_Internal.decrypt(rawPrefsValue);
        } catch (UnableToDecryptInput ex) {
          logException(
                  ex,
                  Level.SEVERE,
                  "Cluster {0} unable to decrypt cluster hosts from preferences using {1}: {2}",
                  getClusterLabel(), rawPrefsValue, ex.getLocalizedMessage()
          );
          encodedSettings = rawPrefsValue;
        }
      }
      if (StringCheck.isNotEmptyNorNull(encodedSettings)) {
        final DatabaseConnectionSettings settings = DatabaseConnectionSettings.decode(encodedSettings);
        List<DatabaseConnectionSettings> decodedSettings = settings.getClusterHosts();
        for (DatabaseConnectionSettings host : decodedSettings) {
          try {
            final DBDatabase db = host.createDBDatabase();
            databases.add(db);
          } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
      }
    }
    return databases;
  }

  public boolean isSynchronized() {
    if (configuration.isUseAutoReconnect()) {
      // if we're using AutoReconnect then it is only synchronised when
      // the number of ready databases is exactly the same as the number of members
      return members.getDatabases(DBDatabaseCluster.Status.READY).length == members.size();
    } else {
      // if we AREN'T using AutoConnect then Quarantined and Dead databases are ignored as well
      // and it's only new/unsynchronised databases that are counted      
      return members.getDatabases(
              DBDatabaseCluster.Status.READY,
              DBDatabaseCluster.Status.QUARANTINED,
              DBDatabaseCluster.Status.DEAD).length == members.size();
    }
  }

  public boolean isNotSynchronized() {
    return !isSynchronized();
  }

  public void waitUntilSynchronised() {
    synchronisingLock.lock();
    try {
      while (isNotSynchronized() && stillRunning) {
        allDatabasesAreSynchronised.await(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException ex) {
      logException(ex, Level.SEVERE, "INTERRUPTED WHILE TRYING TO SYNCHRONISE CLUSTER {0}: {1}", clusterLabel, ex.getLocalizedMessage());
      Thread.currentThread().interrupt();
    } finally {
      synchronisingLock.unlock();
    }
  }

  public void waitUntilSynchronised(long timeoutInMilliseconds) {
    long actualTimeout = timeoutInMilliseconds > 0 ? timeoutInMilliseconds : 1000;
    synchronisingLock.lock();
    try {
      if (isNotSynchronized() && stillRunning) {
        allDatabasesAreSynchronised.await(actualTimeout, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException ex) {
      Logger.getLogger(ClusterDetails.class.getName()).log(Level.SEVERE, null, ex);
      Thread.currentThread().interrupt();
    } finally {
      synchronisingLock.unlock();
    }
  }

  /**
   * Waits until the database has been synchronized.
   *
   * <p>
   * Throws DatabaseNotSynchronized if the database cannot be synchronized. It
   * might not be in the cluster for instance or the cluster may think the
   * database connection is "dead".
   * </p>
   *
   * @param db
   * @throws nz.co.gregs.dbvolution.exceptions.UnableToSynchronizeDatabase
   */
  @Deprecated
  public void waitUntilDatabaseHasSynchronised(DBDatabase db) throws nz.co.gregs.dbvolution.exceptions.UnableToSynchronizeDatabase {
    waitUntilDatabaseHasSynchronised(db, 0L);
  }

  /**
   * Waits until the database has been synchronized or until the timeout has
   * been exceeded.
   *
   * <p>
   * Throws DatabaseNotSynchronized if the database cannot be synchronized. It
   * might not be in the cluster for instance or the cluster may think the
   * database connection is "dead".
   * </p>
   *
   * @param db
   * @param timeoutInMilliseconds
   * @throws nz.co.gregs.dbvolution.exceptions.UnableToSynchronizeDatabase
   */
  public void waitUntilDatabaseHasSynchronised(DBDatabase db, long timeoutInMilliseconds) throws UnableToSynchronizeDatabase {
    // simplest case: the database is already synchronised
    if (getStatusOf(db).equals(Status.READY)) {
      return;
    }
    // if the database isn't in the cluster just throw and go
    if (!this.clusterContains(db)) {
      throw new UnableToSynchronizeDatabase(getClusterLabel(), db, "database not found within cluster");
    }
    // if the database is dead or weird just throw and go
    if (getStatusOf(db).anyOf(Status.DEAD, Status.UNKNOWN)) {
      throw new UnableToSynchronizeDatabase(clusterLabel, db, "database dead or in otherwise unknown state");
    }

    // ok, now we can wait...
    waitUntilDatabaseHasSynchronised_internal(db, timeoutInMilliseconds);
  }

  private void waitUntilDatabaseHasSynchronised_internal(DBDatabase database, long timeoutInMilliseconds) throws UnableToSynchronizeDatabase {
    synchronisingLock.lock();
    try {
      final LocalDateTime start = LocalDateTime.now();
      final LocalDateTime end = start.plus(timeoutInMilliseconds, ChronoUnit.MILLIS);
      if (isEligibleForSynchronizing(database) && getStatusOf(database) != DBDatabaseCluster.Status.READY) {
        while (stillRunning
                && (timeoutInMilliseconds == 0l || (timeoutInMilliseconds > 0l && LocalDateTime.now().isBefore(end)))
                && isEligibleForSynchronizing(database)
                && getStatusOf(database) != DBDatabaseCluster.Status.READY) {
          aDatabaseHasBeenSynchronised.await(100, TimeUnit.MILLISECONDS);
          doSanityCheck();
        }
        if (!stillRunning) {
          throw new DatabaseShutdownInProgress();
        }
      }
    } catch (InterruptedException ex) {
      logException(ex, Level.SEVERE, "INTERRUPTED WHILE TRYING TO SYNCHRONISE CLUSTER {0}: {1}", clusterLabel, ex.getLocalizedMessage());
      Thread.currentThread().interrupt();
    } finally {
      synchronisingLock.unlock();
    }
  }

  private boolean isEligibleForSynchronizing(DBDatabase database) {
    final DBDatabaseCluster.Status statusOfDatabase = getStatusOf(database);
    final boolean notDead = statusOfDatabase != DBDatabaseCluster.Status.DEAD;
    return clusterContains(database) && (notDead || configuration.isUseAutoReconnect());
  }

  public long synchronizeSecondaryDatabases() {
    long sychronisedDBs = 0l;
    if (stillRunning) {
      DBDatabase[] addedDBs;
      addedDBs = members.getDatabases(DBDatabaseCluster.Status.UNSYNCHRONISED);
      for (DBDatabase db : addedDBs) {
        if (stillRunning) {
          //Do The Synchronising...
          sychronisedDBs += synchronizeSecondaryDatabase(db) ? 1 : 0;
        }
      }
    }
    return sychronisedDBs;
  }

  public boolean synchronizeSecondaryDatabase(DBDatabase secondary) {
    members.setSynchronising(secondary);

    DBDatabase template = null;
    boolean proceedWithSynchronization = true;
    final String secondaryLabel = secondary.getLabel();
    LOG.log(Level.FINEST, "Cluster {0} preparing for synchronisation of {1} database", new Object[]{clusterLabel, secondaryLabel});
    try {
      // we need to unpause the template no matter what happens so use a finally clause
      try {
        template = getTemplateDatabase();
        if (template != null) {
          // Check that we're not synchronising the reference database
          if (!template.getSettings().equals(secondary.getSettings())) {
            LOG.log(Level.FINEST, "{0} cluster can synchronise {1} database", new Object[]{clusterLabel, secondaryLabel});
            copyTemplateActionQueueToSecondary(template, secondary);
            // TODO change to use a queue of tables so we can re-try tables that require another table to exist
            for (DBRow table : getRequiredAndTrackedTables()) {
              final String tableName = table.getTableName();
              if (proceedWithSynchronization) {
                LOG.log(Level.FINEST, "{0} cluster checking table {1} exists", new Object[]{clusterLabel, tableName, secondaryLabel});
                // make sure the table exists in the cluster already
                if (template.tableExists(table)) {
                  LOG.log(Level.FINEST, "{0} cluster includes table {1}", new Object[]{clusterLabel, tableName});
                  // Make sure it exists in the new database
                  if (secondary.tableExists(table) == true) {
                    LOG.log(Level.FINEST, "{0} cluster removing data from table {2} on {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                    secondary.setPreventDroppingOfTables(false);
                    secondary.dropTable(table);
                    LOG.log(Level.FINEST, "{0} cluster removed data from table {2} on {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                  }
                  LOG.log(Level.FINEST, "{0} cluster creating table {2} ON {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                  secondary.createTable(table);
                  LOG.log(Level.FINEST, "{0} cluster created table {2} ON {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                  // Check that the table has data
                  final DBTable<DBRow> primaryTable = template.getDBTable(table);
                  try {
                    final Long primaryTableCount = primaryTable.count();
                    try {
                      if (primaryTableCount > 0) {
                        final DBTable<DBRow> primaryData = primaryTable.setBlankQueryAllowed(true).setTimeoutToForever();
                        // Check that the new database has data
                        LOG.log(Level.FINEST, "{0} cluster filling table {2} on {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                        List<DBRow> allRows = primaryData.getAllRows();
                        LOG.log(Level.FINEST, "{0} cluster filling table {2} ON {1} database with {3} rows", new Object[]{clusterLabel, secondaryLabel, tableName, allRows.size()});
                        final DBTable<DBRow> secondaryTable = secondary.getDBTable(table);
                        try {
                          secondaryTable.insert(allRows);
                          LOG.log(Level.FINEST, "{0} cluster filling table {2} ON {1} database", new Object[]{clusterLabel, secondaryLabel, tableName});
                        } catch (SQLException ex) {
                          proceedWithSynchronization = false;
                          logException(ex, Level.SEVERE, "CLUSTER {0} QUARANTINING DATABASE {1} BECAUSE OF {2}", clusterLabel, secondaryLabel, ex.getLocalizedMessage());
                          quarantineDatabaseAutomatically(secondary, ex);
                          //exit the loop, to avoid unnecessary tests
                          break;
                        }
                      }
                    } catch (SQLException exceptionGettingData) {
                      logException(exceptionGettingData, 
                              Level.WARNING, 
                              "CLUSTER {0} FAILED TO RETRIEVE TABLE DATA for {1}, will skip it: {2}", 
                              clusterLabel, tableName, exceptionGettingData.getLocalizedMessage());
                      // lets just skip this table since it seems to be broken
                    }
                  } catch (SQLException exceptionCountingPrimaryTable) {
                      logException(exceptionCountingPrimaryTable, 
                              Level.WARNING, 
                              "CLUSTER {0} failed to count TABLE {1} during synchonisation, will skip it: {2}", 
                              clusterLabel, tableName, exceptionCountingPrimaryTable.getLocalizedMessage());
                    // lets just skip this table since it seems to be broken
                  }
                }
              }
              LOG.log(Level.FINEST, "{0} cluster finished with table: {1}", new Object[]{clusterLabel, tableName});
            }
            // We've caught up with the template database so change the status 
            // to reflect the new state.
            members.setPaused(secondary);
          }
        }
      } catch (NoAvailableDatabaseException except) {
        // must be the first database
      } finally {
        // we no longer need the template, so let it get to work again
        releaseTemplateDatabase(template);
      }
      if (proceedWithSynchronization) {
        LOG.log(Level.FINEST, "{0} START SYNCHRONISING ACTIONS ON: {1}", new Object[]{clusterLabel, secondaryLabel});
        synchronizeActions(secondary);
        LOG.log(Level.FINEST, "{0} SUCCESSFULLY SYNCHRONISED: {1}", new Object[]{clusterLabel, secondaryLabel});
      }
    } catch (Exception exc) {
      logException(
              exc,
              Level.WARNING, 
              "CLUSTER {0} FAILED TO SYNCHRONISE: {1}", 
              clusterLabel, secondaryLabel
      );
      members.setUnsynchronised(secondary);
      return false;
    }
    // Successfully synchronised the new database :)
    return proceedWithSynchronization;
  }

  private void releaseTemplateDatabase(DBDatabase primary) {
    final boolean nullPrimary = primary != null;
    if (nullPrimary) {
      if (clusterContains(primary)) {
        synchronizeActions(primary);
      } else {
        // this might be ok, as an autorebuild cluster can use a database that 
        // isn't in the cluster to recreate the structure and data.
        LOG.log(Level.WARNING, "{0} SYNCHRONISING - FAILED TO RELEASE TEMPLATE {1} BECAUSE IT IS NOT A MEMBER - {2}", new Object[]{clusterLabel, primary.getLabel(), primary.getJdbcURL()});
      }
    }
  }

  private void copyTemplateActionQueueToSecondary(DBDatabase template, DBDatabase secondary) {
    Queue<DBAction> templateQ = getActionQueue(template);
    Queue<DBAction> secondaryQ = getActionQueue(secondary);
    secondaryQ.clear();
    secondaryQ.addAll(templateQ);
  }

  private void synchronizeActions(DBDatabase db) {
    if (db != null) {
      try {
        Queue<DBAction> queue = getActionQueue(db);
        while (queue != null && !queue.isEmpty()) {
          DBAction action = queue.remove();
          db.executeDBAction(action);
        }
        if (hasReadyDatabases()) {
          DBDatabase readyDatabase = getRandomReadyDatabase();
          if (readyDatabase != null) {
            db.setPrintSQLBeforeExecuting(readyDatabase.getPrintSQLBeforeExecuting());
            db.setBatchSQLStatementsWhenPossible(readyDatabase.getBatchSQLStatementsWhenPossible());
          }
        }
        readyDatabase(db);
      } catch (SQLException e) {
        quarantineDatabase(db, e);
      }
    }
  }

  public void quarantineDatabaseAutomatically(DBDatabase suspectDatabase, Throwable sqlException) {
    try {
      quarantineDatabase(suspectDatabase, sqlException);
    } catch (UnableToRemoveLastDatabaseFromClusterException doesntNeedToBeHandledAsItsAutomaticAndNotManual) {
      ;
    }
  }

  private void signalThatAllDatabasesHaveBeenSynchronised() {
    synchronisingLock.lock();
    try {
      allDatabasesAreSynchronised.signalAll();
    } finally {
      synchronisingLock.unlock();
    }
  }

  private void signalThatADatabaseHasBeenSynchronised() {
    synchronisingLock.lock();
    try {
      aDatabaseHasBeenSynchronised.signalAll();
      if (isSynchronized()) {
        signalThatAllDatabasesHaveBeenSynchronised();
      }
    } finally {
      synchronisingLock.unlock();
    }
  }

  public void setClusterSettings(DatabaseConnectionSettings settings) {
    this.clusterSettings = settings;
  }

  public DBDatabaseCluster.Status getStatusOf(DBDatabase db) {
    return members.getStatusOf(db);
  }

  public void setPreferredDatabase(DBDatabase database) {
    preferredDatabase = database;
  }

  public boolean hasPreferredDatabase() {
    return preferredDatabase != null;
  }

  private boolean preferredDatabaseIsReady() {
    return getStatusOf(preferredDatabase).equals(DBDatabaseCluster.Status.READY);
  }

  public void setPreferredDatabaseRequired(boolean b) {
    preferredDatabaseRequired = b;
  }

  public boolean isPreferredDatabaseRequired() {
    return preferredDatabaseRequired;
  }

  public DBDatabase[] getDatabasesForReconnecting() {
    return members.getDatabases(DBDatabaseCluster.Status.QUARANTINED, DBDatabaseCluster.Status.DEAD);
  }

  public void shutdown() {
    this.stillRunning = false;
  }

  public boolean isShuttingDown() {
    return !stillRunning;
  }

  private void doSanityCheck() {
    final DBDatabase[] syncking = members.getDatabases(Status.SYNCHRONIZING);
    final DBDatabase[] paused = members.getDatabases(PAUSED);
    if (syncking.length == 0 && paused.length > 0) {
      for (DBDatabase db : paused) {
        releaseTemplateDatabase(db);
      }
    }
  }

  public synchronized void addActionToQueues(DBAction action) {
    final DBDatabase[] allDatabases = getAllDatabases();
    for (DBDatabase db : allDatabases) {
      addActionToQueue(db, action);
    }
  }

  public synchronized void removeActionFromQueues(DBAction action) {
    final DBDatabase[] allDatabases = getAllDatabases();
    for (DBDatabase db : allDatabases) {
      removeActionFromQueue(db, action);
    }
  }

  public void addActionToQueue(DBDatabase database, DBAction action) {
    Queue<DBAction> queue = getActionQueue(database);
    if (queue != null) {
      synchronized (queue) {
        queue.add(action);
      }
    }
  }

  public void removeActionFromQueue(DBDatabase database, DBAction action) {
    final Queue<DBAction> queue = getActionQueue(database);
    if (queue != null) {
      synchronized (queue) {
        queue.remove(action);
      }
    }
  }

  public void setTimeout(Timeout maximumTimeForDatabaseEvents) {
    this.members.stream().forEach((e) -> e.database.setTimeout(maximumTimeForDatabaseEvents));
  }

  /**
   * @return the loggedExceptions
   */
  public List<LoggedException> getLoggedExceptions() {
    return loggedExceptions;
  }
}
