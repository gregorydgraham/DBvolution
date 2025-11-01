/*
 * Copyright 2023 Gregory Graham.
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
package nz.co.gregs.dbvolution.actions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.databases.metadata.Options;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.generation.DataRepo;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class DBAddMissingColumnsToTable extends DBAction {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(DBAddMissingColumnsToTable.class);

  public <R extends DBRow> DBAddMissingColumnsToTable(DBRow table) {
    super(table, QueryIntention.ADD_MISSING_COLUMNS_TO_TABLE);
  }

  @Override
  protected DBActionList getRevertDBActionList() {
    DBActionList reverts = new DBActionList();
    return reverts;
  }

  @Override
  public List<String> getSQLStatements(DBDatabase db) {
    List<String> result = new ArrayList<>(1);

    return result;
  }
  
  @Override
  protected DBActionList prepareActionList(DBDatabase database) throws AccidentalBlankQueryException, SQLException, UnableToInstantiateDBRowSubclassException {
    DBActionList actions = new DBActionList();
    try {

      List<PropertyWrapper<?, ?, ?>> newColumns = new ArrayList<>();
      DBRow table = getRow();

      Options opts = Options.empty()
              .setDBDatabase(database)
              .setRequiredTables(table)
              .setCompileImmediately(false);
      DataRepo repo = DataRepo.getDataRepoFor(opts);
      Optional<DBTableClass> maybeExistingTable = repo.getTable(table);
      if (maybeExistingTable.isPresent()) {
        DBTableClass existingTable = maybeExistingTable.get();
        List<DBTableField> existingFields = existingTable.getFields();
        Map<String, DBTableField> existingColumns 
                = existingFields
                        .stream()
                        .collect(Collectors.toMap((f)->f.columnName, (f)->f));

        var columnPropertyWrappers = table.getColumnPropertyWrappers();
        for (var columnPropertyWrapper : columnPropertyWrappers) {
          if (columnPropertyWrapper != null && !columnPropertyWrapper.hasColumnExpression()) {
            String columnName = columnPropertyWrapper.columnName();
            DBDefinition definition = database.getDefinition();
            String formattedColumnName = definition.formatColumnName(columnName);
            DBTableField got = existingColumns.get(formattedColumnName);
            if (got == null) {
              newColumns.add(columnPropertyWrapper);
            }
          }
        }
        for (var newColumn : newColumns) {
          actions.add(new DBAlterTableAddColumnIfNeeded(table, newColumn));
        }
      }
    } catch (IOException ex) {
      System.getLogger(DBAddMissingColumnsToTable.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
    }

    return actions;
  }

  @Override
  protected void prepareRollbackData(DBDatabase db, DBActionList actions) {
    // with any real database attempting this is absurd
  }
}
