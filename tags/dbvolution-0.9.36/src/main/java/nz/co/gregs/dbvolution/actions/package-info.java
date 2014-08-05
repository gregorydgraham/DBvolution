/**
 * DBvolution: Always Java, Never SQL.
 * <p>
 * DBAction collects the semantics of INSERT, UPDATE, and DELETE in a database agnostic way.
 * <p>
 * Use the {@link nz.co.gregs.dbvolution.actions.DBInsert#getInserts(nz.co.gregs.dbvolution.DBRow...) DBInsert getInserts method} to create inserts that will work on any database.
 * <p>
 * Use the {@link nz.co.gregs.dbvolution.actions.DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...) DBUpdate getUpdates method} to create updates that will work on any database.
 * <p>
 * Use the {@link nz.co.gregs.dbvolution.actions.DBDelete#getDeletes(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBRow...) DBDelete getDeletes method} to create inserts that will work on any database AND give enough information to recreate the deleted rows.
 * <p>
 * I've been writing SQL for 20 years and DBV is easier, I hope you like it.
 * <p>
 * <b>Gregory Graham<b>
 */
package nz.co.gregs.dbvolution.actions;
