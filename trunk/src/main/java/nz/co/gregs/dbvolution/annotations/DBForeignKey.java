/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import nz.co.gregs.dbvolution.DBRow;

/**
 * 
 * Indicates that this field is a Foreign Key to another database table and specifies the table
 * 
 * public class MyTable extends DBRow {
 * \@DBForeignKey(OtherTable.class)
 * \@DBColumn("other_table_fk")
 * public DBInteger otherTableFK = new DBInteger();
 * ...
 * 
 * The class reference is to another DBRow class that represents the table of the foreign key.
 * 
 * DBForeignKey works with DBPrimaryKey and DBQuery to make Natural Joins happen automatically.
 * 
 * DBForeignKey does not require a foreign key relationship to exist within the database and does not enforce referential integrity.
 * 
 * DBForeignKey is generated automatically by DBTableClassGenerator if the foreign key is specified within the database.
 *
 * @author gregory.graham
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DBForeignKey {

	/**
	 * Identifies the foreign table by its {@code DBRow} implementation class.
	 * @return the DBRow subclass that this foreign key references.
	 */
    Class<? extends DBRow> value();
    
    /**
     * Identifies the foreign column by column name.
     * This must be a column in the foreign class.
     * 
     * <p> If not specified, and the foreign class as exactly one primary key column,
     * the primary key of the foreign class is used.
     * <p> Must be specified if the foreign class has no primary key,
     * or if it has multiple primary key columns (not supported yet).
     * @return the name of the column this foreign key references.
     */
    String column() default "";

}
