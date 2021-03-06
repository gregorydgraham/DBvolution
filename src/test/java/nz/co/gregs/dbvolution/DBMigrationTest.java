/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBMigrationTest extends AbstractTest {

	public DBMigrationTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Before
	public void setup() throws SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Villain());
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Hero());

		database.createTable(new Villain());
		database.createTable(new Hero());

		database.insert(new Villain("Dr Nonono"), new Villain("Dr Karma"), new Villain("Dr Dark"));
		database.insert(new Hero("James Security"), new Hero("Straw Richards"), new Hero("Lightwing"));
	}

	@After
	public void teardown() throws SQLException {
//		database.preventDroppingOfTables(false);
//		database.dropTableNoExceptions(new Villain());
//		database.preventDroppingOfTables(false);
//		database.dropTableNoExceptions(new Hero());
	}
	
	public static class Villain extends DBRow {
		
		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString name = new DBString();

		public Villain() {
		}

		public Villain(String name) {
			this.name.setValue(name);
		}
	}

	public static class Hero extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString name = new DBString();

		public Hero(String name) {
			this.name.setValue(name);
		}

		public Hero() {
		}
	}

	public static class Fight extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString hero = new DBString();

		@DBColumn
		public DBString villain = new DBString();
	}

	public static class Professional extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		public DBString title = new DBString();

		@DBColumn
		public DBString surname = new DBString();
	}
	
	@Test
	public void testMapping1ColumnWithDBmigrationMap() throws SQLException{
		database.setPrintSQLBeforeExecuting(true);
		DBMigration<MapVillainToProfessional> migration = database.getDBMigrationMap(new MapVillainToProfessional());
		migration.setBlankQueryAllowed(Boolean.TRUE);
		List<MapVillainToProfessional> rows = migration.getAllRows();
		
		for(Professional prof: rows){
			Assert.assertThat(prof.title.stringValue(), Matchers.is("Dr"));
			Assert.assertThat(prof.surname.stringValue(), Matchers.isOneOf("Nonono", "Karma", "Dark"));
		}
		database.setPrintSQLBeforeExecuting(false);
		
		database.preventDroppingOfTables(false);
		final Professional professional = new Professional();
		database.dropTableNoExceptions(professional);
		database.createTable(professional);
		
		migration.migrateAllRows();
		
		DBTable<Professional> table = database.getDBTable(professional);
		List<Professional> allRows = table.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), Matchers.is(3));
		for(Professional prof: allRows){
			Assert.assertThat(prof.title.stringValue(), Matchers.is("Dr"));
			Assert.assertThat(prof.surname.stringValue(), Matchers.isOneOf("Nonono", "Karma", "Dark"));
		}
	}

	public static class MapVillainToProfessional extends Professional{
		private static final long serialVersionUID = 1L;
		public Villain baddy = new Villain();

		{
			baddy.name.permittedPattern("Dr %");
			title = baddy.column(baddy.name).substringBefore(" ").asExpressionColumn();
			surname = baddy.column(baddy.name).substringAfter(" ").asExpressionColumn();
		}
	}
	
	public static class MapHeroAndVillianToFight extends Fight {
		private static final long serialVersionUID = 1L;

		public Villain baddy = new Villain();
		public Hero goody = new Hero();

		{
			hero = goody.column(goody.name).asExpressionColumn();
			villain = baddy.column(baddy.name).asExpressionColumn();
		}
	}

	@Test
	public void testJoining2TablesWithDBMigationMap() throws SQLException, UnexpectedNumberOfRowsException {

		DBMigration<MapHeroAndVillianToFight> migration = database.getDBMigrationMap(new MapHeroAndVillianToFight());
		migration.setBlankQueryAllowed(Boolean.TRUE);
		migration.setCartesianJoinAllowed(Boolean.TRUE);
		List<MapHeroAndVillianToFight> fights = migration.getAllRows();
		database.print(fights);
		Assert.assertThat(fights.size(), Matchers.is(9));
		Assert.assertThat(fights.get(0).villain.stringValue(), Matchers.isOneOf("Dr Nonono", "Dr Karma", "Dr Dark"));
		Assert.assertThat(fights.get(0).hero.stringValue(), Matchers.isOneOf("James Security", "Straw Richards", "Lightwing"));
		
		for(Fight fight: fights){
			Assert.assertThat(fight.villain.stringValue(), Matchers.isOneOf("Dr Nonono","Dr Karma","Dr Dark"));
			Assert.assertThat(fight.hero.stringValue(), Matchers.isOneOf("James Security","Straw Richards","Lightwing"));
		}
		
		database.preventDroppingOfTables(false);
		final Fight fight = new Fight();
		database.dropTableNoExceptions(fight);
		database.createTable(fight);
		
		migration.migrateAllRows();
		
		DBTable<Fight> query = database.getDBTable(fight);
		List<Fight> allRows = query.setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), Matchers.is(9));
		for(Fight newFight: allRows){
			Assert.assertThat(newFight.villain.stringValue(), Matchers.isOneOf("Dr Nonono","Dr Karma","Dr Dark"));
			Assert.assertThat(newFight.hero.stringValue(), Matchers.isOneOf("James Security","Straw Richards","Lightwing"));
		}
		
	}


}
