/*
 * Copyright 2015 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.datatypes.spatial2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import com.vividsolutions.jts.geom.Point;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.LineSegment2DResult;

/**
 * Represents datatypes and columns that are composed of a 2 points
 * connected as a line.
 *
 * <p>
 * Use this type if the database column stores a series of 2-dimensional (that
 * is X and Y points) that are contiguous and open.
 *
 * <p>
 * Alternatives to a DBLineSegment2D are a series of points constituting a line {@link DBLine2D},
 * infinite lines {@link @DBRay2D}, closed paths {@link DBRing2D}, and closed
 * paths defining a solid {@link DBPolygon2D}.
 *
 * <p>
 * Common datatypes covered by this type include LINESTRING.
 *
 * <p>
 * Spatial types are not automatically generated during schema extraction so you
 * may need to change some DBString fields.
 *
 * @author gregorygraham
 */
public class DBLineSegment2D extends QueryableDatatype implements LineSegment2DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 *
	 * Use this method to create the DBLine2D used in your DBRow subclass.
	 *
	 */
	public DBLineSegment2D() {
	}

	/**
	 * Create a DBLine2D with the value set to the {@link LineString} provided.
	 *
	 * <p>
	 * This is a convenient way to assign a constant value in an expression or
	 * DBRow subclass.
	 *
	 * @param lineSegment 
	 */
	public DBLineSegment2D(LineSegment lineSegment) {
		super(lineSegment);
	}

	/**
	 * Create a DBLine2D using the expression supplied.
	 *
	 * <p>
	 * Useful for defining expression columns in DBRow subclass that acquire their
	 * value from a transformation of data at query time.
	 *
	 * @param columnExpression
	 */
	public DBLineSegment2D(nz.co.gregs.dbvolution.expressions.LineSegment2DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Set the value of this DBLine2D to the value provided.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param line
	 */
	public void setValue(LineSegment line) {
		setLiteralValue(line);
	}

	/**
	 * Set the value of this DBLine2D to the value provided.
	 *
	 * <p>
	 * The series of points will combined into a line for you.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param point1
	 * @param point2
	 */
	public void setValue(Point point1, Point point2) {
		LineSegment line = new LineSegment(point1.getCoordinate(), point2.getCoordinate());
		setLiteralValue(line);
	}

	/**
	 * Set the value of this DBLine2D to the value provided.
	 *
	 * <p>
	 * The series of coordinates will combined into a line for you.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param coord1 
	 * @param coord2 
	 */
	public void setValue(Coordinate coord1, Coordinate coord2) {
		LineSegment line = new LineSegment(coord1, coord2);
		setLiteralValue(line);
	}

	@Override
	public LineSegment getValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return (LineSegment) getLiteralValue();
		}
	}

	/**
	 * Transform the value of the DBLine2D into a
	 * {@link LineString JTS LineString}
	 *
	 * @return the value of this object if defined and not NULL, NULL otherwise.
	 */
	public LineSegment jtsLineSegmentValue() {
		return getValue();
	}

	@Override
	public String getSQLDatatype() {
		return " LINESTRING ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		LineSegment lineString = getValue();
		if (lineString == null) {
			return db.getDefinition().getNull();
		} else {
			String str = db.getDefinition().transformLineSegmentIntoDatabaseLineSegment2DFormat(lineString);
			return str;
		}
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		LineSegment lineSegment = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				lineSegment = database.getDefinition().transformDatabaseLineSegment2DValueToJTSLineSegment(string);
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(DBLineSegment2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string);
			}
			return lineSegment;
		}
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

}
