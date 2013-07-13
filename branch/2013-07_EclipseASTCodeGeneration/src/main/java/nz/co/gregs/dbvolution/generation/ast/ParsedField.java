package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.ClassInstanceCreation;
import org.eclipse.jdt.core.dom.FieldDeclaration;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.MarkerAnnotation;
import org.eclipse.jdt.core.dom.Modifier;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;
import org.eclipse.jdt.core.dom.VariableDeclarationFragment;

/**
 * The parsed details of an member field within a class.
 * 
 * <p> Member field declarations can specify multiple variables within the same declaration;
 * while only specifying the type once, and the annotations once.
 * This type transparently handles that and exposes the variables and independent
 * fields.
 * @author Malcolm Lett
 */
public class ParsedField {
	private ParsedFieldDeclaration parsedFieldDeclaration;
	private String name;
	
	/**
	 * Creates a new field and prepares the type context for addition of the field.
	 * Updates the imports in the type context.
	 * 
	 * <p> Note: field name duplication avoidance must be done outside of this method.
	 * @param typeContext
	 * @param fieldName
	 * @param fieldType
	 * @param isPrimaryKey
	 * @param columnName
	 * @return
	 */
	// TODO: use ParsedTypeRef instead of manually managing this
	public static ParsedField newDBTableColumnInstance(ParsedTypeContext typeContext, String fieldName, Class<?> fieldType, boolean isPrimaryKey, String columnName) {
		AST ast = typeContext.getAST();
		
		// add imports
		boolean fieldTypeImported = typeContext.ensureImport(fieldType);
		boolean dbTableColumnImported = typeContext.ensureImport(DBTableColumn.class);
		boolean dbTablePrimaryKeyImported = typeContext.ensureImport(DBTablePrimaryKey.class);
		
		// add field
		VariableDeclarationFragment variable = ast.newVariableDeclarationFragment();
		variable.setName(ast.newSimpleName(fieldName));
		FieldDeclaration field = ast.newFieldDeclaration(variable);
		field.setType(ast.newSimpleType(ast.newName(
				nameOf(fieldType, fieldTypeImported))));

		// add annotations
		if (isPrimaryKey) {
			MarkerAnnotation annotation = ast.newMarkerAnnotation();
			annotation.setTypeName(ast.newSimpleName(
					nameOf(DBTablePrimaryKey.class, dbTablePrimaryKeyImported)));
			field.modifiers().add(annotation);
		}
		StringLiteral annotationValue = ast.newStringLiteral();
		annotationValue.setLiteralValue(columnName);
		SingleMemberAnnotation annotation = ast.newSingleMemberAnnotation();
		annotation.setTypeName(ast.newSimpleName(
					nameOf(DBTableColumn.class, dbTableColumnImported)));
		annotation.setValue(annotationValue);
		field.modifiers().add(annotation);
		
		// set visibility modifiers
		field.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		
		// add initialisation section
		ClassInstanceCreation initializer = ast.newClassInstanceCreation();
		initializer.setType(ast.newSimpleType(ast.newName(
				nameOf(fieldType, fieldTypeImported))));
		variable.setInitializer(initializer);
		
		// wrap with domain-specific types
		ParsedFieldDeclaration parsedFieldDeclaration = new ParsedFieldDeclaration(typeContext, field);
		if (parsedFieldDeclaration.getFields().isEmpty()) {
			throw new AssertionError("Internal logic error: expected 1 field, got none");
		}
		else if (parsedFieldDeclaration.getFields().size() > 1) {
			throw new AssertionError("Internal logic error: expected 1 field, got "+parsedFieldDeclaration.getFields().size());
		}
		return parsedFieldDeclaration.getFields().get(0);
	}
	
	/** Fully qualified or simple name, depending on whether imported */
	private static String nameOf(Class<?> type, boolean imported) {
		return imported ? type.getSimpleName() : type.getName();
	}
	
	/**
	 * Construct new instances for each field variable declared by the field declaration.
	 * @param typeContext
	 * @param astNode
	 * @return
	 */
	public static List<ParsedField> of(ParsedTypeContext typeContext, FieldDeclaration astNode) {
		return new ParsedFieldDeclaration(typeContext, astNode).getFields();
	}

	/**
	 * Used internally only for a single field variable.
	 * @param parsedFieldDeclaration
	 * @param name
	 */
	private ParsedField(ParsedFieldDeclaration parsedFieldDeclaration, VariableDeclarationFragment variableDeclaration) {
		this.parsedFieldDeclaration = parsedFieldDeclaration;
		this.name = variableDeclaration.getName().getFullyQualifiedName();
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (ParsedAnnotation annotation: getAnnotations()) {
			buf.append(annotation).append("\n");
		}
		buf.append("field "+getName());
		buf.append(";");
		if (isDBTableColumn()) {
			buf.append(" // columnName="+getColumnNameIfSet());
		}
		return buf.toString();
	}
	
	public FieldDeclaration astNode() {
		return parsedFieldDeclaration.astNode();
	}
	
	public ParsedTypeRef getType() {
		return parsedFieldDeclaration.getType();
	}
	
	/**
	 * Gets all types that are referenced by the field declaration.
	 * For simple types, this is one value.
	 * For types with generics, this is one value plus one for each
	 * generic parameter.
	 * For recursive arrays, this can be any number of values.
	 * The resultant list can be used for constructing imports etc.
	 * @return
	 */
	public List<Class<?>> getReferencedTypes() {
		return getType().getReferencedTypes();
	}
	
	public String getName() {
		return name;
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return parsedFieldDeclaration.getAnnotations();
	}
	
	/**
	 * Indicates whether this annotation is {@link nz.co.gregs.dbvolution.annotations.DBTableColumn}.
	 */
	public boolean isDBTableColumn() {
		return parsedFieldDeclaration.isDBTableColumn();
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the field name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBTableColumn()) {
				String columnName = annotation.getColumnNameIfSet();
				if (columnName == null) {
					columnName = getName();
				}
				return columnName;
			}
		}
		return null;
	}

	private static String joinNamesOf(List<ParsedField> fields, String delimiter) {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (ParsedField field: fields) {
			if (!first) buf.append(delimiter);
			first = false;
			
			buf.append(field.getName());
		}
		return buf.toString();
	}
	
	private static String join(List<String> strings, String delimiter) {
		StringBuilder buf = new StringBuilder();
		boolean first = true;
		for (String str: strings) {
			if (!first) buf.append(delimiter);
			first = false;
			
			buf.append(str);
		}
		return buf.toString();
	}
	
	/**
	 * Models the actual field declaration within the source file that
	 * contains one or more variable declarations, and zero or more
	 * shared annotations.
	 */
	public static class ParsedFieldDeclaration {
		private ParsedTypeContext typeContext;
		private FieldDeclaration astNode;
		private ParsedTypeRef type;
		private List<ParsedAnnotation> annotations;
		private List<ParsedField> fields; // one or more variables within the field declaration
		
		public ParsedFieldDeclaration(ParsedTypeContext typeContext, FieldDeclaration astNode) {
			this.typeContext = typeContext;
			this.astNode = astNode;
			
			// field type
			this.type = new ParsedTypeRef(typeContext, astNode.getType());

	    	// field annotations
			this.annotations = new ArrayList<ParsedAnnotation>();
	    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
	    		if (modifier.isAnnotation()) {
	    			annotations.add(new ParsedAnnotation(typeContext, (Annotation)modifier));
	    		}
	    	}		
			
			// field names
			this.fields = new ArrayList<ParsedField>();
	    	for (VariableDeclarationFragment variable: (List<VariableDeclarationFragment>)astNode.fragments()) {
	    		fields.add(new ParsedField(this, variable));
	    	}
		}
		
		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			for (ParsedAnnotation annotation: getAnnotations()) {
				buf.append(annotation).append("\n");
			}
			buf.append("field "+joinNamesOf(getFields(), ", "));
			buf.append(";");
			if (isDBTableColumn()) {
				buf.append(" // columnNames="+join(getColumnNamesIfSet(),","));
			}
			return buf.toString();
		}
		
		public FieldDeclaration astNode() {
			return astNode;
		}
		
		public ParsedTypeRef getType() {
			return type;
		}
		
		public List<ParsedAnnotation> getAnnotations() {
			return annotations;
		}
		
		public List<ParsedField> getFields() {
			return fields;
		}
		
		/**
		 * Indicates whether this annotation is {@link nz.co.gregs.dbvolution.annotations.DBTableColumn}.
		 */
		public boolean isDBTableColumn() {
			for (ParsedAnnotation annotation: getAnnotations()) {
				if (annotation.isDBTableColumn()) {
					return true;
				}
			}
			return false;
		}
		
		/**
		 * Gets the table names, as specified via the {@code DBTableColumn} annotation
		 * or defaulted based on the field names, if it has a {@code DBTableColumn}
		 * annotation.
		 * Really just here for use by the {@link #toString()} method.
		 * @return {@code null} if not applicable
		 */
		public List<String> getColumnNamesIfSet() {
			Set<String> uniqueNames = new LinkedHashSet<String>(); // retains order
			for (ParsedField field: fields) {
				String name = field.getColumnNameIfSet();
				if (name != null) {
					uniqueNames.add(name);
				}
			}
			return new ArrayList<String>(uniqueNames);
		}
	}
}
