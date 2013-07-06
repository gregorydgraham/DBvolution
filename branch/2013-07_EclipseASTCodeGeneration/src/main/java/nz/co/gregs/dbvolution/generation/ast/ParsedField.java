package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;

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

public class ParsedField {
	private ParsedTypeContext typeContext;
	private FieldDeclaration astNode;
	private List<String> names; // supports multiple variables on same field declaration
	private List<ParsedAnnotation> annotations;
	
	// TODO: apply logic to infer field name
	// TODO: ensure don't duplicate field names
	public static ParsedField newDBTableColumnInstance(ParsedTypeContext typeContext, boolean isPrimaryKey, String tableName, Class<?> fieldType) {
		AST ast = typeContext.getAST();
		
		String fieldName = DBTableClassGenerator.toFieldCase(tableName);
		
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
		StringLiteral annotationName = ast.newStringLiteral();
		annotationName.setLiteralValue(tableName);
		SingleMemberAnnotation annotation = ast.newSingleMemberAnnotation();
		annotation.setTypeName(ast.newSimpleName(
					nameOf(DBTableColumn.class, dbTableColumnImported)));
		annotation.setValue(annotationName);
		field.modifiers().add(annotation);
		
		// set visibility modifiers
		field.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		
		// add initialisation section
		ClassInstanceCreation initializer = ast.newClassInstanceCreation();
		initializer.setType(ast.newSimpleType(ast.newName(
				nameOf(fieldType, fieldTypeImported))));
		variable.setInitializer(initializer);

		return new ParsedField(typeContext, field);
	}
	
	/** Fully qualified or simple name, depending on whether imported */
	private static String nameOf(Class<?> type, boolean imported) {
		return imported ? type.getSimpleName() : type.getName();
	}
	
	public ParsedField(ParsedTypeContext typeContext, FieldDeclaration astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;

		// field names
		this.names = new ArrayList<String>();
    	for (VariableDeclarationFragment variable: (List<VariableDeclarationFragment>)astNode.fragments()) {
    		names.add(variable.getName().getFullyQualifiedName());
    	}
		
    	// field annotations
		this.annotations = new ArrayList<ParsedAnnotation>();
    	for(IExtendedModifier modifier: (List<IExtendedModifier>)astNode.modifiers()) {
    		if (modifier.isAnnotation()) {
    			annotations.add(new ParsedAnnotation(typeContext, (Annotation)modifier));
    		}
    	}		
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (ParsedAnnotation annotation: annotations) {
			buf.append(annotation).append("\n");
		}
		buf.append("field "+join(getNames(), ", "));
		buf.append(";");
		if (isDBTableColumn()) {
			buf.append(" // columnName="+getColumnNameIfSet());
		}
		return buf.toString();
	}
	
	public FieldDeclaration astNode() {
		return astNode;
	}
	
	public List<String> getNames() {
		return names;
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return annotations;
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
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the method name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBTableColumn()) {
				String columnName = annotation.getColumnNameIfSet();
				if (columnName == null) {
					columnName = getNames().get(0);
				}
				return columnName;
			}
		}
		return null;
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
}
