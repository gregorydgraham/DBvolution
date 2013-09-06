package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.Annotation;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.Modifier;
import org.eclipse.jdt.core.dom.ReturnStatement;
import org.eclipse.jdt.core.dom.SingleVariableDeclaration;
import org.eclipse.jdt.core.dom.TagElement;
import org.eclipse.jdt.core.dom.TextElement;
import org.eclipse.jdt.core.dom.Type;

/**
 * The parsed details of a member method within a class.
 * @author Malcolm Lett
 */
public class ParsedMethod {
	private ParsedTypeContext typeContext;
	private ParsedTypeRef returnType;
	private List<ParsedTypeRef> argumentTypes;
	private MethodDeclaration astNode;
	private List<ParsedAnnotation> annotations;
	
	/**
	 * Creates a standard getter method for the given field.
	 * @param typeContext
	 * @param field
	 * @return the new method, ready to be added to the java type
	 */
	public static ParsedMethod newFieldGetterInstance(ParsedTypeContext typeContext, ParsedField field) {
		AST ast = typeContext.getAST();
		
		String methodName = "get"+titleCaseOf(field.getName());
		
		// add imports: TODO
		// (field may not be actually defined in this class, so still need to check imports)
		//boolean fieldTypeImported = typeContext.ensureImport(field.); // TODO: need ParsedField.getType() to work for this
		
		// add method
		MethodDeclaration method = ast.newMethodDeclaration();
		method.setName(ast.newSimpleName(methodName));
		method.setReturnType2((Type) ASTNode.copySubtree(ast, field.astNode().getType()));
		
		// add body
		ReturnStatement returnStatement = ast.newReturnStatement();
		returnStatement.setExpression(ast.newSimpleName(field.getName()));
		method.setBody(ast.newBlock());
		method.getBody().statements().add(returnStatement);
		
		// set visibility modifiers
		method.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));
		
		// add javadoc
		TextElement javadocText = ast.newTextElement();
		javadocText.setText("the "+field.getName());
		TagElement javadocTag = ast.newTagElement();
		javadocTag.setTagName("@return");
		javadocTag.fragments().add(javadocText);
		method.setJavadoc(ast.newJavadoc());
		method.getJavadoc().tags().add(javadocTag);
		
		return new ParsedMethod(typeContext, method);
	}
	
	private static String titleCaseOf(String name) {
		return name.substring(0,1).toUpperCase() + name.substring(1);
	}
	
	
	public ParsedMethod(ParsedTypeContext typeContext, MethodDeclaration astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
		
		// return type
		this.returnType = new ParsedTypeRef(typeContext, astNode.getReturnType2());
		
		// argument types
		this.argumentTypes = new ArrayList<ParsedTypeRef>();
		for (SingleVariableDeclaration varDecl: (List<SingleVariableDeclaration>)astNode.parameters()) {
			argumentTypes.add(new ParsedTypeRef(typeContext, varDecl.getType()));
		}
		
    	// method annotations
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
		buf.append("method "+getName());
		buf.append("();");
		return buf.toString();
	}
	
	public MethodDeclaration astNode() {
		return astNode;
	}
	
	public ParsedTypeRef getReturnType() {
		return returnType;
	}
	
	public List<ParsedTypeRef> getArgumentTypes() {
		return argumentTypes;
	}
	
	public String getName() {
		return astNode.getName().getFullyQualifiedName();
	}
	
	public List<ParsedAnnotation> getAnnotations() {
		return annotations;
	}
	
	/**
	 * Indicates whether this method is declared with a
	 * {@link nz.co.gregs.dbvolution.annotations.DBColumn} annotation.
	 */
	public boolean isDBColumn() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBColumn()) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Gets the table name, as specified via the {@code DBTableColumn} annotation
	 * or defaulted based on the field name, if it has a {@code DBTableColumn}
	 * annotation.
	 * @return {@code null} if not applicable
	 */
	public String getColumnNameIfSet() {
		for (ParsedAnnotation annotation: getAnnotations()) {
			if (annotation.isDBColumn()) {
				String columnName = annotation.getColumnNameIfSet();
				if (columnName == null) {
					columnName = JavaRules.propertyNameOf(this); // default based on property name
				}
				return columnName;
			}
		}
		return null;
	}
}
