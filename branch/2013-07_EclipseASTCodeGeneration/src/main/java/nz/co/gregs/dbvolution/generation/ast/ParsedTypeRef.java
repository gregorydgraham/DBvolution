package nz.co.gregs.dbvolution.generation.ast;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ArrayType;
import org.eclipse.jdt.core.dom.Name;
import org.eclipse.jdt.core.dom.ParameterizedType;
import org.eclipse.jdt.core.dom.PrimitiveType;
import org.eclipse.jdt.core.dom.PrimitiveType.Code;
import org.eclipse.jdt.core.dom.QualifiedType;
import org.eclipse.jdt.core.dom.SimpleType;
import org.eclipse.jdt.core.dom.Type;
import org.eclipse.jdt.core.dom.WildcardType;

/**
 * A reference to a type.
 * Models the type of a field, getter/setter method, type passed
 * to an annotation, or an annotation type itself.
 */
public class ParsedTypeRef {
	private static final Class<?> UNRECOGNISED_JAVA_TYPE = ParsedTypeRef.class; // marker value
	private ParsedTypeContext typeContext;
	private Type astNode;
	private Class<?> javaType = null; // only if available

	/**
	 * Constructs a new type reference that is guaranteed to be imported by the context.
	 * @param typeContext
	 * @param javaType
	 * @return
	 */
	public static ParsedTypeRef newClassInstance(ParsedTypeContext typeContext, Class<?> javaType) {
		AST ast = typeContext.getAST();
		
		boolean fieldTypeImported = typeContext.ensureImport(javaType);
		Type type = ast.newSimpleType(ast.newName(nameOf(javaType, fieldTypeImported)));
		return new ParsedTypeRef(typeContext, type);
	}
	
	/** Fully qualified or simple name, depending on whether imported */
	private static String nameOf(Class<?> type, boolean imported) {
		return imported ? type.getSimpleName() : type.getName();
	}
	public ParsedTypeRef(ParsedTypeContext typeContext, Type astNode) {
		this.typeContext = typeContext;
		this.astNode = astNode;
	}
	
	@Override
	public String toString() {
		return astNode.toString();
	}

	/**
	 * Hashcode based on string representation of referenced type.
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/**
	 * Equality based on string representation of referenced type.
	 * Only an approximate concept of equality that is suitable for use within
	 * the same source file.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ParsedTypeRef)) {
			return false;
		}
		ParsedTypeRef other = (ParsedTypeRef) obj;
		return this.toString().equals(other.toString());
	}

	public Type astNode() {
		return astNode;
	}
	
	public Name nameAstNode() {
		return ((SimpleType) astNode).getName();
	}
	
	/**
	 * Gets the java type, if recognised.
	 * @return the type, if recognised, {@code null} otherwise.
	 */
	public Class<?> getJavaTypeIfKnown() {
		if (javaType == null) {
			// TODO: calculate its value, use UNRECOGNISED_JAVA_TYPE if don't recognise it
		}
		return (javaType == UNRECOGNISED_JAVA_TYPE) ? null : javaType;
	}

	/**
	 * Gets all types that are referenced by the field declaration.
	 * For simple types, this is one value.
	 * For types with generics, this is one value plus one for each
	 * generic parameter.
	 * For recursive arrays, this can be any number of values.
	 * The resultant list can be used for constructing imports etc.
	 * @deprecated not working yet
	 * @return
	 */
	@Deprecated
	public List<Class<?>> getReferencedTypes() {
		return javaTypesOf(astNode);
	}
	
	// TODO: this will never actually do anything useful because the handling 
	// of SimpleType is relied upon by every other type.
	private List<Class<?>> javaTypesOf(Type type) {
		if (type.isArrayType()) {
			return javaTypesOf(((ArrayType) type).getComponentType());
		}
		else if (type.isParameterizedType()) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			List<Class<?>> javaTypes = javaTypesOf(parameterizedType.getType());
			for (Type typeArg: (List<Type>) parameterizedType.typeArguments()) {
				javaTypes.addAll(javaTypesOf(typeArg));
			}
			return javaTypes;
		}
		else if (type.isPrimitiveType()) {
			Class<?> clazz = null;
			Code code = ((PrimitiveType) type).getPrimitiveTypeCode();
			if (code.equals(PrimitiveType.BOOLEAN)) {
				clazz = Boolean.class;
			}
			else if (code.equals(PrimitiveType.BYTE)) {
				clazz = Byte.class;
			}
			else if (code.equals(PrimitiveType.CHAR)) {
				clazz = Character.class;
			}
			else if (code.equals(PrimitiveType.DOUBLE)) {
				clazz = Double.class;
			}
			else if (code.equals(PrimitiveType.FLOAT)) {
				clazz = Float.class;
			}
			else if (code.equals(PrimitiveType.INT)) {
				clazz = Integer.class;
			}
			else if (code.equals(PrimitiveType.LONG)) {
				clazz = Long.class;
			}
			else if (code.equals(PrimitiveType.SHORT)) {
				clazz = Short.class;
			}
			else if (code.equals(PrimitiveType.VOID)) {
				clazz = Void.class;
			}
			else {
				throw new UnsupportedOperationException("Unrecognised PrimitiveType.Code: "+code);
			}
			List<Class<?>> javaTypes = new ArrayList<Class<?>>();
			javaTypes.add(clazz);
			return javaTypes;
		}
		else if (type.isQualifiedType()) {
			QualifiedType qualifiedType = (QualifiedType) type;
			List<Class<?>> javaTypes = javaTypesOf(qualifiedType.getQualifier());
			// not worrying about the qualified name (qualifiedType.getName()) for now
			return javaTypes;
		}
		else if (type.isSimpleType()) {
			SimpleType simpleType = (SimpleType) type;
			Name typeName = simpleType.getName();
			// TODO: need to do something intelligent with this, but what?
			// Can qualify the name via the imports, but I can't turn it into a Class<?> instance because
			// there's no reason that it has to be in my classpath.
			return new ArrayList<Class<?>>();
		}
		else if (type.isUnionType()) {
			throw new IllegalArgumentException("Union types not expected here");
		}
		else if (type.isWildcardType()) {
			WildcardType wildcard = (WildcardType) type;
			if (wildcard.getBound() != null) {
				return javaTypesOf(wildcard.getBound());
			}
			else {
				return new ArrayList<Class<?>>();
			}
		}
		else {
			throw new UnsupportedOperationException("Unrecognised Type type: "+type.getClass().getSimpleName()); 
		}
	}
}
