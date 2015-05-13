package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 * Internal class. Do not use.
 *
 * Used internally to bridge between packages. Makes it possible to hide
 * internal methods on the QueryableDatatype so that they don't pollute the API
 * or JavaDocs, while still providing access to the internal methods from other
 * packages within DBvolution.
 *
 * For example QueryableDatatype.setPropertyWrapper() is set to package-private,
 * so the only way of calling it from other packages is via this class. If
 * QueryableDatatype.setPropertyWrapper() was public, then this class wouldn't
 * be needed, but it would pollute the public API.
 */
public class InternalQueryableDatatypeProxy {

	private final QueryableDatatype qdt;

	/**
	 * Internal class, do not use.
	 *
	 * @param qdt	qdt
	 */
	public InternalQueryableDatatypeProxy(QueryableDatatype qdt) {
		this.qdt = qdt;
	}

	/**
	 * Internal class, do not use.
	 * <p>
	 * Injects the PropertyWrapper into the QDT.
	 * <p>
	 * For use with QDT types that need meta-data only available via property
	 * wrappers.
	 *
	 * @param propertyWrapperDefn	propertyWrapperDefn
	 */
	public void setPropertyWrapper(PropertyWrapperDefinition propertyWrapperDefn) {
		qdt.setPropertyWrapper(propertyWrapperDefn);
	}

	/**
	 * Internal class, do not use.
	 * <p>
	 * Hides the generic setValue(Object) method within QueryableDatatype while
	 * allowing it to be used.
	 *
	 * @param obj	obj
	 */
	public void setValue(Object obj) {
		qdt.setValue(obj);
	}
}
