package nz.co.gregs.dbvolution.datatypes;

/**
 * Syncs between a simple-type external value
 * and a QDT internal value.
 * @author Malcolm Lett
 */
public class SimpleValueQueryableDatatypeSyncer extends QueryableDatatypeSyncer {

	/**
	 * 
	 * @param propertyName used in error messages
	 * @param internalQdtType
	 * @param typeAdaptor
	 */
	public SimpleValueQueryableDatatypeSyncer(String propertyName, Class<? extends QueryableDatatype> internalQdtType,
			DBTypeAdaptor<Object, Object> typeAdaptor) {
		super(propertyName, internalQdtType, typeAdaptor);
	}

	public void setInternalFromExternalSimpleValue(Object externalValue) {
		Object internalValue = adaptValueFromExternal(externalValue);
		internalQdt.setValue(internalValue);
	}

	/**
	 * Note: directly returning the value from the type adaptor,
	 * without casting to the specific type expected by the target
	 * java property.
	 * @return
	 */
	// TODO: need to decide where type casting is needed
	public Object getExternalSimpleValueFromInternal() {
		return adaptValueFromInternal(internalQdt.literalValue);
	}
}
