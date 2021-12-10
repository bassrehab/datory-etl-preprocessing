package com.subhadipmitra.datory.preprocessing.bridge.python;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class DatoryPythonFactory {
	private PyObject clazz;

	public DatoryPythonFactory() {
		PythonInterpreter interpreter = new PythonInterpreter();
		interpreter.exec("from DatoryPython import DatoryPython");
		clazz = interpreter.get("DatoryPython");
	}

	public DatoryPythonType create() {
		return (DatoryPythonType) clazz.__call__().__tojava__(DatoryPythonType.class);
	}
}
