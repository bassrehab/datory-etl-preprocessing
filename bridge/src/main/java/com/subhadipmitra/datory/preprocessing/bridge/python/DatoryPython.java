package com.subhadipmitra.datory.preprocessing.bridge.python;

public class DatoryPython {
	private DatoryPythonType object;

	public DatoryPython() {
		object = new DatoryPythonFactory().create();
	}

	public void hello(String name) {
		object.hello("Datory Python");
	}

	public static void main(String[] args) {
		new DatoryPython().hello("Datory Python");
	}
}
