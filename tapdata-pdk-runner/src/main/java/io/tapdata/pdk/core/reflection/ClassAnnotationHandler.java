package io.tapdata.pdk.core.reflection;

import io.tapdata.pdk.core.error.CoreException;

import java.lang.annotation.Annotation;
import java.util.Set;

public abstract class ClassAnnotationHandler  {
	public abstract void handle(Set<Class<?>> classes) throws CoreException;
	public Object getKey() {
		return this.getClass();
	}

	public abstract Class<? extends Annotation> watchAnnotation();
}
