package io.symplify.sqs;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class Body<T> {

  public String type;
  public String contactId;
  public String mappingSelector;
  public T properties;

  public Body(){}
}
