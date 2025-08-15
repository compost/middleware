package io.symplify.sqs;

public class Body<T> {

  public String type;
  public String contactId;
  public String mappingSelector;
  public T properties;

  public Body(){}
}
