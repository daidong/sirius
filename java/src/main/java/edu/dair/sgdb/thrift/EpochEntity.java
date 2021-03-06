/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.dair.sgdb.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)", date = "2018-10-18")
public class EpochEntity implements org.apache.thrift.TBase<EpochEntity, EpochEntity._Fields>, java.io.Serializable, Cloneable, Comparable<EpochEntity> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("EpochEntity");

  private static final org.apache.thrift.protocol.TField SERVER_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("serverId", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField EPOCH_FIELD_DESC = new org.apache.thrift.protocol.TField("epoch", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new EpochEntityStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new EpochEntityTupleSchemeFactory();

  public int serverId; // required
  public int epoch; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SERVER_ID((short)1, "serverId"),
    EPOCH((short)2, "epoch");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SERVER_ID
          return SERVER_ID;
        case 2: // EPOCH
          return EPOCH;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SERVERID_ISSET_ID = 0;
  private static final int __EPOCH_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SERVER_ID, new org.apache.thrift.meta_data.FieldMetaData("serverId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.EPOCH, new org.apache.thrift.meta_data.FieldMetaData("epoch", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(EpochEntity.class, metaDataMap);
  }

  public EpochEntity() {
  }

  public EpochEntity(
    int serverId,
    int epoch)
  {
    this();
    this.serverId = serverId;
    setServerIdIsSet(true);
    this.epoch = epoch;
    setEpochIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public EpochEntity(EpochEntity other) {
    __isset_bitfield = other.__isset_bitfield;
    this.serverId = other.serverId;
    this.epoch = other.epoch;
  }

  public EpochEntity deepCopy() {
    return new EpochEntity(this);
  }

  @Override
  public void clear() {
    setServerIdIsSet(false);
    this.serverId = 0;
    setEpochIsSet(false);
    this.epoch = 0;
  }

  public int getServerId() {
    return this.serverId;
  }

  public EpochEntity setServerId(int serverId) {
    this.serverId = serverId;
    setServerIdIsSet(true);
    return this;
  }

  public void unsetServerId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SERVERID_ISSET_ID);
  }

  /** Returns true if field serverId is set (has been assigned a value) and false otherwise */
  public boolean isSetServerId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SERVERID_ISSET_ID);
  }

  public void setServerIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SERVERID_ISSET_ID, value);
  }

  public int getEpoch() {
    return this.epoch;
  }

  public EpochEntity setEpoch(int epoch) {
    this.epoch = epoch;
    setEpochIsSet(true);
    return this;
  }

  public void unsetEpoch() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __EPOCH_ISSET_ID);
  }

  /** Returns true if field epoch is set (has been assigned a value) and false otherwise */
  public boolean isSetEpoch() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __EPOCH_ISSET_ID);
  }

  public void setEpochIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __EPOCH_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case SERVER_ID:
      if (value == null) {
        unsetServerId();
      } else {
        setServerId((java.lang.Integer)value);
      }
      break;

    case EPOCH:
      if (value == null) {
        unsetEpoch();
      } else {
        setEpoch((java.lang.Integer)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SERVER_ID:
      return getServerId();

    case EPOCH:
      return getEpoch();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case SERVER_ID:
      return isSetServerId();
    case EPOCH:
      return isSetEpoch();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof EpochEntity)
      return this.equals((EpochEntity)that);
    return false;
  }

  public boolean equals(EpochEntity that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_serverId = true;
    boolean that_present_serverId = true;
    if (this_present_serverId || that_present_serverId) {
      if (!(this_present_serverId && that_present_serverId))
        return false;
      if (this.serverId != that.serverId)
        return false;
    }

    boolean this_present_epoch = true;
    boolean that_present_epoch = true;
    if (this_present_epoch || that_present_epoch) {
      if (!(this_present_epoch && that_present_epoch))
        return false;
      if (this.epoch != that.epoch)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + serverId;

    hashCode = hashCode * 8191 + epoch;

    return hashCode;
  }

  @Override
  public int compareTo(EpochEntity other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetServerId()).compareTo(other.isSetServerId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetServerId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serverId, other.serverId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetEpoch()).compareTo(other.isSetEpoch());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEpoch()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.epoch, other.epoch);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("EpochEntity(");
    boolean first = true;

    sb.append("serverId:");
    sb.append(this.serverId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("epoch:");
    sb.append(this.epoch);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'serverId' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'epoch' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class EpochEntityStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public EpochEntityStandardScheme getScheme() {
      return new EpochEntityStandardScheme();
    }
  }

  private static class EpochEntityStandardScheme extends org.apache.thrift.scheme.StandardScheme<EpochEntity> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, EpochEntity struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SERVER_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.serverId = iprot.readI32();
              struct.setServerIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // EPOCH
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.epoch = iprot.readI32();
              struct.setEpochIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetServerId()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'serverId' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetEpoch()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'epoch' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, EpochEntity struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(SERVER_ID_FIELD_DESC);
      oprot.writeI32(struct.serverId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(EPOCH_FIELD_DESC);
      oprot.writeI32(struct.epoch);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class EpochEntityTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public EpochEntityTupleScheme getScheme() {
      return new EpochEntityTupleScheme();
    }
  }

  private static class EpochEntityTupleScheme extends org.apache.thrift.scheme.TupleScheme<EpochEntity> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, EpochEntity struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.serverId);
      oprot.writeI32(struct.epoch);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, EpochEntity struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.serverId = iprot.readI32();
      struct.setServerIdIsSet(true);
      struct.epoch = iprot.readI32();
      struct.setEpochIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

