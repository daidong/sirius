/**
 * Autogenerated by Thrift Compiler (1.0.0-dev)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.ttu.discl.iogp.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (1.0.0-dev)", date = "2016-7-30")
public class EpochEntity implements org.apache.thrift.TBase<EpochEntity, EpochEntity._Fields>, java.io.Serializable, Cloneable, Comparable<EpochEntity> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("EpochEntity");

  private static final org.apache.thrift.protocol.TField SERVER_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("serverId", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField EPOCH_FIELD_DESC = new org.apache.thrift.protocol.TField("epoch", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new EpochEntityStandardSchemeFactory());
    schemes.put(TupleScheme.class, new EpochEntityTupleSchemeFactory());
  }

  public int serverId; // required
  public int epoch; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SERVER_ID((short)1, "serverId"),
    EPOCH((short)2, "epoch");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
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
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SERVERID_ISSET_ID = 0;
  private static final int __EPOCH_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SERVER_ID, new org.apache.thrift.meta_data.FieldMetaData("serverId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.EPOCH, new org.apache.thrift.meta_data.FieldMetaData("epoch", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
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
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SERVERID_ISSET_ID);
  }

  /** Returns true if field serverId is set (has been assigned a value) and false otherwise */
  public boolean isSetServerId() {
    return EncodingUtils.testBit(__isset_bitfield, __SERVERID_ISSET_ID);
  }

  public void setServerIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SERVERID_ISSET_ID, value);
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
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __EPOCH_ISSET_ID);
  }

  /** Returns true if field epoch is set (has been assigned a value) and false otherwise */
  public boolean isSetEpoch() {
    return EncodingUtils.testBit(__isset_bitfield, __EPOCH_ISSET_ID);
  }

  public void setEpochIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __EPOCH_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SERVER_ID:
      if (value == null) {
        unsetServerId();
      } else {
        setServerId((Integer)value);
      }
      break;

    case EPOCH:
      if (value == null) {
        unsetEpoch();
      } else {
        setEpoch((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SERVER_ID:
      return Integer.valueOf(getServerId());

    case EPOCH:
      return Integer.valueOf(getEpoch());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SERVER_ID:
      return isSetServerId();
    case EPOCH:
      return isSetEpoch();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof EpochEntity)
      return this.equals((EpochEntity)that);
    return false;
  }

  public boolean equals(EpochEntity that) {
    if (that == null)
      return false;

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
    List<Object> list = new ArrayList<Object>();

    boolean present_serverId = true;
    list.add(present_serverId);
    if (present_serverId)
      list.add(serverId);

    boolean present_epoch = true;
    list.add(present_epoch);
    if (present_epoch)
      list.add(epoch);

    return list.hashCode();
  }

  @Override
  public int compareTo(EpochEntity other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetServerId()).compareTo(other.isSetServerId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetServerId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serverId, other.serverId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEpoch()).compareTo(other.isSetEpoch());
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
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("EpochEntity(");
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

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class EpochEntityStandardSchemeFactory implements SchemeFactory {
    public EpochEntityStandardScheme getScheme() {
      return new EpochEntityStandardScheme();
    }
  }

  private static class EpochEntityStandardScheme extends StandardScheme<EpochEntity> {

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

  private static class EpochEntityTupleSchemeFactory implements SchemeFactory {
    public EpochEntityTupleScheme getScheme() {
      return new EpochEntityTupleScheme();
    }
  }

  private static class EpochEntityTupleScheme extends TupleScheme<EpochEntity> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, EpochEntity struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.serverId);
      oprot.writeI32(struct.epoch);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, EpochEntity struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.serverId = iprot.readI32();
      struct.setServerIdIsSet(true);
      struct.epoch = iprot.readI32();
      struct.setEpochIsSet(true);
    }
  }

}

