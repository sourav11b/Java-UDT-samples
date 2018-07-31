package com.nyp.dxc.samples.codec;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.nyp.dxc.samples.model.Address;

public class AddressCodec extends TypeCodec<Address> {

    private static final Object NULL = null;

	private final TypeCodec<UDTValue> innerCodec;

    private final UserType userType;

    public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
        this.userType = (UserType)innerCodec.getCqlType();
    }

    @Override
    public ByteBuffer serialize(Address value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(toUDTValue(value), protocolVersion);
    }

    @Override
    public Address deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return toAddress(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public Address parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(NULL) ? null : toAddress(innerCodec.parse(value));
    }

    @Override
    public String format(Address value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(toUDTValue(value));
    }

    protected Address toAddress(UDTValue value) {
        return value == null ? null : new Address(
            value.getString("street"), 
            value.getInt("zipcode")
        );
    }

    protected UDTValue toUDTValue(Address value) {
        return value == null ? null : userType.newValue()
            .setString("street", value.getStreet())
            .setInt("zipcode", value.getZipcode());
    }
}