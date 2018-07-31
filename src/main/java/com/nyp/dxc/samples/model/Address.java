package com.nyp.dxc.samples.model;

public class Address {

    @Override
	public String toString() {
		return "Address [street=" + street + ", zipcode=" + zipcode + "]";
	}

	private final String street;

    private final int zipcode;

    public Address(String street, int zipcode) {
        this.street = street;
        this.zipcode = zipcode;
    }

	public String getStreet() {
		return street;
	}

	public int getZipcode() {
		return zipcode;
	}

    
    
    // getters, setters, equals() and hashcode() omitted
}