package com.nyp.dxc.samples;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.nyp.dxc.samples.codec.AddressCodec;
import com.nyp.dxc.samples.model.Address;

public class InsertWithUDT {

	public static void main(String[] args) {
		CodecRegistry codecRegistry = new CodecRegistry();

		DseSession session = createConnection(codecRegistry);
		UserType addressType = session.getCluster().getMetadata().getKeyspace("spark_demo").getUserType("address");
		UUID uuid = insertUDT(session, addressType);
		updateUDT(session, uuid, addressType);

		TypeCodec<UDTValue> addressTypeCodec = codecRegistry.codecFor(addressType);
		AddressCodec addressCodec = new AddressCodec(addressTypeCodec, Address.class);
		codecRegistry.register(addressCodec);
		readUDT(session, uuid);
		insertUDTWithCodec(session);
		
		
		session.close();
		System.exit(0);

	}

	private static UUID insertUDT(DseSession session, UserType addressType) {

		PreparedStatement statement = session
				.prepare("INSERT INTO spark_demo.users (id, name, addresses) VALUES (?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);

		UUID uuid = UUID.randomUUID();

		UDTValue address = addressType.newValue().setString("street", "1 long st").setInt("zipcode", 2000);
		Set<UDTValue> addresses = new HashSet<UDTValue>();
		addresses.add(address);

		session.execute(boundStatement.bind(uuid, "sample name", addresses));

		System.out.println("Inserted records");

		return uuid;
	}
	
	private static UUID insertUDTWithCodec(DseSession session) {

		PreparedStatement statement = session
				.prepare("INSERT INTO spark_demo.users (id, name, addresses) VALUES (?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);

		UUID uuid = UUID.randomUUID();
		
		Address address= new Address("codec address", 5000);

		Set<Address> addresses = new HashSet<Address>();
		addresses.add(address);

		session.execute(boundStatement.bind(uuid, "sample name", addresses));

		System.out.println("Inserted records with coded : "+uuid);

		return uuid;
	}

	private static void updateUDT(DseSession session, UUID uuid, UserType addressType) {

		PreparedStatement statement = session
				.prepare("update spark_demo.users set addresses = addresses + ? where id = ?");
		UDTValue address = addressType.newValue().setString("street", "2 short st").setInt("zipcode", 3000);
		Set<UDTValue> addresses = new HashSet<UDTValue>();
		addresses.add(address);
		BoundStatement boundStatement = new BoundStatement(statement);

		session.execute(boundStatement.bind(addresses, uuid));

		System.out.println("added address");
	}

	private static void readUDT(DseSession session, UUID uuid) {

		PreparedStatement statement = session.prepare("select * from spark_demo.users where id = ?");

		BoundStatement boundStatement = new BoundStatement(statement);

		ResultSet results = session.execute(boundStatement.bind(uuid));

		Set<Address> addresses = results.one().getSet("addresses", Address.class);

		System.out.println("read addresses : " + addresses);
	}

	private static DseSession createConnection(CodecRegistry codecRegistry) {
		DseCluster cluster = null;

		AuthProvider authProvider = new DsePlainTextAuthProvider("cassandra", "cassandra");

		// set pooling , ssl, load balancing policy in production as apaplicable
		cluster = DseCluster.builder()
				.addContactPoints(new String[] { "13.57.200.192", "13.56.252.218", "52.8.95.139" })
				.withAuthProvider(authProvider).withCodecRegistry(codecRegistry)

				.build();
		System.out.println("The keyspaces known by Connection are: " + cluster.getMetadata().getAllHosts().toString());

		// you can get lots of meta data, the below shows the keyspaces it can find out
		// about
		// this is all part of the client gossip like query process
		System.out.println("The keyspaces known by Connection are: " + cluster.getMetadata().getKeyspaces().toString());

		// you don't have to specify a consistency level, there is always default
		System.out.println("The Default Consistency Level is: "
				+ cluster.getConfiguration().getQueryOptions().getConsistencyLevel());

		// finally create a session to connect, alternatively and what you normally will
		// do is specify the keyspace
		// i.e. DseSession session = cluster.connect("keyspace_name");
		DseSession session = cluster.connect();

		return session;

	}

}
