package cs4224.project.cassandra.models;

import java.nio.ByteBuffer;
import java.util.HashMap;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class OrderlineCodec extends TypeCodec<Orderline> {
	private final TypeCodec<UDTValue> innerCodec;
	private final UserType userType;

	public OrderlineCodec(DataType cqlType, Class<Orderline> javaClass) {
		super(cqlType, javaClass);
		innerCodec = null;
		userType = null;

	}

	public OrderlineCodec(TypeCodec<UDTValue> innerCodec, Class<Orderline> javaType) {
		super(innerCodec.getCqlType(), javaType);
		this.innerCodec = innerCodec;
		this.userType = (UserType) innerCodec.getCqlType();
	}

	@Override
	public ByteBuffer serialize(Orderline value, ProtocolVersion protocolVersion) throws InvalidTypeException {
//		System.out.println("[OrderlineCodec] serial:" + value);
		return innerCodec.serialize(toUDTValue(value), protocolVersion);
	}

	@Override
	public Orderline deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
//		System.out.println("[OrderlineCodec] deserial: " + bytes);
		return toOrderline(innerCodec.deserialize(bytes, protocolVersion));
	}

	@Override
	public Orderline parse(String value) throws InvalidTypeException {
		System.out.println("[OrderlineCodec] parse: " + value);
		if( value == null || value.isEmpty()) {
			return null;
		} else {
			return toOrderline(innerCodec.parse(value));
		}
	}

	@Override
	public String format(Orderline value) throws InvalidTypeException {
		return value == null ? null : innerCodec.format(toUDTValue(value));
	}
	
	protected Orderline toOrderline(UDTValue value) {
        return value == null ? null : new Orderline(
                value.getInt("ol_i_id"), 
                value.getDouble("ol_amount")
            );
        }

//	protected Orderline toOrderline(String value) {
//		if (value == null) {
//			return null;
//		} else {
//			HashMap<String, String> fields = new HashMap<>();
//			String[] field_strings = value.split(",");
//			for (String field_string : field_strings) {
//				String[] parsed = field_string.split(":");
//				fields.put(parsed[0], parsed[1]);
//			}
//			String ol_i_id_str = fields.get("ol_i_id");
//			String ol_amount_str = fields.get("ol_amount");
//			return new Orderline(Integer.parseInt(ol_i_id_str), Double.parseDouble(ol_amount_str));
//		}
//	}
	
	 protected UDTValue toUDTValue(Orderline value) {
	        return value == null ? null : userType.newValue()
	            .setInt("ol_i_id", value.getId())
	            .setDouble("ol_amount", value.getAmount());
	    }
}
