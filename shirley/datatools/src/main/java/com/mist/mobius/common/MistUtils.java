package com.mist.mobius.common;


import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.log4j.Logger;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.TimeZone;
import java.util.UUID;


public class MistUtils {
    private static Logger logger = Logger.getLogger(MistUtils.class);

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }


    public static byte[] hexToBytes(String hexString) {
        HexBinaryAdapter adapter = new HexBinaryAdapter();
        byte[] bytes = adapter.unmarshal(hexString);
        return bytes;
    }

    /**
     * convert byte[16] to UUID
     *
     * @param bytes
     * @return
     */
    public static String getUUID(byte[] bytes) {
        if (bytes.length == 0 || bytes.length != 16) {
            // logger.warn(String.format("the UUID byte array size %d is not 16", bytes.length));
            return "";
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(high, low);
        bb.clear();
        return uuid.toString();
    }

    public static String getUUID(ByteString bytes) {
        return getUUID(bytes.toByteArray());
    }

    public static String getUUID(ByteString bytes, String def) {
        try {
            String uuidString = getUUID(bytes.toByteArray());
            return uuidString.equals("") ? def : uuidString;
        } catch (Exception e) {
            return def;
        }
    }

    public static String getUUID(String str, String def) {
        try {
            String uuidString = getUUID(hexToBytes(str.replaceAll("-", "")));
            return uuidString.equals("") ? def : uuidString;
        } catch (Exception e) {
            return def;
        }
    }

    public static class IPPort {
        public String ip;
        public int port;
        public boolean isV6;

        public IPPort() {
            ip = "0.0.0.0";
            port = 0;
            isV6 = false;
        }

        public String toString() {
            if (isV6) {
                return String.format("[%s]:%d", ip, port);
            }
            return String.format("%s:%d", ip, port);
        }
    }

    // we assume there is only one ip address from argument bytes
    public static IPPort getIpPort(ByteString bytes) {
        IPPort ipport = new IPPort();
        // IPv6 if not 4+2
        if (bytes.size() != 6) {
            ipport.isV6 = true;
        }

        // 4+2 or 16+2 bytes: IPv4/IPv6 followed by big-endian port number
        try {
            // big-endian unsigned 2 bytes to int
            ipport.port = java.nio.ByteBuffer.wrap(bytes.substring(bytes.size() - 2).toByteArray()).getChar();
            ipport.ip = getIp(bytes.substring(0, bytes.size() - 2));
        } catch (Exception e) {
        }
        return ipport;
    }

    public static String getMacFromUUID(byte[] bytes) {
        if (bytes.length == 0 || bytes.length != 16) {
            // logger.warn(String.format("the UUID byte array size %d is not 16", bytes.length));
            return "";
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(high, low);
        bb.clear();
        String strUuid = uuid.toString();
        if (!strUuid.startsWith("00000000-0000-0000-1000-")) {
            return "";
        }
        String[] parts = strUuid.split("-");
        return parts[4];
    }

    /**
     * Convert UUID(AP): 00000000-0000-0000-1000-5c5b350eb92f to MAC(AP) 5c5b350eb92f (without colon/dash)
     *
     * @param uuid: String AP UUID
     * @return String AP MAC without colon/dash
     */
    public static String getMacFromUUID(String uuid) {
        if (uuid.length() == 0 || uuid.length() != 36) {
            return "";
        }
        if (!uuid.startsWith("00000000-0000-0000-1000-")) {
            return "";
        }
        return uuid.substring(24, 36);
    }

    /**
     * convert byte[6] to MAC, output is lower case hex with separator
     *
     * @param bytes
     * @param separator
     * @return
     */
    public static String getMAC(byte[] bytes, String separator, String def) {
        if (bytes == null || bytes.length != 6) {
            return def;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(String.format("%02x%s", bytes[i], (i < bytes.length - 1) ? separator : ""));
        }
        return sb.toString();
    }

    /**
     * convert byte[6] to UUID with separator "-"
     *
     * @param bytes
     * @return
     */
    public static String getMAC(byte[] bytes) {
        return getMAC(bytes, "-", "");
    }

    /**
     * convert byte[6] to UUID with separator "-"
     *
     * @param bytes
     * @return
     */
    public static String getMAC(byte[] bytes, String def) {
        return getMAC(bytes, "-", def);
    }

    /**
     * convert PB.ByteString to MAC
     *
     * @param bytes
     * @return
     */
    public static String getMAC(ByteString bytes) {
        return getMAC(bytes.toByteArray(), "-", "");
    }

    public static String getMACHex(ByteString bytes, String def) {
        return getMAC(bytes.toByteArray(), "", def);
    }

    public static String getMACHex(String mac, String def) {
        try {
            String macInHex = mac.replaceAll("[^A-Fa-f0-9]", "").toLowerCase();
            return macInHex.length() == 12 ? macInHex : def;
        } catch (Exception e) {
            return def;
        }
    }

    public static ByteString getMACByteString(String mac) {
        String macHex = mac.toLowerCase().replaceAll("[^a-f0-9]", "");
        int len = macHex.length();
        byte[] macBytes = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        if (len != 12) {
            return ByteString.copyFrom(macBytes);
        }
        for (int i = 0; i < 6; i++) {
            int idx = i * 2;
            int val = Integer.parseInt(macHex.substring(idx, idx + 2), 16);
            macBytes[i] = (byte) val;
        }
        return ByteString.copyFrom(macBytes);
    }

    /**
     * convert byte[4] to Ipv4, byte[16] to Ipv6 or empty to 0.0.0.0
     *
     * @param bytes
     * @return
     */
    public static String getIp(byte[] bytes) {
        if (bytes.length == 0) return "0.0.0.0";
        try {
            return InetAddress.getByAddress(bytes).getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static String getIp(ByteString bytes) {
        return getIp(bytes.toByteArray());
    }

    /**
     * convert byte[4] to Ipv4, byte[16] to Ipv6 or empty to 0.0.0.0 with /{mask}
     *
     * @param address
     * @param mask
     * @return
     */
    public static String getCIDR(byte[] address, int mask) {
        return String.format("%s/%d", getIp(address), mask);
    }

    /**
     * convert google.protobuf.Timestamp to double
     *
     * @param timestamp
     * @return
     */
    public static double getTimestamp(Timestamp timestamp) {
        return timestamp != null ? (timestamp.getSeconds() + (timestamp.getNanos() / 1000000) * .001) : 0;
    }

    /**
     * true 1
     * false 0
     *
     * @param b
     * @return
     */
    public static int booleanToInt(boolean b) {
        return (b ? 1 : 0);
    }

    /**
     * https://github.com/mistsys/mist-ap/blob/master/ep/ap/aph.go#L111
     *
     * @param band
     * @return
     */
    public static String getBand(int band) {
        if (band == 0) {
            return "24";
        } else if (band == 1) {
            return "5";
        } else if (band == 2) {
            return "4.9";
        } else if (band == 3) {
            return "2.4FH";
        } else {
            return "?";
        }
    }

    /**
     * Parses user specified fields like ssid, userAgent into UTF-8 String. If parsing fails, return ???? as default string.
     *
     * @param bytes
     * @return
     */
    public static String getParsedField(byte[] bytes) {
        String parsed = "????";
        try {
            parsed = new String(bytes, "UTF-8");
        } catch (Exception e) {
            logger.error("Can not parse user field to UTF-8 String: " + e.getMessage());
        }
        return parsed;
    }

    public static String getString(ByteString byteString, String def) {
        return getString(byteString, def, false);
    }

    /**
     * Option to validate UTF-8 encoding of the provided byte string before decoding them as UTF-8 string.
     *
     * @param byteString   byte string value
     * @param def          default/fallback string value
     * @param validateUTF8 boolean to enable/disable UTF-8 encoding validation
     * @return string
     */
    public static String getString(ByteString byteString, String def, boolean validateUTF8) {
        try {
            if (!validateUTF8)
                return new String(byteString.toByteArray(), "UTF-8");
            return byteString.isValidUtf8() ? new String(byteString.toByteArray(), "UTF-8") : def;
        } catch (Exception e) {
            logger.error("Can not parse user field to UTF-8 String: " + e.getMessage());
        }
        return def;
    }

    public static final String NONE_UUID = "00000000-0000-0000-0000-000000000000";

    public static boolean isNoneUUID(String uuid) {
        if (uuid == null || "".equals(uuid) || NONE_UUID.equals(uuid)) {
            return true;
        }
        return false;
    }

}
