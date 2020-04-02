package com.mist;

import com.mist.mobius.generated.ClientStatsAnalytics;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

public class ClientRowIterator implements Iterator<Row> {

    List<ClientStatsAnalytics.ClientStatsAnalyticsOut> data;
    int index;
    ClientStatsAnalytics.ClientStatsAnalyticsOut prev;
    StructType schema;

    Comparator<ClientStatsAnalytics.ClientStatsAnalyticsOut> compareByTs = new Comparator<ClientStatsAnalytics.ClientStatsAnalyticsOut>() {
        public int compare(ClientStatsAnalytics.ClientStatsAnalyticsOut o1, ClientStatsAnalytics.ClientStatsAnalyticsOut o2) {
            return Long.valueOf(o1.getTerminatorTimestamp()).compareTo(Long.valueOf(o2.getTerminatorTimestamp()));
        }
    };

    public ClientRowIterator(Iterable<ClientStatsAnalytics.ClientStatsAnalyticsOut> inputs, StructType schema) {
        schema = schema;
        index = 0;
        prev = null;
        data = new ArrayList<ClientStatsAnalytics.ClientStatsAnalyticsOut>();
        for (ClientStatsAnalytics.ClientStatsAnalyticsOut clientStats: inputs) {
            data.add(clientStats);
        }
        Collections.sort(data, compareByTs);
    }

    public boolean hasNext() {
        return index < data.size() - 1;
    }

    public Row next() {

        ClientStatsAnalytics.ClientStatsAnalyticsOut current = data.get(index);

        Row row = null;
        try {
            if (prev == null) {
                prev = data.get(index);
                row = toRow(current, null);
            } else if ( prev.getUptime()  < current.getUptime()) {
                    row = toRow(current, prev);
            } else
                row = toRow(current, null);
        } catch (Exception e) {

        } finally {
            index++;
            prev = current;
        }

        return row;
    }


    private Row toRow(ClientStatsAnalytics.ClientStatsAnalyticsOutOrBuilder current, ClientStatsAnalytics.ClientStatsAnalyticsOutOrBuilder prev) throws IntrospectionException, InvocationTargetException, IllegalAccessException {

        Boolean delta = prev != null;
//
//        List<Object> values = new ArrayList<Object>();
//
//        schema.toList();
//
//        for (String fname: schema.fieldNames()) {
//            int index = schema.fieldIndex(fname);
//            Object f = new PropertyDescriptor(fname, ClientStatsAnalytics.ClientStatsAnalyticsOutOrBuilder.class).getReadMethod().invoke(current);
//
//            if (f == null ) {
//
//            }
//            values.set(index, f);
//        }
//        return RowFactory.create(values);

        StructType[] dd = {};
        return RowFactory.create(
                current.getApId(),
                current.getSiteId(),
                current.getOrgId(),
                current.getFirmwareVersion(),
                current.getModel(),
                current.getUptime(),  // 5
                current.getInterval(),
                current.getWhen(),
                dd,  // 8 radios
                current.getWlansList(),
                current.getAppSsList(),    // 10
                current.getTerminatorRemoteAddr(), // 11
                current.getTerminatorTimestamp(),  // 12
                current.getClientMac(),  // 13
                current.getClientWcid(),  // 14
                current.getClientRadioIndex(), // 15
                current.getClientBand(), // 16
                current.getClientBssid(), // 17
                current.getClientWlanId(), // 18
                current.getClientWlanSsid(),
                current.getClientConnectedTimeSec(), // 20
                current.getClientInactiveTimeMilliSec(),
                current.getClientRssi(),
                current.getClientPerAntennaRssiList().toArray(),
                current.getClientAvgRssi(),
                current.getClientTxBitRate(),    // 25
                current.getClientTxUnicastBytes(),
                prev != null ? safeSetInteger(current.getClientTxUnicastPkts(), prev.getClientTxUnicastPkts()) : current.getClientTxUnicastPkts(),
                prev != null ? safeSetInteger(current.getClientTxPktsSent(), prev.getClientTxPktsSent()) : current.getClientTxPktsSent(),
                prev != null ? safeSetInteger(current.getClientTxRetries(), prev.getClientTxRetries()) : current.getClientTxRetries(),
                prev != null ? safeSetInteger(current.getClientTxRetried(), prev.getClientTxRetried()) : current.getClientTxRetried(),  //30
                prev != null ? safeSetInteger(current.getClientTxFailed(), prev.getClientTxFailed()) : current.getClientTxFailed(),
                current.getClientTxbps(),
                current.getClientTxpps(),
                prev != null ? safeSetInteger(current.getClientTxRetryExhausted(), prev.getClientTxRetryExhausted()) : current.getClientTxRetryExhausted(),
                current.getClientTxRateFallBack(),   //35
                current.getClientRxBitRate(),
                prev != null ? safeSetLong(current.getClientRxBytes(), prev.getClientRxBytes()) : current.getClientRxBytes(),
                prev != null ? safeSetInteger(current.getClientRxPkts(), prev.getClientRxPkts()) : current.getClientRxPkts(),
                current.getClientRxMcastPkts(),
                current.getClientRxMcastBytes(),  //40
                prev != null ? safeSetInteger(current.getClientRxRetried(), prev.getClientRxRetried()) : current.getClientRxRetried(),
                prev != null ? safeSetInteger(current.getClientRxRetries(), prev.getClientRxRetries()) : current.getClientRxRetries(),
                prev != null ? safeSetInteger(current.getClientRxDups(), prev.getClientRxDups()) : current.getClientRxDups(),
                current.getClientRxDecryptFailures(),
                current.getClientRxbps(),   // 45
                current.getClientRxpps(),
                current.getClientRxProbeReqs24(),
                current.getClientRxProbeReqs5(),
                current.getClientPqRequested(),
                current.getClientPqDropped(),  //50
                current.getClientPqRetried(),
                current.getClientPqRtsfail(),
                current.getClientPqThroughput(),
                current.getClientPqAiruse(),
                current.getClientPqmaxUsed(),  //55
                current.getClientPqLength(),
                null, // client_tx_delay
                current.getClientIpv4(),
                current.getClientVlanId(),
                current.getClientUsername(),  //60
                null, // client_cckrates
                null, // client_ofdmrates
                null, // client_mcsrates
                current.getClientProtocol(),
                current.getClientNumStreams(),  // 65
                null, // client_multi_pskname
                current.getCloudLastRtt(),
                current.getCloudWhenLastRtt(),
                current.getClientCount(),
                delta,     // 70
                "dummy",
                0,
                current.getIsActive(),
                current.getHostname(),
                current.getUserAgent(),  //75
                null, // client_mfg
                null, // client_device
                null, // client_os
                null // client_model
        );
    }

    private static int safeSetInteger(int value1, int value2) {

        if ( value1 >= 0 && value2 >= 0)
            return value1 >= value2 ? value1 - value2 : 0;
        else {
            long longVal1 = int2long(value1);
            long longVal2 = int2long(value2);

            long delta = longVal1 >= longVal2 ? longVal1 - longVal2 : 0;
            if ( delta > Integer.MAX_VALUE )
                return Integer.MAX_VALUE;
            else
                return (int)delta;
        }
    }

    private static Long int2long(int intVal) {

        ByteBuffer bf = ByteBuffer.allocate(8);
        bf.putInt(0);
        bf.putInt(intVal);

        return ByteBuffer.wrap(bf.array()).getLong();
    }

    private static long safeSetLong(long value1, long value2) {
        return value1 >= value2 ? value1 - value2 : 0;
    }
}

/**
 Some(0)   => ap_id
 Some(1)   => site_id
 Some(2)   => org_id
 Some(3)   => firmware_version
 Some(4)   => model
 Some(5)   => uptime
 Some(6)   => interval
 Some(7)   => when
 Some(8)   => radios
 Some(9)   => wlans
 Some(10)   => app_ss
 Some(11)   => terminator_remote_addr
 Some(12)   => terminator_timestamp
 Some(13)   => client_mac
 Some(14)   => client_wcid
 Some(15)   => client_radio_index
 Some(16)   => client_band
 Some(17)   => client_bssid
 Some(18)   => client_wlan_id
 Some(19)   => client_wlan_ssid
 Some(20)   => client_connected_time_sec
 Some(21)   => client_inactive_time_milli_sec
 Some(22)   => client_rssi
 Some(23)   => client_per_antenna_rssi
 Some(24)   => client_avg_rssi
 Some(25)   => client_tx_bit_rate
 Some(26)   => client_tx_unicast_bytes
 Some(27)   => client_tx_unicast_pkts
 Some(28)   => client_tx_pkts_sent
 Some(29)   => client_tx_retries
 Some(30)   => client_tx_retried
 Some(31)   => client_tx_failed
 Some(32)   => client_txbps
 Some(33)   => client_txpps
 Some(34)   => client_tx_retry_exhausted
 Some(35)   => client_tx_rate_fall_back
 Some(36)   => client_rx_bit_rate
 Some(37)   => client_rx_bytes
 Some(38)   => client_rx_pkts
 Some(39)   => client_rx_mcast_pkts
 Some(40)   => client_rx_mcast_bytes
 Some(41)   => client_rx_retried
 Some(42)   => client_rx_retries
 Some(43)   => client_rx_dups
 Some(44)   => client_rx_decrypt_failures
 Some(45)   => client_rxbps
 Some(46)   => client_rxpps
 Some(47)   => client_rx_probe_reqs24
 Some(48)   => client_rx_probe_reqs5
 Some(49)   => client_pq_requested
 Some(50)   => client_pq_dropped
 Some(51)   => client_pq_retried
 Some(52)   => client_pq_rtsfail
 Some(53)   => client_pq_throughput
 Some(54)   => client_pq_airuse
 Some(55)   => client_pqmax_used
 Some(56)   => client_pq_length
 Some(57)   => client_tx_delay
 Some(58)   => client_ipv4
 Some(59)   => client_vlan_id
 Some(60)   => client_username
 Some(61)   => client_cckrates
 Some(62)   => client_ofdmrates
 Some(63)   => client_mcsrates
 Some(64)   => client_protocol
 Some(65)   => client_num_streams
 Some(66)   => client_multi_pskname
 Some(67)   => cloud_last_rtt
 Some(68)   => cloud_when_last_rtt
 Some(69)   => client_count
 Some(70)   => delta
 Some(71)   => deltaErrorMsg
 Some(72)   => delta_interval
 Some(73)   => isActive
 Some(74)   => hostname
 Some(75)   => user_agent
 Some(76)   => client_mfg
 Some(77)   => client_device
 Some(78)   => client_os
 Some(79)   => client_model
 */