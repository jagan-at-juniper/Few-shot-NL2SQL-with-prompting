package com.mist;

import com.google.protobuf.util.Timestamps;
import com.mist.mobius.common.MistUtils;
import com.mist.mobius.generated.ApstatsAnalyticsRaw;
import com.mist.mobius.generated.ClientStatsAnalytics;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class ClientDataIterator implements Iterator<ClientStatsAnalytics.ClientStatsAnalyticsOut> {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw;
    ClientStatsAnalytics.ClientStatsAnalyticsOut.Builder clientStatsOutBuilder;
    List<ApstatsAnalyticsRaw.ClientStats> clients;
    int clientCount;
    int index;
    String sert;

    public ClientDataIterator(ApstatsAnalyticsRaw.APStats clientStatsAnalyticsRaw, String sert) {

        this.sert = sert;
        this.clientStatsAnalyticsRaw = clientStatsAnalyticsRaw;
        String apMAC = MistUtils.getMAC(clientStatsAnalyticsRaw.getId().toByteArray());
        String siteId = MistUtils.getUUID(clientStatsAnalyticsRaw.getSiteId().toByteArray());
        long statsCaptureTime = Timestamps.toMicros(clientStatsAnalyticsRaw.getWhen());

        this.clientStatsOutBuilder =
                ClientStatsAnalytics.ClientStatsAnalyticsOut.newBuilder();

        clientStatsOutBuilder.setApId(apMAC);
        clientStatsOutBuilder.setSiteId(siteId);
        String orgId = MistUtils.getUUID(clientStatsAnalyticsRaw.getOrgId().toByteArray());
        clientStatsOutBuilder.setOrgId(orgId);
        clientStatsOutBuilder.setInterval(new Double(clientStatsAnalyticsRaw.getInterval()).longValue());
        clientStatsOutBuilder.setFirmwareVersion(clientStatsAnalyticsRaw.getFirmwareVersion());
        clientStatsOutBuilder.setModel(clientStatsAnalyticsRaw.getModel());
        clientStatsOutBuilder.setUptime(clientStatsAnalyticsRaw.getUptime());

        clientStatsOutBuilder.setWhen(statsCaptureTime);
        parseInfoFromTerminator(clientStatsAnalyticsRaw, clientStatsOutBuilder);
        clientStatsOutBuilder.setCloudLastRtt(clientStatsAnalyticsRaw.getCloud().getLastRtt());

        clients = clientStatsAnalyticsRaw.getClientsList();

        clientCount = clients.size();
        clientStatsOutBuilder.setClientCount(clientCount);
        index = 0;

    }

    public boolean hasNext() {
        return index <= clientCount - 1;
    }

    public ClientStatsAnalytics.ClientStatsAnalyticsOut next() {
        try {
            ApstatsAnalyticsRaw.ClientStats client = clients.get(index);

            String clientId = MistUtils.getMAC(client.getMac().toByteArray(), "", "");

            String wcid = mac2wcid(sert, clientId.replaceAll("-", ""));
            String clientBand = MistUtils.getBand(client.getBand());

//        clientStatsOutBuilder.setClientMac(wcid);
//        clientStatsOutBuilder.setClientWcid(wcid);

            clientStatsOutBuilder.setClientMac(clientId);
            clientStatsOutBuilder.setClientWcid(clientId);
            clientStatsOutBuilder.setClientBand(clientBand);
            clientStatsOutBuilder.setClientRadioIndex(client.getRadioIndex());
            clientStatsOutBuilder.setClientBssid(MistUtils.getMAC(client.getBssid().toByteArray()));
            clientStatsOutBuilder.setClientAvgRssi(client.getAvgRssi());
            clientStatsOutBuilder.setClientIpv4(MistUtils.getIp(client.getIpv4().toByteArray()));
            clientStatsOutBuilder.setClientVlanId(client.getVlanId());
            clientStatsOutBuilder.setClientWlanId(MistUtils.getMAC(client.getWlan().getId().toByteArray()));
            clientStatsOutBuilder.setClientWlanSsid(client.getWlan().getSsid());
            clientStatsOutBuilder.setClientRssi(client.getRssi());
            clientStatsOutBuilder.setClientNumStreams(client.getNumStreams());
            clientStatsOutBuilder.setClientProtocol(client.getProtocol());

            clientStatsOutBuilder.setClientRxPkts(client.getRxPkts());
            clientStatsOutBuilder.setClientRxbps(client.getRxbps());
            clientStatsOutBuilder.setClientRxDups(client.getRxDups());
            clientStatsOutBuilder.setClientRxBytes(client.getRxBytes());
            clientStatsOutBuilder.setClientRxBitRate(client.getRxBitRate());
            clientStatsOutBuilder.setClientRxRetried(client.getRxRetried());
            clientStatsOutBuilder.setClientRxRetries(client.getRxRetries());

            clientStatsOutBuilder.setClientTxRetryExhausted(client.getTxRetryExhausted());
            clientStatsOutBuilder.setClientTxPktsSent(client.getTxPktsSent());
            clientStatsOutBuilder.setClientTxbps(client.getTxbps());
            clientStatsOutBuilder.setClientTxpps(client.getTxpps());
            clientStatsOutBuilder.setClientTxRetried(client.getTxRetried());
            clientStatsOutBuilder.setClientTxRetries(client.getTxRetries());
            clientStatsOutBuilder.setClientTxBitRate(client.getTxBitRate());
            clientStatsOutBuilder.setClientTxFailed(client.getTxFailed());

            // MIST-23927
            clientStatsOutBuilder.setClientTxUnicastBytes(client.getTxUnicastBytes());
            clientStatsOutBuilder.setClientTxUnicastPkts(client.getTxUnicastPkts());

            clientStatsOutBuilder.setClientPqLength(client.getPqLength());
            clientStatsOutBuilder.setClientPqRetried(client.getPqRetried());


            clientStatsOutBuilder.setClientPqAiruse(client.getPqAiruse());

            clientStatsOutBuilder.setClientPqmaxUsed(client.getPqmaxUsed());
            clientStatsOutBuilder.setClientPqRtsfail(client.getPqRtsfail());
            clientStatsOutBuilder.setClientPqRequested(client.getPqRequested());
            clientStatsOutBuilder.setClientPqDropped(client.getPqDropped());
            clientStatsOutBuilder.setClientPqThroughput(client.getPqThroughput());

            clientStatsOutBuilder.setClientConnectedTimeSec(client.getConnectedTimeSec());
            clientStatsOutBuilder.setClientInactiveTimeMilliSec(client.getInactiveTimeMilliSec());

            // add user agent and hostname
            clientStatsOutBuilder.setUserAgent(client.getUserAgent());
            clientStatsOutBuilder.setHostname(client.getHostname());

            // clearing the previous saved PerAntennaRssiList and adding new entries
            clientStatsOutBuilder.clearClientPerAntennaRssi();
            List<Integer> antenaList = client.getPerAntennaRssiList();
            clientStatsOutBuilder.addAllClientPerAntennaRssi(antenaList);

            return clientStatsOutBuilder.build();
        } finally {
            index++;
        }
    }

    public void remove() {

    }

    public static void parseInfoFromTerminator(
            ApstatsAnalyticsRaw.APStats clientStats, ClientStatsAnalytics.ClientStatsAnalyticsOut.Builder output) {

        if (!clientStats.hasInfoFromTerminator()) return;

        ApstatsAnalyticsRaw.InfoFromTerminator infoFromTerminator = clientStats.getInfoFromTerminator();
        MistUtils.IPPort remoteAddr = MistUtils.getIpPort(infoFromTerminator.getRemoteAddr());
        long epoch = Timestamps.toMicros(infoFromTerminator.getTimestamp());

        output.setTerminatorRemoteAddr(remoteAddr.toString());
        output.setTerminatorTimestamp(epoch);
    }

    public static String mac2wcid(String sert, String mac) {

        if (!mac.matches("[0-9a-f]{12}") || StringUtils.repeat("0", 12).equals(mac)) {
            System.err.println("Invalid mac address passed in: " + mac);
            return null;
        }

        try {

            byte[] KeyByte = sert.getBytes(UTF_8);
            SecretKey secKey = new SecretKeySpec(KeyByte, 0, KeyByte.length, "AES");

            if (secKey != null) {
                byte[] cipherText = encryptText(mac, secKey);
                String wcid = MistUtils.getUUID(cipherText);

                return wcid;
            } else {
                System.err.println("secKey == null for orgID ");
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
        return null;
    }

    private static byte[] encryptText(String plainText, SecretKey secKey) throws Exception {
        byte[] plainTextB = MistUtils.hexToBytes(plainText);
        byte[] text = new byte[16];

        for (int i = 0; i < plainTextB.length; i++) {
            text[i] = plainTextB[i];
        }
        for (int i = plainTextB.length; i < 16; i++) {
            text[i] = 0;
        }

        Cipher aesCipher = Cipher.getInstance("AES/ECB/NoPadding");
        aesCipher.init(Cipher.ENCRYPT_MODE, secKey);
        byte[] byteCipherText = aesCipher.doFinal(text);
        return byteCipherText;
    }

    public static  void main(String[] args) {
        System.out.println(mac2wcid("dummy", "30e171a66b2c"));

    }
}
