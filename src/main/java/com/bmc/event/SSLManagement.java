package com.bmc.event;

import org.apache.http.conn.ssl.X509HostnameVerifier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public final class SSLManagement {

    static X509HostnameVerifier getAcceptAllHostnameVerifier() {
        return new X509HostnameVerifier() {
            @Override
            public void verify(String s, SSLSocket sslSocket) throws IOException {

            }

            @Override
            public void verify(String s, X509Certificate x509Certificate) throws SSLException {

            }

            @Override
            public void verify(String s, String[] strings, String[] strings1) throws SSLException {

            }

            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        };
    }

    static SSLContext createTrustAllSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sc = SSLContext.getInstance("SSL");
        TrustManager[] trustAllCerts = createTrustAllManager();
        sc.init(null, trustAllCerts, new SecureRandom());
        return sc;
    }

    private static TrustManager[] createTrustAllManager() {
        return new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }
        };
    }
}
