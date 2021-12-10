package com.subhadip.datory.preprocessing.utils;

import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

public class EncryptionUtil {

    private static final Logger LOG = Logger.getLogger(EncryptionUtil.class);

    private static final char[] PASSWORD = "<YOUR_PASSWORD>".toCharArray();
    private static final byte[] SALT = { (byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12, (byte) 0xde, (byte) 0x33, (byte) 0x10,
            (byte) 0x12, };
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final String CANNOT_CREATE_CIPHER = "CANNOT_CREATE_CIPHER";
    public static final String ENCODING_NOT_SUPPORTED = "ENCODING_NOT_SUPPORTED";

    private static Cipher createCipher(boolean forEncryption) {
        try {
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
            LOG.debug("Secret key factory created");

            SecretKey key = keyFactory.generateSecret(new PBEKeySpec(PASSWORD));
            LOG.debug("Secret key created");

            Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
            LOG.debug("Cipher created");

            pbeCipher.init((forEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE), key, new PBEParameterSpec(SALT, 20));
            LOG.debug("Cipher initialized");

            return pbeCipher;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | NoSuchPaddingException | InvalidKeyException
                | InvalidAlgorithmParameterException e) {
            throw new RuntimeException("Not able to create Cipher Text", e);
        }
    }

    private static byte[] base64Decode(String property) {
        return new Base64().decode(property);
    }

    private static String base64Encode(byte[] bytes) {
        return new Base64().encodeToString(bytes);
    }

    public static String encrypt(String property) {
        try {
            return base64Encode(createCipher(true).doFinal(property.getBytes(UTF_8)));
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Not able to Encrypt User Password", e);
        }
    }

    public static String decrypt(String property) {
        try {
            return new String(createCipher(false).doFinal(base64Decode(property)), UTF_8);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException("Not able to decrypt password", e);
        }
    }

}
