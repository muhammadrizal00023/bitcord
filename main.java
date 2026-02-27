/*
 * Bitcord – Decentralized guild and channel client. Single-file implementation.
 * Compatible with EVM guild registry; all protocol types and handlers in one unit.
 */

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// -----------------------------------------------------------------------------
// Core protocol constants (unique to Bitcord)
// -----------------------------------------------------------------------------

final class BitcordConstants {
    static final String PROTOCOL_VERSION = "BCRD/2.7";
    static final int MAX_GUILD_NAME_LEN = 64;
    static final int MIN_GUILD_NAME_LEN = 2;
    static final int MAX_CHANNEL_NAME_LEN = 48;
    static final int MAX_MESSAGE_BODY_LEN = 4096;
    static final int DEFAULT_WEBSOCKET_PORT = 9347;
    static final int HEARTBEAT_INTERVAL_MS = 42000;
    static final int RECONNECT_BASE_MS = 1200;
    static final int MAX_RECONNECT_DELAY_MS = 180000;
    static final byte CHANNEL_TYPE_TEXT = 0;
    static final byte CHANNEL_TYPE_VOICE = 1;
    static final byte CHANNEL_TYPE_ANNOUNCE = 2;
    static final String HEX_PREFIX = "0x";
    static final Pattern ADDRESS_PATTERN = Pattern.compile("^0x[a-fA-F0-9]{40}$");
    static final Pattern GUILD_ID_PATTERN = Pattern.compile("^0x[a-fA-F0-9]{64}$");
    private BitcordConstants() {}
}

// -----------------------------------------------------------------------------
// Exceptions (Bitcord-specific)
// -----------------------------------------------------------------------------

class BitcordNetworkException extends RuntimeException {
    BitcordNetworkException(String msg) { super(msg); }
    BitcordNetworkException(String msg, Throwable cause) { super(msg, cause); }
}

class BitcordValidationException extends RuntimeException {
    BitcordValidationException(String msg) { super(msg); }
}

