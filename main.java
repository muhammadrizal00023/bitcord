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

class BitcordAuthException extends RuntimeException {
    BitcordAuthException(String msg) { super(msg); }
}

class BitcordRateLimitException extends RuntimeException {
    private final long retryAfterMs;
    BitcordRateLimitException(String msg, long retryAfterMs) {
        super(msg);
        this.retryAfterMs = retryAfterMs;
    }
    long getRetryAfterMs() { return retryAfterMs; }
}

// -----------------------------------------------------------------------------
// Value types
// -----------------------------------------------------------------------------

final class GuildId {
    private final String value;
    GuildId(String value) {
        if (value == null || !BitcordConstants.GUILD_ID_PATTERN.matcher(value).matches())
            throw new BitcordValidationException("Invalid guild id: " + value);
        this.value = value;
    }
    String getValue() { return value; }
    @Override public boolean equals(Object o) {
        return o instanceof GuildId && value.equals(((GuildId) o).value);
    }
    @Override public int hashCode() { return value.hashCode(); }
    @Override public String toString() { return value; }
}

final class ChannelId {
    private final String value;
    ChannelId(String value) {
        if (value == null || value.length() != 66 || !value.startsWith(BitcordConstants.HEX_PREFIX))
            throw new BitcordValidationException("Invalid channel id: " + value);
        this.value = value;
    }
    String getValue() { return value; }
    @Override public boolean equals(Object o) {
        return o instanceof ChannelId && value.equals(((ChannelId) o).value);
    }
    @Override public int hashCode() { return value.hashCode(); }
    @Override public String toString() { return value; }
}

final class WalletAddress {
    private final String value;
    WalletAddress(String value) {
        if (value == null || !BitcordConstants.ADDRESS_PATTERN.matcher(value).matches())
            throw new BitcordValidationException("Invalid address: " + value);
        this.value = value;
    }
    String getValue() { return value; }
    @Override public boolean equals(Object o) {
        return o instanceof WalletAddress && value.equalsIgnoreCase(((WalletAddress) o).value);
    }
    @Override public int hashCode() { return value.toLowerCase().hashCode(); }
    @Override public String toString() { return value; }
}

// -----------------------------------------------------------------------------
// Guild model
// -----------------------------------------------------------------------------

final class GuildSnapshot {
    private final GuildId guildId;
    private final String name;
    private final WalletAddress owner;
    private final long createdAt;
    private final boolean archived;
    private final int channelCount;
    private final int memberCount;

    GuildSnapshot(GuildId guildId, String name, WalletAddress owner, long createdAt,
                  boolean archived, int channelCount, int memberCount) {
        this.guildId = guildId;
        this.name = name;
        this.owner = owner;
        this.createdAt = createdAt;
        this.archived = archived;
        this.channelCount = channelCount;
        this.memberCount = memberCount;
    }

    GuildId getGuildId() { return guildId; }
    String getName() { return name; }
    WalletAddress getOwner() { return owner; }
    long getCreatedAt() { return createdAt; }
    boolean isArchived() { return archived; }
    int getChannelCount() { return channelCount; }
    int getMemberCount() { return memberCount; }
}

// -----------------------------------------------------------------------------
// Channel model
// -----------------------------------------------------------------------------

final class ChannelSnapshot {
    private final ChannelId channelId;
    private final GuildId guildId;
    private final String name;
    private final byte channelType;
    private final boolean archived;
    private final long createdAt;

    ChannelSnapshot(ChannelId channelId, GuildId guildId, String name, byte channelType,
                    boolean archived, long createdAt) {
        this.channelId = channelId;
        this.guildId = guildId;
        this.name = name;
        this.channelType = channelType;
        this.archived = archived;
        this.createdAt = createdAt;
    }

    ChannelId getChannelId() { return channelId; }
    GuildId getGuildId() { return guildId; }
    String getName() { return name; }
    byte getChannelType() { return channelType; }
    boolean isArchived() { return archived; }
    long getCreatedAt() { return createdAt; }
}

// -----------------------------------------------------------------------------
// Role model
// -----------------------------------------------------------------------------

final class RoleSnapshot {
    private final long roleId;
    private final String name;

    RoleSnapshot(long roleId, String name) {
        this.roleId = roleId;
        this.name = name;
    }
    long getRoleId() { return roleId; }
    String getName() { return name; }
}

// -----------------------------------------------------------------------------
// Message model (in-memory / wire)
// -----------------------------------------------------------------------------

final class BitcordMessage {
    private final String messageId;
    private final ChannelId channelId;
    private final WalletAddress author;
    private final String content;
    private final long timestamp;
    private final boolean edited;

    BitcordMessage(String messageId, ChannelId channelId, WalletAddress author,
                   String content, long timestamp, boolean edited) {
        this.messageId = messageId;
        this.channelId = channelId;
        this.author = author;
        this.content = content;
        this.timestamp = timestamp;
        this.edited = edited;
    }

    String getMessageId() { return messageId; }
    ChannelId getChannelId() { return channelId; }
    WalletAddress getAuthor() { return author; }
    String getContent() { return content; }
    long getTimestamp() { return timestamp; }
    boolean isEdited() { return edited; }
}

// -----------------------------------------------------------------------------
// Event envelope (for WebSocket / internal bus)
// -----------------------------------------------------------------------------

enum BitcordEventKind {
    GUILD_CREATED,
    CHANNEL_CREATED,
    MEMBER_JOINED,
    MEMBER_LEFT,
    ROLE_ASSIGNED,
    ROLE_REVOKED,
    MESSAGE_RECEIVED,
    MESSAGE_UPDATED,
    MESSAGE_DELETED,
    CHANNEL_ARCHIVED,
    GUILD_ARCHIVED,
    HEARTBEAT_ACK,
    READY,
    ERROR
}

final class BitcordEvent {
    private final BitcordEventKind kind;
    private final String payloadJson;
    private final long sequence;
    private final long serverTimeMs;

    BitcordEvent(BitcordEventKind kind, String payloadJson, long sequence, long serverTimeMs) {
        this.kind = kind;
        this.payloadJson = payloadJson;
        this.sequence = sequence;
        this.serverTimeMs = serverTimeMs;
    }

    BitcordEventKind getKind() { return kind; }
    String getPayloadJson() { return payloadJson; }
    long getSequence() { return sequence; }
    long getServerTimeMs() { return serverTimeMs; }
}

// -----------------------------------------------------------------------------
// REST client (simulated / stub for EVM RPC + custom API)
// -----------------------------------------------------------------------------

final class BitcordRestClient {
    private final String baseUrl;
    private final WalletAddress identity;
    private final Map<String, String> headers;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final AtomicInteger requestIdGen = new AtomicInteger(0);

    BitcordRestClient(String baseUrl, WalletAddress identity, Map<String, String> headers,
                      int connectTimeoutMs, int readTimeoutMs) {
        this.baseUrl = baseUrl;
        this.identity = identity;
        this.headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.headers.putIfAbsent("X-Bitcord-Version", BitcordConstants.PROTOCOL_VERSION);
        this.headers.putIfAbsent("X-Wallet-Address", identity.getValue());
    }

    String getBaseUrl() { return baseUrl; }
    WalletAddress getIdentity() { return identity; }

    String get(String path) {
        int id = requestIdGen.incrementAndGet();
        try {
            URL url = new URL(baseUrl + path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            for (Map.Entry<String, String> e : headers.entrySet())
                conn.setRequestProperty(e.getKey(), e.getValue());
            conn.setRequestProperty("X-Request-Id", String.valueOf(id));
            int code = conn.getResponseCode();
            if (code == 429) {
                String retry = conn.getHeaderField("Retry-After");
                long retryMs = retry != null ? Long.parseLong(retry) * 1000L : 60000L;
                throw new BitcordRateLimitException("Rate limited", retryMs);
            }
            if (code < 200 || code >= 300)
                throw new BitcordNetworkException("HTTP " + code + " for GET " + path);
            return readFully(conn.getInputStream());
        } catch (IOException e) {
            throw new BitcordNetworkException("GET " + path + " failed", e);
        }
    }

    String post(String path, String body) {
        int id = requestIdGen.incrementAndGet();
        try {
            URL url = new URL(baseUrl + path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            for (Map.Entry<String, String> e : headers.entrySet())
                conn.setRequestProperty(e.getKey(), e.getValue());
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("X-Request-Id", String.valueOf(id));
            if (body != null && !body.isEmpty()) {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                conn.setRequestProperty("Content-Length", String.valueOf(bytes.length));
                try (OutputStream out = conn.getOutputStream()) { out.write(bytes); }
            }
            int code = conn.getResponseCode();
            if (code == 429) {
                String retry = conn.getHeaderField("Retry-After");
                long retryMs = retry != null ? Long.parseLong(retry) * 1000L : 60000L;
                throw new BitcordRateLimitException("Rate limited", retryMs);
            }
            InputStream in = code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream();
            return in != null ? readFully(in) : "";
        } catch (IOException e) {
            throw new BitcordNetworkException("POST " + path + " failed", e);
        }
    }

    private static String readFully(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] b = new byte[4096];
        int n;
        while ((n = in.read(b)) != -1) buf.write(b, 0, n);
        return buf.toString(StandardCharsets.UTF_8.name());
    }
}

// -----------------------------------------------------------------------------
// WebSocket connection manager
// -----------------------------------------------------------------------------

final class BitcordWebSocketConfig {
    private final String wsUrl;
    private final WalletAddress identity;
    private final int heartbeatIntervalMs;
    private final int maxReconnectDelayMs;
    private final int baseReconnectMs;

    BitcordWebSocketConfig(String wsUrl, WalletAddress identity, int heartbeatIntervalMs,
                           int maxReconnectDelayMs, int baseReconnectMs) {
        this.wsUrl = wsUrl;
        this.identity = identity;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxReconnectDelayMs = maxReconnectDelayMs;
        this.baseReconnectMs = baseReconnectMs;
    }

    String getWsUrl() { return wsUrl; }
    WalletAddress getIdentity() { return identity; }
    int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    int getMaxReconnectDelayMs() { return maxReconnectDelayMs; }
    int getBaseReconnectMs() { return baseReconnectMs; }
}

// -----------------------------------------------------------------------------
// Simple JSON (minimal, no external deps)
// -----------------------------------------------------------------------------

final class SimpleJson {
    private SimpleJson() {}

    static String escape(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') sb.append("\\\"");
            else if (c == '\\') sb.append("\\\\");
            else if (c == '\n') sb.append("\\n");
            else if (c == '\r') sb.append("\\r");
            else if (c == '\t') sb.append("\\t");
            else if (c < 32) sb.append(String.format("\\u%04x", (int) c));
            else sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }

    static String object(String... kv) {
        if (kv.length % 2 != 0) throw new IllegalArgumentException("key-value pairs");
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < kv.length; i += 2) {
            if (i > 0) sb.append(',');
            sb.append(escape(kv[i])).append(':').append(kv[i + 1].startsWith("{") || kv[i + 1].startsWith("[") ? kv[i + 1] : escape(kv[i + 1]));
        }
        sb.append('}');
        return sb.toString();
    }

    static String array(String... items) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.length; i++) {
            if (i > 0) sb.append(',');
            String x = items[i];
            sb.append(x.startsWith("{") || x.startsWith("[") ? x : escape(x));
        }
        sb.append(']');
        return sb.toString();
    }
}

// -----------------------------------------------------------------------------
// Local cache (guilds, channels, members, messages)
// -----------------------------------------------------------------------------

final class BitcordCache {
    private final ConcurrentHashMap<GuildId, GuildSnapshot> guilds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, ChannelSnapshot> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GuildId, List<ChannelSnapshot>> guildChannels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GuildId, Set<WalletAddress>> guildMembers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ChannelId, List<BitcordMessage>> channelMessages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GuildId, Map<WalletAddress, List<RoleSnapshot>>> memberRoles = new ConcurrentHashMap<>();
    private final int maxMessagesPerChannel;
    private final long messageTtlMs;

    BitcordCache(int maxMessagesPerChannel, long messageTtlMs) {
        this.maxMessagesPerChannel = maxMessagesPerChannel;
        this.messageTtlMs = messageTtlMs;
    }

    void putGuild(GuildSnapshot g) {
        guilds.put(g.getGuildId(), g);
    }

    GuildSnapshot getGuild(GuildId id) { return guilds.get(id); }

    void putChannel(ChannelSnapshot c) {
        channels.put(c.getChannelId(), c);
        guildChannels.computeIfAbsent(c.getGuildId(), k -> new CopyOnWriteArrayList<>()).add(c);
    }

    ChannelSnapshot getChannel(ChannelId id) { return channels.get(id); }

    List<ChannelSnapshot> getChannelsForGuild(GuildId guildId) {
        List<ChannelSnapshot> list = guildChannels.get(guildId);
        return list == null ? Collections.emptyList() : new ArrayList<>(list);
    }

    void addGuildMember(GuildId guildId, WalletAddress member) {
        guildMembers.computeIfAbsent(guildId, k -> ConcurrentHashMap.newKeySet()).add(member);
    }

    void removeGuildMember(GuildId guildId, WalletAddress member) {
        Set<WalletAddress> set = guildMembers.get(guildId);
        if (set != null) set.remove(member);
        memberRoles.getOrDefault(guildId, Collections.emptyMap()).remove(member);
    }

    Set<WalletAddress> getGuildMembers(GuildId guildId) {
        Set<WalletAddress> set = guildMembers.get(guildId);
        return set == null ? Collections.emptySet() : new HashSet<>(set);
    }

    void appendMessage(ChannelId channelId, BitcordMessage msg) {
        List<BitcordMessage> list = channelMessages.computeIfAbsent(channelId, k -> new CopyOnWriteArrayList<>());
        list.add(msg);
        while (list.size() > maxMessagesPerChannel) list.remove(0);
    }

    List<BitcordMessage> getMessages(ChannelId channelId, int limit) {
        List<BitcordMessage> list = channelMessages.get(channelId);
        if (list == null) return Collections.emptyList();
        int from = Math.max(0, list.size() - limit);
        return new ArrayList<>(list.subList(from, list.size()));
    }

    void putMemberRoles(GuildId guildId, WalletAddress member, List<RoleSnapshot> roles) {
        memberRoles.computeIfAbsent(guildId, k -> new ConcurrentHashMap<>()).put(member, new ArrayList<>(roles));
    }

    List<RoleSnapshot> getMemberRoles(GuildId guildId, WalletAddress member) {
        Map<WalletAddress, List<RoleSnapshot>> map = memberRoles.get(guildId);
        if (map == null) return Collections.emptyList();
        List<RoleSnapshot> r = map.get(member);
        return r == null ? Collections.emptyList() : new ArrayList<>(r);
    }

    void clear() {
        guilds.clear();
        channels.clear();
        guildChannels.clear();
        guildMembers.clear();
        channelMessages.clear();
        memberRoles.clear();
    }
}

// -----------------------------------------------------------------------------
// Event dispatcher (listeners)
// -----------------------------------------------------------------------------

interface BitcordEventListener {
    void onEvent(BitcordEvent event);
}

final class BitcordEventDispatcher {
    private final List<BitcordEventListener> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;

    BitcordEventDispatcher(ExecutorService executor) {
        this.executor = executor;
    }
