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

    void addListener(BitcordEventListener l) { listeners.add(l); }
    void removeListener(BitcordEventListener l) { listeners.remove(l); }

    void dispatch(BitcordEvent event) {
        for (BitcordEventListener l : listeners)
            executor.execute(() -> { try { l.onEvent(event); } catch (Throwable t) { /* log */ } });
    }

    void shutdown() { executor.shutdown(); }
}

// -----------------------------------------------------------------------------
// Rate limiter (token bucket)
// -----------------------------------------------------------------------------

final class BitcordRateLimiter {
    private final int maxTokens;
    private final double refillPerSecond;
    private double tokens;
    private long lastRefillNs;

    BitcordRateLimiter(int maxTokens, double refillPerSecond) {
        this.maxTokens = maxTokens;
        this.refillPerSecond = refillPerSecond;
        this.tokens = maxTokens;
        this.lastRefillNs = System.nanoTime();
    }

    synchronized boolean tryConsume() {
        refill();
        if (tokens >= 1) { tokens -= 1; return true; }
        return false;
    }

    synchronized void consume() {
        refill();
        while (tokens < 1) {
            try { wait(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
            refill();
        }
        tokens -= 1;
    }

    private void refill() {
        long now = System.nanoTime();
        double elapsed = (now - lastRefillNs) / 1e9;
        tokens = Math.min(maxTokens, tokens + elapsed * refillPerSecond);
        lastRefillNs = now;
    }
}

// -----------------------------------------------------------------------------
// Hashing / id helpers
// -----------------------------------------------------------------------------

final class BitcordCrypto {
    private static final SecureRandom RNG = new SecureRandom();

    static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : hash) sb.append(String.format("%02x", b & 0xff));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    static String randomHex(int byteLength) {
        byte[] b = new byte[byteLength];
        RNG.nextBytes(b);
        StringBuilder sb = new StringBuilder(byteLength * 2);
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }

    static String newMessageId() {
        return BitcordConstants.HEX_PREFIX + randomHex(32);
    }

    static String newChannelId(GuildId guildId) {
        return BitcordConstants.HEX_PREFIX + sha256Hex(guildId.getValue() + System.nanoTime() + randomHex(8));
    }
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

final class BitcordValidator {
    static void validateGuildName(String name) {
        if (name == null)
            throw new BitcordValidationException("Guild name null");
        int len = name.trim().length();
        if (len < BitcordConstants.MIN_GUILD_NAME_LEN || len > BitcordConstants.MAX_GUILD_NAME_LEN)
            throw new BitcordValidationException("Guild name length must be " + BitcordConstants.MIN_GUILD_NAME_LEN + "-" + BitcordConstants.MAX_GUILD_NAME_LEN);
    }

    static void validateChannelName(String name) {
        if (name == null)
            throw new BitcordValidationException("Channel name null");
        int len = name.trim().length();
        if (len < BitcordConstants.MIN_GUILD_NAME_LEN || len > BitcordConstants.MAX_CHANNEL_NAME_LEN)
            throw new BitcordValidationException("Channel name length invalid");
    }

    static void validateMessageContent(String content) {
        if (content == null)
            throw new BitcordValidationException("Message content null");
        if (content.length() > BitcordConstants.MAX_MESSAGE_BODY_LEN)
            throw new BitcordValidationException("Message too long");
    }

    static void validateChannelType(byte type) {
        if (type != BitcordConstants.CHANNEL_TYPE_TEXT
                && type != BitcordConstants.CHANNEL_TYPE_VOICE
                && type != BitcordConstants.CHANNEL_TYPE_ANNOUNCE)
            throw new BitcordValidationException("Invalid channel type: " + type);
    }
}

// -----------------------------------------------------------------------------
// Main client facade
// -----------------------------------------------------------------------------

public final class Bitcord {

    private final BitcordRestClient restClient;
    private final BitcordCache cache;
    private final BitcordEventDispatcher dispatcher;
    private final BitcordRateLimiter rateLimiter;
    private final WalletAddress identity;
    private final AtomicLong sequence = new AtomicLong(0);

    private Bitcord(BitcordRestClient restClient, BitcordCache cache,
                   BitcordEventDispatcher dispatcher, BitcordRateLimiter rateLimiter,
                   WalletAddress identity) {
        this.restClient = restClient;
        this.cache = cache;
        this.dispatcher = dispatcher;
        this.rateLimiter = rateLimiter;
        this.identity = identity;
    }

    public WalletAddress getIdentity() { return identity; }

    public GuildSnapshot fetchGuild(GuildId guildId) {
        rateLimiter.consume();
        String json = restClient.get("/guilds/" + guildId.getValue());
        return parseGuildSnapshot(json);
    }

    public ChannelSnapshot fetchChannel(ChannelId channelId) {
        rateLimiter.consume();
        String json = restClient.get("/channels/" + channelId.getValue());
        return parseChannelSnapshot(json);
    }

    public List<ChannelSnapshot> fetchGuildChannels(GuildId guildId) {
        rateLimiter.consume();
        String json = restClient.get("/guilds/" + guildId.getValue() + "/channels");
        return parseChannelList(json);
    }

    public List<WalletAddress> fetchGuildMembers(GuildId guildId) {
        rateLimiter.consume();
        String json = restClient.get("/guilds/" + guildId.getValue() + "/members");
        return parseAddressList(json);
    }

    public void createGuild(String name) {
        BitcordValidator.validateGuildName(name);
        rateLimiter.consume();
        String body = SimpleJson.object("name", name);
        restClient.post("/guilds", body);
    }

    public void createChannel(GuildId guildId, String name, byte channelType) {
        BitcordValidator.validateChannelName(name);
        BitcordValidator.validateChannelType(channelType);
        rateLimiter.consume();
        String body = SimpleJson.object(
                "guildId", guildId.getValue(),
                "name", name,
                "channelType", String.valueOf(channelType));
        restClient.post("/channels", body);
    }

    public void joinGuild(GuildId guildId) {
        rateLimiter.consume();
        restClient.post("/guilds/" + guildId.getValue() + "/join", "{}");
    }

    public void leaveGuild(GuildId guildId) {
        rateLimiter.consume();
        restClient.post("/guilds/" + guildId.getValue() + "/leave", "{}");
    }

    public void sendMessage(ChannelId channelId, String content) {
        BitcordValidator.validateMessageContent(content);
        rateLimiter.consume();
        String body = SimpleJson.object(
                "channelId", channelId.getValue(),
                "content", content);
        restClient.post("/messages", body);
    }

    public void addEventListener(BitcordEventListener listener) {
        dispatcher.addListener(listener);
    }

    public void removeEventListener(BitcordEventListener listener) {
        dispatcher.removeListener(listener);
    }

    public void updateCacheFromEvent(BitcordEvent event) {
        switch (event.getKind()) {
            case GUILD_CREATED:
                GuildSnapshot g = parseGuildSnapshot(event.getPayloadJson());
                if (g != null) cache.putGuild(g);
                break;
            case CHANNEL_CREATED:
                ChannelSnapshot c = parseChannelSnapshot(event.getPayloadJson());
                if (c != null) cache.putChannel(c);
                break;
            case MEMBER_JOINED:
                String gj = event.getPayloadJson();
                GuildId gid = parseGuildIdFromPayload(gj);
                WalletAddress addr = parseAddressFromPayload(gj);
                if (gid != null && addr != null) cache.addGuildMember(gid, addr);
                break;
            case MEMBER_LEFT:
                String gl = event.getPayloadJson();
                GuildId gid2 = parseGuildIdFromPayload(gl);
                WalletAddress addr2 = parseAddressFromPayload(gl);
                if (gid2 != null && addr2 != null) cache.removeGuildMember(gid2, addr2);
                break;
            case MESSAGE_RECEIVED:
                BitcordMessage msg = parseMessage(event.getPayloadJson());
                if (msg != null) cache.appendMessage(msg.getChannelId(), msg);
                break;
            default:
                break;
        }
    }

    public GuildSnapshot getCachedGuild(GuildId guildId) {
        return cache.getGuild(guildId);
    }

    public ChannelSnapshot getCachedChannel(ChannelId channelId) {
        return cache.getChannel(channelId);
    }

    public List<ChannelSnapshot> getCachedChannelsForGuild(GuildId guildId) {
        return cache.getChannelsForGuild(guildId);
    }

    public List<BitcordMessage> getCachedMessages(ChannelId channelId, int limit) {
        return cache.getMessages(channelId, limit);
    }

    public void clearCache() {
        cache.clear();
    }

    public void dispatchEvent(BitcordEventKind kind, String payloadJson) {
        BitcordEvent ev = new BitcordEvent(kind, payloadJson, sequence.incrementAndGet(), System.currentTimeMillis());
        updateCacheFromEvent(ev);
        dispatcher.dispatch(ev);
    }

    private static GuildSnapshot parseGuildSnapshot(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            String id = extractString(json, "guildId");
            String name = extractString(json, "name");
            String owner = extractString(json, "owner");
            long createdAt = extractLong(json, "createdAt");
            boolean archived = extractBoolean(json, "archived");
            int channelCount = (int) extractLong(json, "channelCount");
            int memberCount = (int) extractLong(json, "memberCount");
            if (id == null || name == null || owner == null) return null;
            return new GuildSnapshot(new GuildId(id), name, new WalletAddress(owner), createdAt, archived, channelCount, memberCount);
        } catch (Exception e) { return null; }
    }

    private static ChannelSnapshot parseChannelSnapshot(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            String cid = extractString(json, "channelId");
            String gid = extractString(json, "guildId");
            String name = extractString(json, "name");
            byte type = (byte) extractLong(json, "channelType");
            boolean archived = extractBoolean(json, "archived");
            long createdAt = extractLong(json, "createdAt");
            if (cid == null || gid == null || name == null) return null;
            return new ChannelSnapshot(new ChannelId(cid), new GuildId(gid), name, type, archived, createdAt);
        } catch (Exception e) { return null; }
    }

    private static BitcordMessage parseMessage(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            String msgId = extractString(json, "messageId");
            String channelId = extractString(json, "channelId");
            String author = extractString(json, "author");
            String content = extractString(json, "content");
            long timestamp = extractLong(json, "timestamp");
            boolean edited = extractBoolean(json, "edited");
            if (msgId == null || channelId == null || author == null || content == null) return null;
            return new BitcordMessage(msgId, new ChannelId(channelId), new WalletAddress(author), content, timestamp, edited);
        } catch (Exception e) { return null; }
    }

    private static GuildId parseGuildIdFromPayload(String json) {
        String id = extractString(json, "guildId");
        return id != null ? new GuildId(id) : null;
    }

    private static WalletAddress parseAddressFromPayload(String json) {
        String a = extractString(json, "member");
        if (a == null) a = extractString(json, "address");
        return a != null ? new WalletAddress(a) : null;
    }

    private static List<ChannelSnapshot> parseChannelList(String json) {
        List<ChannelSnapshot> out = new ArrayList<>();
        if (json == null || !json.trim().startsWith("[")) return out;
        int start = json.indexOf('[') + 1;
        int depth = 1;
        StringBuilder current = new StringBuilder();
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') { depth++; current.append(c); }
            else if (c == '}') { depth--; current.append(c); if (depth == 1) { ChannelSnapshot sn = parseChannelSnapshot(current.toString()); if (sn != null) out.add(sn); current.setLength(0); } }
            else if (depth >= 2) current.append(c);
            else if (c == '[') depth++;
            else if (c == ']') depth--;
        }
        return out;
    }

    private static List<WalletAddress> parseAddressList(String json) {
        List<WalletAddress> out = new ArrayList<>();
        if (json == null || !json.trim().startsWith("[")) return out;
        int i = json.indexOf('[') + 1;
        while (i < json.length()) {
            int q = json.indexOf('"', i);
            if (q == -1) break;
            int end = json.indexOf('"', q + 1);
            if (end == -1) break;
            String addr = json.substring(q + 1, end);
            if (BitcordConstants.ADDRESS_PATTERN.matcher(addr).matches())
                out.add(new WalletAddress(addr));
            i = end + 1;
        }
        return out;
    }

    private static String extractString(String json, String key) {
        String search = "\"" + key + "\"";
        int idx = json.indexOf(search);
        if (idx == -1) return null;
        int colon = json.indexOf(':', idx);
        if (colon == -1) return null;
        int start = json.indexOf('"', colon);
        if (start == -1) return null;
        int end = json.indexOf('"', start + 1);
        if (end == -1) return null;
        return json.substring(start + 1, end);
    }

    private static long extractLong(String json, String key) {
        String search = "\"" + key + "\"";
        int idx = json.indexOf(search);
        if (idx == -1) return 0;
        int colon = json.indexOf(':', idx);
        if (colon == -1) return 0;
        int i = colon + 1;
        while (i < json.length() && (Character.isWhitespace(json.charAt(i)) || json.charAt(i) == ':')) i++;
        StringBuilder num = new StringBuilder();
        while (i < json.length() && (Character.isDigit(json.charAt(i)) || json.charAt(i) == '-')) {
            num.append(json.charAt(i));
            i++;
        }
        return num.length() == 0 ? 0 : Long.parseLong(num.toString());
    }

    private static boolean extractBoolean(String json, String key) {
        String search = "\"" + key + "\"";
        int idx = json.indexOf(search);
        if (idx == -1) return false;
        int colon = json.indexOf(':', idx);
        if (colon == -1) return false;
        int t = json.indexOf("true", colon);
        int f = json.indexOf("false", colon);
        if (t != -1 && (f == -1 || t < f)) return true;
        if (f != -1 && (t == -1 || f < t)) return false;
        return false;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String baseUrl = "https://api.bitcord.example";
        private WalletAddress identity;
        private Map<String, String> headers;
        private int connectTimeoutMs = 10000;
        private int readTimeoutMs = 30000;
        private int maxMessagesPerChannel = 100;
        private long messageTtlMs = 3600000;
        private int rateLimitMax = 30;
        private double rateLimitRefill = 1.0;
        private ExecutorService executor;

        public Builder baseUrl(String url) { this.baseUrl = url; return this; }
        public Builder identity(WalletAddress identity) { this.identity = identity; return this; }
        public Builder identity(String address) { this.identity = new WalletAddress(address); return this; }
        public Builder headers(Map<String, String> headers) { this.headers = headers; return this; }
        public Builder connectTimeoutMs(int ms) { this.connectTimeoutMs = ms; return this; }
        public Builder readTimeoutMs(int ms) { this.readTimeoutMs = ms; return this; }
        public Builder maxMessagesPerChannel(int n) { this.maxMessagesPerChannel = n; return this; }
        public Builder messageTtlMs(long ms) { this.messageTtlMs = ms; return this; }
        public Builder rateLimit(int max, double refillPerSec) { this.rateLimitMax = max; this.rateLimitRefill = refillPerSec; return this; }
        public Builder executor(ExecutorService ex) { this.executor = ex; return this; }

        public Bitcord build() {
            if (identity == null) throw new BitcordValidationException("identity required");
            ExecutorService ex = executor != null ? executor : Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "bitcord-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });
            BitcordRestClient rest = new BitcordRestClient(baseUrl, identity, headers, connectTimeoutMs, readTimeoutMs);
            BitcordCache cache = new BitcordCache(maxMessagesPerChannel, messageTtlMs);
            BitcordEventDispatcher disp = new BitcordEventDispatcher(ex);
            BitcordRateLimiter limiter = new BitcordRateLimiter(rateLimitMax, rateLimitRefill);
            return new Bitcord(rest, cache, disp, limiter, identity);
        }
    }

    // -------------------------------------------------------------------------
    // Entry / demo
    // -------------------------------------------------------------------------

    public static void main(String[] args) {
        WalletAddress addr = new WalletAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1");
        Bitcord client = Bitcord.builder()
                .identity(addr)
                .baseUrl("https://api.bitcord.example")
                .rateLimit(30, 1.0)
                .build();
        client.addEventListener(event -> System.out.println("Event: " + event.getKind() + " seq=" + event.getSequence()));
        System.out.println("Bitcord client ready for " + client.getIdentity());
    }
}

// -----------------------------------------------------------------------------
// Invite model and handling
// -----------------------------------------------------------------------------

final class InviteSnapshot {
    private final String code;
    private final GuildId guildId;
    private final WalletAddress creator;
    private final long expiresAt;
    private final int maxUses;
    private final int useCount;

    InviteSnapshot(String code, GuildId guildId, WalletAddress creator, long expiresAt, int maxUses, int useCount) {
        this.code = code;
        this.guildId = guildId;
        this.creator = creator;
        this.expiresAt = expiresAt;
        this.maxUses = maxUses;
        this.useCount = useCount;
    }
    String getCode() { return code; }
    GuildId getGuildId() { return guildId; }
    WalletAddress getCreator() { return creator; }
    long getExpiresAt() { return expiresAt; }
    int getMaxUses() { return maxUses; }
    int getUseCount() { return useCount; }
    boolean isValid() { return (maxUses <= 0 || useCount < maxUses) && (expiresAt <= 0 || System.currentTimeMillis() < expiresAt); }
}

// -----------------------------------------------------------------------------
// Direct message channel (DM) model
// -----------------------------------------------------------------------------

final class DmChannelSnapshot {
    private final String dmChannelId;
    private final WalletAddress peer;
    private final long createdAt;
    private final BitcordMessage lastMessage;

    DmChannelSnapshot(String dmChannelId, WalletAddress peer, long createdAt, BitcordMessage lastMessage) {
        this.dmChannelId = dmChannelId;
        this.peer = peer;
        this.createdAt = createdAt;
        this.lastMessage = lastMessage;
    }
    String getDmChannelId() { return dmChannelId; }
    WalletAddress getPeer() { return peer; }
    long getCreatedAt() { return createdAt; }
    BitcordMessage getLastMessage() { return lastMessage; }
}

// -----------------------------------------------------------------------------
// Permission flags (Bitcord-specific bitmask)
// -----------------------------------------------------------------------------

final class BitcordPermissions {
    static final long SEND_MESSAGES = 1L << 0;
    static final long READ_MESSAGES = 1L << 1;
    static final long MANAGE_CHANNEL = 1L << 2;
    static final long MANAGE_GUILD = 1L << 3;
    static final long KICK_MEMBERS = 1L << 4;
    static final long BAN_MEMBERS = 1L << 5;
    static final long ASSIGN_ROLES = 1L << 6;
    static final long CREATE_INVITE = 1L << 7;
    static final long ARCHIVE_CHANNEL = 1L << 8;
    static final long MANAGE_ROLES = 1L << 9;
    static final long ADMIN = (1L << 10) - 1;

    static boolean has(long permissions, long flag) { return (permissions & flag) == flag; }
    static long add(long permissions, long flag) { return permissions | flag; }
    static long remove(long permissions, long flag) { return permissions & ~flag; }
    private BitcordPermissions() {}
}

// -----------------------------------------------------------------------------
// Presence (online/offline/away)
// -----------------------------------------------------------------------------

enum PresenceStatus { ONLINE, OFFLINE, AWAY, DND, INVISIBLE }

final class PresenceSnapshot {
    private final WalletAddress user;
    private final PresenceStatus status;
    private final String customStatus;
    private final long lastSeenMs;

    PresenceSnapshot(WalletAddress user, PresenceStatus status, String customStatus, long lastSeenMs) {
        this.user = user;
        this.status = status;
        this.customStatus = customStatus;
        this.lastSeenMs = lastSeenMs;
    }
    WalletAddress getUser() { return user; }
    PresenceStatus getStatus() { return status; }
    String getCustomStatus() { return customStatus; }
    long getLastSeenMs() { return lastSeenMs; }
}

// -----------------------------------------------------------------------------
// Typing indicator
// -----------------------------------------------------------------------------

final class TypingIndicator {
    private final ChannelId channelId;
    private final WalletAddress user;
    private final long startedAtMs;

    TypingIndicator(ChannelId channelId, WalletAddress user, long startedAtMs) {
        this.channelId = channelId;
        this.user = user;
        this.startedAtMs = startedAtMs;
    }
    ChannelId getChannelId() { return channelId; }
    WalletAddress getUser() { return user; }
    long getStartedAtMs() { return startedAtMs; }
    boolean isExpired(long timeoutMs) { return System.currentTimeMillis() - startedAtMs > timeoutMs; }
}

// -----------------------------------------------------------------------------
// Retry policy with exponential backoff
// -----------------------------------------------------------------------------

final class BitcordRetryPolicy {
    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final Set<Class<? extends Throwable>> retryable;

    BitcordRetryPolicy(int maxAttempts, long initialDelayMs, long maxDelayMs, double multiplier,
                      Set<Class<? extends Throwable>> retryable) {
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        this.retryable = retryable;
    }

    boolean shouldRetry(Throwable t, int attempt) {
        if (attempt >= maxAttempts) return false;
        for (Class<? extends Throwable> c : retryable)
            if (c.isInstance(t)) return true;
        return false;
    }

    long delayMs(int attempt) {
        long d = (long) (initialDelayMs * Math.pow(multiplier, attempt));
        return Math.min(d, maxDelayMs);
    }

    static BitcordRetryPolicy defaultPolicy() {
        Set<Class<? extends Throwable>> set = new HashSet<>();
        set.add(BitcordNetworkException.class);
        set.add(BitcordRateLimitException.class);
        return new BitcordRetryPolicy(5, 1000, 60000, 2.0, set);
    }
}

// -----------------------------------------------------------------------------
// Connection pool for HTTP (simplified)
// -----------------------------------------------------------------------------

final class BitcordConnectionPool {
    private final String host;
    private final int port;
    private final int maxConnections;
    private final BlockingQueue<HttpURLConnection> idle;
    private final AtomicInteger activeCount = new AtomicInteger(0);

    BitcordConnectionPool(String host, int port, int maxConnections) {
        this.host = host;
