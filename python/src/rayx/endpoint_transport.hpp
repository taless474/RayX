// rayx endpoint transport -- A1: honest local cross-process delivery (plain native IPC).
//
// EXPERIMENTAL and deliberately NARROW. This header implements the A1 transport slice:
// a process-local AF_UNIX (SOCK_STREAM) listener and a client dial path that carry the
// fixed typed int64 ping across the process boundary, ON ONE NODE ONLY.
//
// HONESTY (read docs/design/endpoint_transport_slice.md):
//   * This is PLAIN NATIVE LOCAL IPC. It is NOT HPX transport, NOT HPX serving, NOT HPX
//     async I/O, NOT a fabric, NOT parcelport, NOT AGAS, NOT multi-node, and carries NO
//     performance claim.
//   * HPX is NOT the delivery mechanism. The listener is a plain OS accept thread; the
//     ping is computed INLINE via the pure rayx_endpoint::EndpointSeam::on_ping. There is
//     NO hpx::run_as_hpx_thread and NO HPX call anywhere in this file.
//   * AF_UNIX is chosen because it is single-node, NOT routable (cannot be misread as a
//     network/fabric path), and its filesystem permissions are a real local trust
//     boundary. The ping is a non-secret int64 -- this is NOT authentication/crypto.
//   * The future A2 slice is where HPX-managed async serving (e.g. an HPX Asio executor)
//     would live; only A2 may use "HPX serving" / "HPX async I/O" language.
//
// Listener ownership is PROCESS-LOCAL (one socket file per process), routed by the
// target endpoint id carried on the wire against the existing process-local registry.
// Start/stop refcounting (tied to live transport endpoints) lives in _rayx.cpp.

#ifndef RAYX_ENDPOINT_TRANSPORT_HPP
#define RAYX_ENDPOINT_TRANSPORT_HPP

#include "endpoint_seam.hpp"  // EndpointRegistry, EndpointSeam, wire codec, status codes

#include <atomic>
#include <chrono>
#include <cstdlib>   // getenv
#include <cstring>   // strerror, memset, strncpy
#include <functional>  // std::function (bridge CALL_OP handler hook)
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>   // std::pair
#include <vector>    // CallOpArg list for the bridge handler

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>   // mkdir
#include <sys/un.h>
#include <unistd.h>
#include <cerrno>

namespace rayx_endpoint {

// Bounded I/O outcome (every blocking step has a single shared deadline; no hangs).
enum class IoResult { OK, TIMEOUT, CLOSED, ERROR };

inline long mono_ms() {
    return static_cast<long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

inline bool set_nonblocking(int fd) {
    int fl = ::fcntl(fd, F_GETFL, 0);
    if (fl < 0) return false;
    return ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0;
}

// SIGPIPE safety: writing to a peer that has closed its read end must NEVER raise
// SIGPIPE (which would terminate the whole process). We belt-and-suspenders it: set the
// per-socket SO_NOSIGPIPE option where available (macOS/BSD), and pass MSG_NOSIGNAL on
// every send() where available (Linux). With both, a closed-peer write returns EPIPE/
// ECONNRESET, which the I/O layer maps to IoResult::CLOSED.
inline void set_no_sigpipe(int fd) {
#ifdef SO_NOSIGPIPE
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof on);
#else
    (void)fd;
#endif
}

#ifdef MSG_NOSIGNAL
inline constexpr int SEND_FLAGS = MSG_NOSIGNAL;
#else
inline constexpr int SEND_FLAGS = 0;
#endif

// Poll one fd for `events` until the absolute steady deadline. Returns 1 ready, 0
// timeout, -1 error. Loops on EINTR (recomputing the remaining budget).
inline int poll_until(int fd, short events, long deadline_ms) {
    for (;;) {
        long rem = deadline_ms - mono_ms();
        if (rem < 0) rem = 0;
        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = events;
        pfd.revents = 0;
        int r = ::poll(&pfd, 1, static_cast<int>(rem));
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        return r == 0 ? 0 : 1;
    }
}

// Read exactly n bytes (or fail) before the shared deadline. EOF -> CLOSED.
inline IoResult full_read(int fd, unsigned char* buf, std::size_t n, long deadline_ms) {
    std::size_t got = 0;
    while (got < n) {
        int pr = poll_until(fd, POLLIN, deadline_ms);
        if (pr == 0) return IoResult::TIMEOUT;
        if (pr < 0) return IoResult::ERROR;
        ssize_t r = ::read(fd, buf + got, n - got);
        if (r == 0) return IoResult::CLOSED;  // EOF
        if (r < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            if (errno == ECONNRESET || errno == EPIPE) return IoResult::CLOSED;
            return IoResult::ERROR;
        }
        got += static_cast<std::size_t>(r);
    }
    return IoResult::OK;
}

// Write exactly n bytes (or fail) before the shared deadline. Uses send() with
// SEND_FLAGS (MSG_NOSIGNAL where available) so a closed peer yields EPIPE/ECONNRESET
// (mapped to CLOSED), never a process-killing SIGPIPE.
inline IoResult full_write(int fd, const unsigned char* buf, std::size_t n,
                           long deadline_ms) {
    std::size_t put = 0;
    while (put < n) {
        int pr = poll_until(fd, POLLOUT, deadline_ms);
        if (pr == 0) return IoResult::TIMEOUT;
        if (pr < 0) return IoResult::ERROR;
        ssize_t r = ::send(fd, buf + put, n - put, SEND_FLAGS);
        if (r < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            if (errno == ECONNRESET || errno == EPIPE) return IoResult::CLOSED;
            return IoResult::ERROR;
        }
        put += static_cast<std::size_t>(r);
    }
    return IoResult::OK;
}

// Dial an AF_UNIX stream socket within the deadline. Returns the connected fd, or -1
// with `outerr` set (ETIMEDOUT on deadline; ENOENT/ECONNREFUSED if nothing is listening).
inline int dial_unix(const std::string& path, long deadline_ms, int& outerr) {
    outerr = 0;
    struct sockaddr_un addr;
    if (path.size() + 1 > sizeof(addr.sun_path)) {
        outerr = ENAMETOOLONG;
        return -1;
    }
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        outerr = errno;
        return -1;
    }
    set_no_sigpipe(fd);
    if (!set_nonblocking(fd)) {
        outerr = errno;
        ::close(fd);
        return -1;
    }
    std::memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    int r = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof addr);
    if (r == 0) return fd;
    if (errno != EINPROGRESS) {
        outerr = errno;
        ::close(fd);
        return -1;
    }
    int pr = poll_until(fd, POLLOUT, deadline_ms);
    if (pr == 0) {
        outerr = ETIMEDOUT;
        ::close(fd);
        return -1;
    }
    if (pr < 0) {
        outerr = errno;
        ::close(fd);
        return -1;
    }
    int soerr = 0;
    socklen_t l = sizeof soerr;
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l) < 0) {
        outerr = errno;
        ::close(fd);
        return -1;
    }
    if (soerr != 0) {
        outerr = soerr;
        ::close(fd);
        return -1;
    }
    return fd;
}

// Python-facing connect status (mirror endpoint/__init__.py): 0 ok, 5 timeout,
// 6 unreachable. A connect-time probe: dial + immediate close, only to classify
// reachability so a later ping-time dial failure can be reported as "peer closed".
inline int transport_probe(const std::string& path, long timeout_ms) {
    long deadline = mono_ms() + timeout_ms;
    int err = 0;
    int fd = dial_unix(path, deadline, err);
    if (fd >= 0) {
        ::close(fd);
        return 0;
    }
    if (err == ETIMEDOUT) return 5;
    return 6;  // ENOENT / ECONNREFUSED / other -> unreachable
}

// One cross-process ping as ONE bounded, one-shot AF_UNIX round trip: dial, send the
// fixed request, read the fixed response, close -- all within one shared deadline. The
// remote Connection does NOT own a persistent fd; every ping dials afresh. Returns
// (python_status, value): 0 ok / 1 peer-closed / 2 conn-closed / 3 not-found /
// 4 proto-err / 5 timeout. A dial failure here (we reached the peer at connect time) is
// reported as conn-closed (2): the peer's listener went away.
inline std::pair<int, std::int64_t> ping_remote(const std::string& path,
                                                const std::string& peer_id,
                                                std::int64_t nonce, long timeout_ms) {
    long deadline = mono_ms() + timeout_ms;
    int err = 0;
    int fd = dial_unix(path, deadline, err);
    if (fd < 0) return {err == ETIMEDOUT ? PING_TIMEOUT : PING_CONN_CLOSED, 0};

    unsigned char req[WIRE_REQ_SIZE];
    wire_encode_request(ENDPOINT_PROTO_VERSION, peer_id, nonce, req);
    IoResult w = full_write(fd, req, WIRE_REQ_SIZE, deadline);
    if (w != IoResult::OK) {
        ::close(fd);
        return {w == IoResult::TIMEOUT ? PING_TIMEOUT : PING_CONN_CLOSED, 0};
    }
    unsigned char resp[WIRE_RESP_SIZE];
    IoResult rr = full_read(fd, resp, WIRE_RESP_SIZE, deadline);
    ::close(fd);
    if (rr != IoResult::OK)
        return {rr == IoResult::TIMEOUT ? PING_TIMEOUT : PING_CONN_CLOSED, 0};

    int proto = 0, status = 0;
    std::int64_t value = 0;
    if (!wire_decode_response(resp, proto, status, value))
        return {PING_CONN_CLOSED, 0};  // bad magic -> desynced -> closed
    if (proto != ENDPOINT_PROTO_VERSION)
        return {PING_PROTO_ERR, 0};     // response from an incompatible build
    switch (status) {
        case WIRE_OK: return {PING_OK, value};
        case WIRE_PEER_CLOSED: return {PING_PEER_CLOSED, 0};
        case WIRE_NOT_FOUND: return {PING_NOT_FOUND, 0};
        case WIRE_PROTO_ERR: return {PING_PROTO_ERR, 0};
        default: return {PING_CONN_CLOSED, 0};
    }
}

// ===== Endpoint->Runtime bridge (v1) plumbing ==============================
// The transport layer is HPX-free and Runtime-agnostic: it does NOT know how to dispatch a
// Runtime op. _rayx.cpp installs a process-global handler here that performs the actual
// (drain-gated) Runtime dispatch. The handler runs ON the accept thread (a plain
// std::thread, never an HPX thread); it may enter HPX internally to enqueue real work and
// block on the result, but it must NOT throw (it returns a CallResult status instead) and
// must NOT touch Python. Null when no Runtime/extension has registered it.
using BridgeCallOpFn =
    std::function<CallResult(int op_code, const std::vector<CallOpArg>& args)>;
inline BridgeCallOpFn& bridge_call_op_handler() {
    static BridgeCallOpFn fn;
    return fn;
}

// Client side of one CALL_OP: one bounded, one-shot AF_UNIX round trip (dial, send the
// fixed request, read the fixed response, close), mirroring ping_remote. Returns a
// CallResult; a dial failure is conn-closed (we reached the peer at connect time once).
inline CallResult call_op_remote(const std::string& path, const std::string& peer_id,
                                 int op_code, const std::vector<CallOpArg>& args,
                                 long timeout_ms) {
    long deadline = mono_ms() + timeout_ms;
    int err = 0;
    int fd = dial_unix(path, deadline, err);
    if (fd < 0)
        return {err == ETIMEDOUT ? CALL_TIMEOUT : CALL_CONN_CLOSED, 0, 0};

    unsigned char req[WIRE_CALLOP_REQ_SIZE];
    CallOpArg slots[WIRE_MAX_CALL_ARGS] = {};
    int argc = static_cast<int>(std::min(args.size(), WIRE_MAX_CALL_ARGS));
    for (int i = 0; i < argc; ++i) slots[i] = args[i];
    wire_encode_call_op(ENDPOINT_PROTO_VERSION, peer_id, op_code, slots, argc, req);
    IoResult w = full_write(fd, req, WIRE_CALLOP_REQ_SIZE, deadline);
    if (w != IoResult::OK) {
        ::close(fd);
        return {w == IoResult::TIMEOUT ? CALL_TIMEOUT : CALL_CONN_CLOSED, 0, 0};
    }
    unsigned char resp[WIRE_CALLOP_RESP_SIZE];
    IoResult rr = full_read(fd, resp, WIRE_CALLOP_RESP_SIZE, deadline);
    ::close(fd);
    if (rr != IoResult::OK)
        return {rr == IoResult::TIMEOUT ? CALL_TIMEOUT : CALL_CONN_CLOSED, 0, 0};
    int proto = 0, status = 0, tag = 0;
    std::int64_t bits = 0;
    if (!wire_decode_call_resp(resp, proto, status, tag, bits))
        return {CALL_CONN_CLOSED, 0, 0};       // bad magic -> desynced
    if (proto != ENDPOINT_PROTO_VERSION)
        return {CALL_PROTO_ERR, 0, 0};
    return {status, tag, bits};
}

// Create (or validate) the socket directory as an owner-only 0700 directory -- the local
// trust boundary the A1 transport relies on. Throws std::runtime_error if it cannot be
// made/kept owner-only. If it already exists it must be a real directory owned by THIS
// uid; loose group/world bits are tightened (chmod 0700) and re-verified, never accepted.
inline void ensure_sock_dir(const std::string& dir) {
    if (::mkdir(dir.c_str(), 0700) == 0) {
        // Fresh dir: defeat umask so it is exactly 0700.
        if (::chmod(dir.c_str(), 0700) != 0)
            throw std::runtime_error("endpoint transport: chmod(" + dir +
                                     ", 0700) failed: " + std::strerror(errno));
        return;
    }
    if (errno != EEXIST)
        throw std::runtime_error("endpoint transport: mkdir(" + dir +
                                 ") failed: " + std::strerror(errno));
    struct stat st;
    if (::lstat(dir.c_str(), &st) != 0)
        throw std::runtime_error("endpoint transport: lstat(" + dir +
                                 ") failed: " + std::strerror(errno));
    if (!S_ISDIR(st.st_mode))
        throw std::runtime_error("endpoint transport: socket dir " + dir +
                                 " exists but is not a directory");
    if (st.st_uid != ::geteuid())
        throw std::runtime_error("endpoint transport: socket dir " + dir +
                                 " is not owned by the current uid (refusing it)");
    if (st.st_mode & (S_IRWXG | S_IRWXO)) {
        // Group/world accessible: tighten deterministically, then re-verify.
        if (::chmod(dir.c_str(), 0700) != 0)
            throw std::runtime_error("endpoint transport: socket dir " + dir +
                                     " is group/world accessible and chmod 0700 failed: " +
                                     std::strerror(errno));
        if (::lstat(dir.c_str(), &st) != 0 || (st.st_mode & (S_IRWXG | S_IRWXO)))
            throw std::runtime_error("endpoint transport: socket dir " + dir +
                                     " could not be made owner-only (0700)");
    }
}

// Process-local AF_UNIX listener. ONE per process, shared by all transport=True endpoints
// (start/stop refcounting lives in _rayx.cpp; independent of HPX). Serial, one-shot
// serving: accept -> read one fixed
// request -> three-state lookup of the target id in the registry -> compute on_ping
// INLINE (LIVE) -> write one fixed response -> close. No HPX, no per-connection threads,
// no unbounded queues.
class TransportListener {
public:
    static TransportListener& instance() {
        static TransportListener t;
        return t;
    }

    // Start the listener (idempotent if already running). Returns the socket path.
    // Throws std::runtime_error on failure (caller serializes via the endpoint mutex).
    std::string start() {
        if (running_) return path_;
        const std::string dir = sock_dir();
        ensure_sock_dir(dir);  // owner-only 0700 (created or validated/tightened)
        struct sockaddr_un addr;
        const std::string path =
            dir + "/ep-" + std::to_string(static_cast<long>(::getpid())) + ".sock";
        if (path.size() + 1 > sizeof(addr.sun_path))
            throw std::runtime_error("endpoint transport: socket path too long (" +
                                     path + "); set RAYX_ENDPOINT_SOCK_DIR shorter");
        int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0)
            throw std::runtime_error(std::string("endpoint transport: socket() failed: ") +
                                     std::strerror(errno));
        ::unlink(path.c_str());  // unlink a stale node (e.g. from a crashed predecessor)
        std::memset(&addr, 0, sizeof addr);
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
        if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof addr) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error(std::string("endpoint transport: bind failed: ") +
                                     std::strerror(e));
        }
        if (::listen(fd, LISTEN_BACKLOG) != 0) {
            int e = errno;
            ::close(fd);
            ::unlink(path.c_str());
            throw std::runtime_error(std::string("endpoint transport: listen failed: ") +
                                     std::strerror(e));
        }
        if (::pipe(wake_) != 0) {
            int e = errno;
            ::close(fd);
            ::unlink(path.c_str());
            throw std::runtime_error(std::string("endpoint transport: pipe failed: ") +
                                     std::strerror(e));
        }
        listen_fd_ = fd;
        path_ = path;
        stopping_.store(false);
        running_ = true;
        thread_ = std::thread([this] { this->accept_loop(); });
        return path_;
    }

    // Stop the listener: wake the accept thread via the self-pipe (NOT by closing the fd
    // out from under poll), join it, then close + unlink. Idempotent.
    void stop() {
        if (!running_) return;
        stopping_.store(true);
        char b = 1;
        ssize_t n = ::write(wake_[1], &b, 1);
        (void)n;
        if (thread_.joinable()) thread_.join();
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::close(wake_[0]);
        ::close(wake_[1]);
        wake_[0] = wake_[1] = -1;
        ::unlink(path_.c_str());
        running_ = false;
        path_.clear();
    }

    bool running() const { return running_; }
    std::string path() const { return path_; }

private:
    TransportListener() = default;

    static std::string sock_dir() {
        const char* e = std::getenv("RAYX_ENDPOINT_SOCK_DIR");
        return (e && *e) ? std::string(e) : std::string("/tmp/rayx-ep");
    }

    void accept_loop() {
        for (;;) {
            struct pollfd pfds[2];
            pfds[0].fd = listen_fd_;
            pfds[0].events = POLLIN;
            pfds[0].revents = 0;
            pfds[1].fd = wake_[0];
            pfds[1].events = POLLIN;
            pfds[1].revents = 0;
            int r = ::poll(pfds, 2, -1);
            if (r < 0) {
                if (errno == EINTR) continue;
                break;
            }
            if (pfds[1].revents & POLLIN) break;  // stop signal
            if (pfds[0].revents & POLLIN) {
                int cfd = ::accept(listen_fd_, nullptr, nullptr);
                if (cfd < 0) {
                    if (stopping_.load()) break;
                    continue;  // EINTR/EAGAIN/transient
                }
                set_no_sigpipe(cfd);  // a peer that closes mid-write must not signal us
                if (!set_nonblocking(cfd)) {
                    ::close(cfd);
                    continue;
                }
                serve_one(cfd);
                ::close(cfd);
            }
        }
    }

    // Serve exactly one request on an accepted connection, then return (caller closes).
    // Reads a fixed 7-byte prefix (magic+proto+msg) first, then the rest of the frame for
    // the indicated message type. PING bytes are unchanged (the prefix is just the head of
    // the existing 39-byte PING request). An unknown msg type is dropped quietly.
    void serve_one(int cfd) {
        long deadline = mono_ms() + HANDLER_TIMEOUT_MS;
        unsigned char prefix[WIRE_PREFIX_SIZE];
        if (full_read(cfd, prefix, WIRE_PREFIX_SIZE, deadline) != IoResult::OK)
            return;  // probe (EOF) / timeout / error -> drop quietly
        int proto = 0, msg = 0;
        if (!wire_decode_prefix(prefix, proto, msg)) return;  // bad magic
        if (msg == WIRE_MSG_PING) {
            unsigned char req[WIRE_REQ_SIZE];
            std::memcpy(req, prefix, WIRE_PREFIX_SIZE);
            if (full_read(cfd, req + WIRE_PREFIX_SIZE,
                          WIRE_REQ_SIZE - WIRE_PREFIX_SIZE, deadline) != IoResult::OK)
                return;
            serve_ping(cfd, req, deadline);
        } else if (msg == WIRE_MSG_CALL_OP) {
            unsigned char req[WIRE_CALLOP_REQ_SIZE];
            std::memcpy(req, prefix, WIRE_PREFIX_SIZE);
            if (full_read(cfd, req + WIRE_PREFIX_SIZE,
                          WIRE_CALLOP_REQ_SIZE - WIRE_PREFIX_SIZE, deadline) != IoResult::OK)
                return;
            serve_call_op(cfd, req, deadline);
        }
        // unknown msg type -> drop quietly (caller closes the socket)
    }

    // A1 ping: resolve the target id and compute the peer-specific transform INLINE (no
    // HPX). Unchanged from A1 except for being factored out of serve_one.
    void serve_ping(int cfd, const unsigned char* req, long deadline) {
        int proto = 0, msg = 0;
        std::string target;
        std::int64_t nonce = 0;
        if (!wire_decode_request(req, proto, msg, target, nonce)) return;  // bad magic
        unsigned char resp[WIRE_RESP_SIZE];
        if (proto != ENDPOINT_PROTO_VERSION || msg != WIRE_MSG_PING) {
            wire_encode_response(ENDPOINT_PROTO_VERSION, WIRE_PROTO_ERR, 0, resp);
        } else {
            // Three-state lookup so a CLOSED peer (registered then closed, with the
            // listener kept alive by another endpoint) is distinguished from an ABSENT
            // (stale / never-registered) id.
            std::shared_ptr<EndpointSeam> seam;
            switch (EndpointRegistry::instance().lookup(target, seam)) {
                case LOOKUP_LIVE:
                    // INLINE pure transform -- no HPX. The shared_ptr keeps the seam
                    // alive for this compute even if it closes concurrently.
                    wire_encode_response(ENDPOINT_PROTO_VERSION, WIRE_OK,
                                         seam->on_ping(nonce), resp);
                    break;
                case LOOKUP_CLOSED:
                    wire_encode_response(ENDPOINT_PROTO_VERSION, WIRE_PEER_CLOSED, 0, resp);
                    break;
                default:  // LOOKUP_ABSENT
                    wire_encode_response(ENDPOINT_PROTO_VERSION, WIRE_NOT_FOUND, 0, resp);
                    break;
            }
        }
        full_write(cfd, resp, WIRE_RESP_SIZE, deadline);
    }

    // Bridge CALL_OP: resolve the target endpoint (same three-state lookup as ping), then
    // hand the op-code + typed args to the process-global bridge handler (installed by
    // _rayx.cpp), which performs the drain-gated Runtime dispatch. The handler runs on THIS
    // accept thread, may enter HPX to enqueue real work and block on the result, and never
    // throws. No HPX call happens in THIS file; socket I/O stays native.
    void serve_call_op(int cfd, const unsigned char* req, long deadline) {
        int proto = 0, msg = 0, op_code = 0, argc = 0;
        std::string target;
        CallOpArg args[WIRE_MAX_CALL_ARGS] = {};
        unsigned char resp[WIRE_CALLOP_RESP_SIZE];
        if (!wire_decode_call_op(req, proto, msg, target, op_code, argc, args))
            return;  // bad magic
        if (proto != ENDPOINT_PROTO_VERSION) {
            wire_encode_call_resp(ENDPOINT_PROTO_VERSION, CALL_PROTO_ERR, 0, 0, resp);
            full_write(cfd, resp, WIRE_CALLOP_RESP_SIZE, deadline);
            return;
        }
        CallResult cr;
        std::shared_ptr<EndpointSeam> seam;
        switch (EndpointRegistry::instance().lookup(target, seam)) {
            case LOOKUP_LIVE: {
                BridgeCallOpFn& handler = bridge_call_op_handler();
                if (!handler) {
                    cr = {CALL_RUNTIME_ABSENT, 0, 0};  // no Runtime/extension registered
                } else {
                    std::vector<CallOpArg> av(args, args + std::min<int>(argc, WIRE_MAX_CALL_ARGS));
                    cr = handler(op_code, av);  // drain-gated Runtime dispatch (no throw)
                }
                break;
            }
            case LOOKUP_CLOSED:
                cr = {CALL_PEER_CLOSED, 0, 0};
                break;
            default:  // LOOKUP_ABSENT
                cr = {CALL_NOT_FOUND, 0, 0};
                break;
        }
        wire_encode_call_resp(ENDPOINT_PROTO_VERSION, cr.status, cr.tag, cr.bits, resp);
        full_write(cfd, resp, WIRE_CALLOP_RESP_SIZE, deadline);
    }

    static constexpr int LISTEN_BACKLOG = 16;
    static constexpr long HANDLER_TIMEOUT_MS = 5000;  // bounds a slow/stalled client

    std::thread thread_;
    std::atomic<bool> stopping_{false};
    bool running_ = false;
    int listen_fd_ = -1;
    int wake_[2] = {-1, -1};
    std::string path_;
};

// Thin free-function wrappers used by _rayx.cpp (which owns the start/stop refcount).
inline std::string transport_listener_start() {
    return TransportListener::instance().start();
}
inline void transport_listener_stop() { TransportListener::instance().stop(); }
inline bool transport_listener_running() {
    return TransportListener::instance().running();
}

}  // namespace rayx_endpoint

#endif  // RAYX_ENDPOINT_TRANSPORT_HPP
