//
// Routes.swift
//
// Created by Gardner von Holt on 9/29/25.
//
// Conventions and middleware:
// - CORS: origin-based allow-list; allow Content-Type and Authorization headers;
//   methods: GET, PUT, DELETE, HEAD, OPTIONS.
// - Auth: optional Bearer token. If KV_TOKEN is set and non-empty, enforce Authorization: Bearer <token>.
//   On failure: 401 with WWW-Authenticate: Bearer, and a standard JSON error body.
//   Never log token values; log only path and request_id.
//
// Request ID propagation:
// - Accept X-Request-ID (case-insensitive). If missing, generate a UUID.
// - Echo X-Request-ID on error responses and include in JSON error “details.request_id”.
//
// Parameter and input validation:
// - Keys and types must not be empty and must not include slashes, newlines, or control characters.
// - Body for PUT must not be empty; reject with 400 if empty.
// - Query parsing uses URLComponents with a dummy scheme/host from req.uri.path (HB2 target).
//
// Response conventions:
// - JSON: Content-Type: application/json; charset=utf-8
// - CSV download: text/csv; charset=utf-8 and Content-Disposition: attachment; filename="<key>.csv"
// - Status codes: PUT 204, GET 200/404, HEAD 200/404, DELETE 204/404
// - Error schema: { error: String, message: String, details?: { ... } }
//
// Rationale:
// - Consistent headers and error schema improve client integration.
// - Defensive validation prevents path traversal and malformed input.
// - Request ID aids correlation across logs and clients.
//

import Hummingbird
import HTTPTypes
import NIOCore
import Foundation
import Logging

private let headerContentType = HTTPField.Name("Content-Type")!
private let headerContentDisposition = HTTPField.Name("Content-Disposition")!
private let headerWWWAuthenticate = HTTPField.Name("WWW-Authenticate")!
private let headerXRequestID = HTTPField.Name("X-Request-ID")!

private let routesLogger = Logger(label: "Routes")

private func bearerToken(from authorization: String) -> String? {
    let parts = authorization.split(separator: " ", maxSplits: 1, omittingEmptySubsequences: true)
    guard parts.count == 2, parts[0].caseInsensitiveCompare("Bearer") == .orderedSame else { return nil }
    return String(parts[1])
}

private func isValidKeyComponent(_ s: String) -> Bool {
    guard !s.isEmpty else { return false }
    for ch in s {
        if ch == "/" || ch.isNewline { return false }
        // Reject control characters (ASCII 0x00-0x1F and 0x7F)
        for scalar in ch.unicodeScalars {
            let v = scalar.value
            if (v <= 0x1F) || (v == 0x7F) { return false }
        }
    }
    return true
}

private func queryValue(_ name: String, from req: Request) -> String? {
    // HB2 Request stores the target in req.uri.path (which includes query).
    // Prepend a dummy scheme/host so URLComponents can parse.
    let raw = req.uri.path   // eg: "/v1/users?prefix=u&limit=10"
    guard let comps = URLComponents(string: "http://local\(raw)"),
          let items = comps.queryItems else { return nil }
    return items.first(where: { $0.name == name })?.value
}

private func queryValues(_ name: String, from req: Request) -> [String] {
    let raw = req.uri.path
    guard let comps = URLComponents(string: "http://local\(raw)"),
          let items = comps.queryItems else { return [] }
    return items.filter { $0.name == name }.compactMap { $0.value }
}

private func requestID(from req: Request) -> String {
    if let existing = req.headers[headerXRequestID] ?? req.headers[HTTPField.Name("x-request-id")!] {
        let val = existing.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
        if !val.isEmpty { return val }
    }
    return UUID().uuidString
}

private func jsonErrorResponse(status: HTTPResponse.Status, error code: String, message: String, details: [String: String]? = nil, requestID: String? = nil) -> Response {
    struct ErrorBody: Encodable { let error: String; let message: String; let details: [String: String]? }
    var mergedDetails = details ?? [:]
    if let requestID { mergedDetails["request_id"] = requestID }
    let body = ErrorBody(error: code, message: message, details: mergedDetails.isEmpty ? nil : mergedDetails)
    let data = (try? JSONEncoder().encode(body)) ?? Data()
    let allocator = ByteBufferAllocator()
    var buf = allocator.buffer(capacity: data.count)
    buf.writeBytes(data)
    var resp = Response(status: status, body: .init(byteBuffer: buf))
    resp.headers[headerContentType] = "application/json; charset=utf-8"
    if let requestID { resp.headers[headerXRequestID] = requestID }
    return resp
}

struct AuthMiddleware: RouterMiddleware {
    typealias Context = BasicRequestContext
    let token: String?

    func handle(
        _ request: Request,
        context: BasicRequestContext,
        next: (Request, BasicRequestContext) async throws -> Response
    ) async throws -> Response {
        let rid = requestID(from: request)
        guard let token, !token.isEmpty else { return try await next(request, context) }
        guard let headerToken = bearerToken(from: request.headers[.authorization] ?? ""), headerToken == token else {
            context.logger.warning("Unauthorized request", metadata: ["path": .string(request.uri.path), "request_id": .string(rid)])
            var resp = jsonErrorResponse(status: .unauthorized, error: "unauthorized", message: "Missing or invalid bearer token.", details: nil, requestID: rid)
            resp.headers[headerWWWAuthenticate] = "Bearer"
            return resp
        }
        return try await next(request, context)
    }
}

func registerRoutes(
    router: Router<BasicRequestContext>,
    store: KVStore,
    maxBodyBytes: Int,
    token: String?
) {
    router.middlewares.add(
        CORSMiddleware<BasicRequestContext>(
            allowOrigin: .originBased,
            allowHeaders: [.contentType, .authorization],
            allowMethods: [.get, .put, .delete, .head, .options]
        )
    )
    router.middlewares.add(AuthMiddleware(token: token))

    // respond to health request
    router.get("health") { (_: Request, _: BasicRequestContext) async -> String in
        "ok"
    }

    // respond to health request
    router.get("areyouthere") { (_: Request, _: BasicRequestContext) async -> String in
        "The reports of my death are greatly exaggerated."
    }

    // PUT /v1/:type/:key
    router.put("v1/:type/:key") { req, ctx in
        let rid = requestID(from: req)
        guard let type: String = ctx.parameters.get("type") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard let key: String = ctx.parameters.get("key") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Invalid type or key.", requestID: rid)
        }
		
		let buf = try await req.body.collect(upTo: maxBodyBytes)
        let data = Data(buf.readableBytesView)
        if data.isEmpty {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Body must not be empty.", requestID: rid)
        }

        do {
            try await store.put(type: type, key: key, value: data)
        } catch {
            ctx.logger.error("PUT failed", metadata: ["type": .string(type), "key": .string(key), "request_id": .string(rid)])
            throw error
        }
        return Response(status: .noContent)
    }

    // GET /v1/:type/:key -> CSV
    router.get("v1/:type/:key") { req, ctx in
        let rid = requestID(from: req)
        guard let type: String = ctx.parameters.get("type") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard let key: String = ctx.parameters.get("key") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Invalid type or key.", requestID: rid)
        }
        
        do {
            if let data = try await store.get(type: type, key: key) {
                let allocator = ByteBufferAllocator()
                var buf = allocator.buffer(capacity: data.count)
                buf.writeBytes(data)
                var resp = Response(status: .ok, body: .init(byteBuffer: buf))
                resp.headers[headerContentType] = "text/csv; charset=utf-8"
                resp.headers[headerContentDisposition] = "attachment; filename=\"\(key).csv\""
                return resp
            }
        } catch {
            ctx.logger.error("GET failed", metadata: ["type": .string(type), "key": .string(key), "request_id": .string(rid)])
            throw error
        }
        return jsonErrorResponse(status: .notFound, error: "not_found", message: "No such key.", details: ["type": type, "key": key], requestID: rid)
    }

    // HEAD /v1/:type/:key
    router.head("v1/:type/:key") { req, ctx in
        let rid = requestID(from: req)
        guard let type: String = ctx.parameters.get("type") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard let key: String = ctx.parameters.get("key") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Invalid type or key.", requestID: rid)
        }
        do {
            return try await store.exists(type: type, key: key) ? Response(status: .ok) : Response(status: .notFound)
        } catch {
            ctx.logger.error("HEAD exists check failed", metadata: ["type": .string(type), "key": .string(key), "request_id": .string(rid)])
            throw error
        }
    }

    // DELETE /v1/:type/:key
    router.delete("v1/:type/:key") { req, ctx in
        let rid = requestID(from: req)
        guard let type: String = ctx.parameters.get("type") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard let key: String = ctx.parameters.get("key") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type or key.", requestID: rid)
        }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Invalid type or key.", requestID: rid)
        }
        do {
            let exists = try await store.exists(type: type, key: key)
            if !exists {
                return jsonErrorResponse(status: .notFound, error: "not_found", message: "No such key.", details: ["type": type, "key": key], requestID: rid)
            }
            try await store.delete(type: type, key: key)
            return Response(status: .noContent)
        } catch {
            ctx.logger.error("DELETE failed", metadata: ["type": .string(type), "key": .string(key), "request_id": .string(rid)])
            throw error
        }
    }

    // GET /v1/:type?prefix=...&limit=...
    router.get("v1/:type") { req, ctx in
        let rid = requestID(from: req)
        guard let type: String = ctx.parameters.get("type") else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Missing type.", requestID: rid)
        }
        guard isValidKeyComponent(type) else {
            return jsonErrorResponse(status: .badRequest, error: "invalid_argument", message: "Invalid type.", requestID: rid)
        }
		let prefix: String? = queryValue("prefix", from: req)
		let rawLimit = Int(queryValue("limit", from: req) ?? "100") ?? 100
		let limit = min(max(rawLimit, 0), 1000)
        
        do {
            let keys = try await store.list(type: type, prefix: prefix, limit: limit)
            let data = try JSONEncoder().encode(keys)

            let allocator = ByteBufferAllocator()
            var buf = allocator.buffer(capacity: data.count)
            buf.writeBytes(data)
            var resp = Response(status: .ok, body: .init(byteBuffer: buf))
            resp.headers[headerContentType] = "application/json; charset=utf-8"
            return resp
        } catch {
            ctx.logger.error("LIST failed", metadata: ["type": .string(type), "prefix": .string(prefix ?? ""), "limit": .string(String(limit)), "request_id": .string(rid)])
            throw error
        }
    }
    
    routesLogger.info("Routes registered", metadata: [
        "auth": .string(token == nil || token == "" ? "disabled" : "enabled"),
        "max_body_bytes": .string(String(maxBodyBytes))
    ])
}
