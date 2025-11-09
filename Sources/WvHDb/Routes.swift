import Hummingbird
import HTTPTypes
import NIOCore
import Foundation

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

struct AuthMiddleware: RouterMiddleware {
    typealias Context = BasicRequestContext
    let token: String?

    func handle(
        _ request: Request,
        context: BasicRequestContext,
        next: (Request, BasicRequestContext) async throws -> Response
    ) async throws -> Response {
        guard let token, !token.isEmpty else { return try await next(request, context) }
        guard let headerToken = bearerToken(from: request.headers[.authorization] ?? ""), headerToken == token else {
            var error = HTTPError(.unauthorized)
            error.headers[.wwwAuthenticate] = "Bearer"
            throw error
        }
        return try await next(request, context)
    }
}

private func queryValue(_ name: String, from req: Request) -> String? {
    // HB2 Request stores the target in req.uri.path (which includes query).
    // Prepend a dummy scheme/host so URLComponents can parse.
    let raw = req.uri.path   // eg: "/v1/users?prefix=u&limit=10"
    guard let comps = URLComponents(string: "http://local\(raw)"),
          let items = comps.queryItems else { return nil }
    return items.first(where: { $0.name == name })?.value
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
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else { throw HTTPError(.badRequest) }
		
		let buf = try await req.body.collect(upTo: maxBodyBytes)
        let data = Data(buf.readableBytesView)
        if data.isEmpty { throw HTTPError(.badRequest) }

        try await store.put(type: type, key: key, value: data)
        return Response(status: .noContent)
    }

    // GET /v1/:type/:key -> CSV
    router.get("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else { throw HTTPError(.badRequest) }
        
        if let data = try await store.get(type: type, key: key) {
            let allocator = ByteBufferAllocator()
            var buf = allocator.buffer(capacity: data.count)
            buf.writeBytes(data)
            var resp = Response(status: .ok, body: .init(byteBuffer: buf))
            resp.headers[.contentType] = "text/csv; charset=utf-8"
            resp.headers[.contentDisposition] = "attachment; filename=\"\(key).csv\""
            return resp
        }
        throw HTTPError(.notFound)
    }

    // HEAD /v1/:type/:key
    router.head("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else { throw HTTPError(.badRequest) }
        return try await store.exists(type: type, key: key) ? Response(status: .ok) : Response(status: .notFound)
    }

    // DELETE /v1/:type/:key
    router.delete("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        guard isValidKeyComponent(type), isValidKeyComponent(key) else { throw HTTPError(.badRequest) }
        try await store.delete(type: type, key: key)
        return Response(status: .noContent)
    }

    // GET /v1/:type?prefix=...&limit=...
    router.get("v1/:type") { req, ctx in
        guard let type: String = ctx.parameters.get("type") else { throw HTTPError(.badRequest) }
        guard isValidKeyComponent(type) else { throw HTTPError(.badRequest) }
		let prefix: String? = queryValue("prefix", from: req)
		let rawLimit = Int(queryValue("limit", from: req) ?? "100") ?? 100
		let limit = min(max(rawLimit, 0), 1000)
        
        let keys = try await store.list(type: type, prefix: prefix, limit: limit)
        let data = try JSONEncoder().encode(keys)

        let allocator = ByteBufferAllocator()
        var buf = allocator.buffer(capacity: data.count)
        buf.writeBytes(data)
        var resp = Response(status: .ok, body: .init(byteBuffer: buf))
        resp.headers[.contentType] = "application/json; charset=utf-8"
        return resp
    }
}

