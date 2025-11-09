import Hummingbird
import HTTPTypes
import NIOCore
import Foundation

struct AuthMiddleware: RouterMiddleware {
    typealias Context = BasicRequestContext
    let token: String?

    func handle(
        _ request: Request,
        context: BasicRequestContext,
        next: (Request, BasicRequestContext) async throws -> Response
    ) async throws -> Response {
        guard let token, !token.isEmpty else { return try await next(request, context) }
        let auth = request.headers[.authorization] ?? ""
        guard auth == "Bearer \(token)" else { throw HTTPError(.unauthorized) }
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

    router.get("health") { (_: Request, _: BasicRequestContext) async -> String in
        "ok"
    }

    // PUT /v1/:type/:key
    router.put("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
		
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
        
        if let data = try await store.get(type: type, key: key) {
            let allocator = ByteBufferAllocator()
            var buf = allocator.buffer(capacity: data.count)
            buf.writeBytes(data)
            var resp = Response(status: .ok, body: .init(byteBuffer: buf))
            resp.headers[.contentType] = "text/csv; charset=utf-8"
            return resp
        }
        throw HTTPError(.notFound)
    }

    // HEAD /v1/:type/:key
    router.head("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        return try await store.exists(type: type, key: key) ? Response(status: .ok) : Response(status: .notFound)
    }

    // DELETE /v1/:type/:key
    router.delete("v1/:type/:key") { req, ctx in
        guard let type: String = ctx.parameters.get("type"),
              let key: String = ctx.parameters.get("key") else { throw HTTPError(.badRequest) }
        try await store.delete(type: type, key: key)
        return Response(status: .noContent)
    }

    // GET /v1/:type?prefix=...&limit=...
    router.get("v1/:type") { req, ctx in
        guard let type: String = ctx.parameters.get("type") else { throw HTTPError(.badRequest) }
		let prefix: String? = queryValue("prefix", from: req)
		let limit = min(Int(queryValue("limit", from: req) ?? "100") ?? 100, 1000)
        
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