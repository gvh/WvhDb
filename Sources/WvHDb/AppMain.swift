import Hummingbird
import Foundation

@main
struct Main {
    static func main() async throws {
        let cfg = Config.load()

        let dbPath = ProcessInfo.processInfo.environment["KV_DB_PATH"] ?? FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("wvhdb.sqlite").path
        let maxBodyBytes = Int(ProcessInfo.processInfo.environment["KV_MAX_BODY"] ?? "5242880") ?? 5 * 1024 * 1024
        let token = ProcessInfo.processInfo.environment["KV_TOKEN"]

        let store = try KVStore(path: dbPath)

		let router = Router<BasicRequestContext>()
		registerRoutes(router: router, store: store, maxBodyBytes: maxBodyBytes, token: token)

        let app = Application(
            responder: router.buildResponder(),
            configuration: .init(address: .hostname(cfg.host, port: cfg.port))
        )
        try await app.runService()
    }
}
