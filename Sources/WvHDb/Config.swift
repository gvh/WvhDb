import Foundation

struct Config {
    let host: String
    let port: Int
    static func load() -> Config {
        let env = ProcessInfo.processInfo.environment
        let host = env["KV_HOST"] ?? "0.0.0.0"
        let port = Int(env["KV_PORT"] ?? "8080") ?? 8080
        return .init(host: host, port: port)
    }
}