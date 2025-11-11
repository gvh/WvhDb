//
// AppMain.swift
//
// Created by Gardner von Holt on 9/29/25.
//
// Startup sequence and milestones:
// 1) Load configuration (Config.load): also bootstraps rotating file logging.
// 2) Resolve DB path, max body size, and auth token from environment.
// 3) Initialize KVStore (GRDB) and run migrations.
// 4) Register routes and middlewares (CORS + Auth).
// 5) Build Hummingbird responder and run service.
//
// Logging milestones (info level):
// - Starting service with host/port, db_path, max_body_bytes, auth enabled/disabled.
// - After router and app are initialized: “Service initialized; running event loop”.
//
// Rationale:
// - Explicit milestones make it easy to spot where startup failed (config, DB, routes, runtime).
// - Avoid logging secrets; use flags for auth state.
//

import Hummingbird
import Foundation
import Logging

@main
struct Main {
    static func main() async throws {
        let cfg = Config.load()
        let logger = Logger(label: "AppMain")

        let dbPath = ProcessInfo.processInfo.environment["KV_DB_PATH"] ??
                FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("wvhdb.sqlite").path
        let maxBodyBytes = Int(ProcessInfo.processInfo.environment["KV_MAX_BODY"] ?? "5242880") ?? 5 * 1024 * 1024
        let token = ProcessInfo.processInfo.environment["KV_TOKEN"]

        logger.info("Starting service", metadata: ["host": .string(cfg.host), "port": .string(String(cfg.port)), "db_path": .string(dbPath), "max_body_bytes": .string(String(maxBodyBytes)), "auth": .string(token == nil || token == "" ? "disabled" : "enabled")])

        let store = try KVStore(path: dbPath)

		let router = Router<BasicRequestContext>()
		registerRoutes(router: router, store: store, maxBodyBytes: maxBodyBytes, token: token)

        let app = Application(
            responder: router.buildResponder(),
            configuration: .init(address: .hostname(cfg.host, port: cfg.port))
        )
        logger.info("Service initialized; running event loop")
        try await app.runService()
    }
}
