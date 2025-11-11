//
// Config.swift
//
// Created by Gardner von Holt on 9/29/25.
//
// Responsibilities:
// - Read service configuration from environment (KV_HOST, KV_PORT, etc.).
// - Bootstrap global rotating file logging exactly once per process.
// - Log the resolved configuration (without exposing secrets).
//
// Logging bootstrap details:
// - Uses LoggingSetup.bootstrapRotatingFileLogger(...) with a path that expands "~".
// - Ensures log directory exists; if not, falls back to /tmp/app.log and logs a warning.
// - Accepts LOG_LEVEL plus common aliases (verbose|warn|err|crit) and logs the resolved level.
// - Guarded by LOG_BOOTSTRAPPED to prevent double initialization.
//
// Safety notes:
// - Do not log token values (KV_TOKEN). Only log auth enabled/disabled.
// - Keep defaults sane (0.0.0.0:8080, 5MB max body) and clamp limits where relevant.
//
// Rationale:
// - Centralized bootstrap ensures all subsystems share the same logging backend.
// - Early, structured info logs help diagnose environment issues (paths, permissions, levels).
//

import Foundation
import Logging

struct Config {
    let host: String
    let port: Int
    private static let logger = Logger(label: "Config")
    static func load() -> Config {
        // Initialize rotating file logging once
        if ProcessInfo.processInfo.environment["LOG_BOOTSTRAPPED"] == nil {
            let env = ProcessInfo.processInfo.environment
            // Resolve log path (expand ~) and ensure directory exists
            let rawLogPath = env["LOG_PATH"] ?? "~/Logs/app.log"
            let expandedLogPath = (rawLogPath as NSString).expandingTildeInPath
            var logPath = expandedLogPath
            do {
                let logURL = URL(fileURLWithPath: expandedLogPath)
                let dirURL = logURL.deletingLastPathComponent()
                try FileManager.default.createDirectory(at: dirURL, withIntermediateDirectories: true)
            } catch {
                // Fall back to /tmp if we cannot create the directory
                let fallback = "/tmp/app.log"
                logPath = fallback
                logger.warning("Failed to prepare log directory; falling back to /tmp", metadata: [
                    "requested_path": .string(rawLogPath),
                    "expanded_path": .string(expandedLogPath),
                    "error": .string(String(describing: error))
                ])
            }
            let maxSize = Int(env["LOG_MAX_SIZE_BYTES"] ?? "5242880") ?? 5 * 1024 * 1024
            let maxFiles = Int(env["LOG_MAX_FILES"] ?? "5") ?? 5
            let levelStr = env["LOG_LEVEL"]?.lowercased() ?? "info"
            let level: Logger.Level = {
                switch levelStr {
                    case "trace", "verbose": return .trace
                    case "debug": return .debug
                    case "info", "information": return .info
                    case "notice": return .notice
                    case "warning", "warn": return .warning
                    case "error", "err": return .error
                    case "critical", "crit": return .critical
                    default: return .info
                }
            }()
            LoggingSetup.bootstrapRotatingFileLogger(
                filePath: logPath,
                maxFileSizeBytes: maxSize,
                maxFileCount: maxFiles,
                logLevel: level
            )
            logger.info("Logging bootstrapped", metadata: [
                "level": .string(level.rawValue),
                "path": .string(logPath),
                "max_size_bytes": .string(String(maxSize)),
                "max_files": .string(String(maxFiles))
            ])
            // Mark as bootstrapped in the environment for this process lifetime
            setenv("LOG_BOOTSTRAPPED", "1", 1)
        }
        let env = ProcessInfo.processInfo.environment
        let host = env["KV_HOST"] ?? "0.0.0.0"
        let port = Int(env["KV_PORT"] ?? "8080") ?? 8080
        logger.info("Loaded configuration", metadata: ["host": .string(host), "port": .string(String(port))])
        return .init(host: host, port: port)
    }
}
