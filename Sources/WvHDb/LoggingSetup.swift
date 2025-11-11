//
// LoggingSetup.swift
//
// Created by Gardner von Holt on 9/29/25.
//
// Rotating file logging (SwiftLog):
// - We install a single global LogHandler that writes to a file with size-based rotation.
// - We use a final class (not a struct) for the handler to avoid mutating conformance issues
//   and deprecated default method forwarding in SwiftLog.
// - We implement ONLY the full-arity LogHandler.log(level:message:metadata:source:file:function:line:)
//   to avoid recursion/ping‑pong between overloads.
// - We never create/use Logger from inside the handler to avoid recursive logging.
// - We keep a single FileHandle and rotate on a serial DispatchQueue to ensure thread safety.
// - We expand "~" in LOG_PATH and ensure the directory exists. If creation fails, we fall back to /tmp/app.log.
//
// Environment variables for logging:
// - LOG_PATH: file path for logs (e.g., ./Logs/app.log). Tilde (~) is expanded.
// - LOG_MAX_SIZE_BYTES: rotate when the active file exceeds this size (default: 5MB).
// - LOG_MAX_FILES: number of rotated files to keep (default: 5).
// - LOG_LEVEL: trace|debug|info|notice|warning|error|critical; also accepts aliases:
//              verbose→trace, warn→warning, err→error, crit→critical.
//
// Rationale:
// - Class-based handler + explicit full-arity log method = clean protocol conformance,
//   no deprecated default usage, and no recursion.
// - A persistent FileHandle reduces churn and error likelihood on frequent writes.
// - Rotation and append happen on a dedicated queue to keep writes ordered and safe.
//

import Foundation
import Logging

// Simple size-based rotating file log handler.
// - Rotates when the active log file exceeds maxFileSizeBytes
// - Keeps up to maxFileCount files: app.log, app.log.1, ..., app.log.(max-1)
// - Thread-safe via a serial DispatchQueue
final class RotatingFileLogHandler: LogHandler {
    private let label: String
    private let fileURL: URL
    private let maxFileSizeBytes: Int
    private let maxFileCount: Int
    private let queue: DispatchQueue

    private var _logLevel: Logger.Level
    private var _metadata: Logger.Metadata

    // Keep a persistent file handle to minimize open/close churn.
    private var handle: FileHandle?
    // Track if we've already logged a write failure to avoid spamming logs.
    private var loggedWriteFailure = false

    // Shared date formatter to avoid per-log allocation
    private static let tsFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()

    init(label: String,
         fileURL: URL,
         maxFileSizeBytes: Int,
         maxFileCount: Int,
         logLevel: Logger.Level = .info)
    {
        self.label = label
        self.fileURL = fileURL
        self.maxFileSizeBytes = maxFileSizeBytes
        self.maxFileCount = max(1, maxFileCount)
        self.queue = DispatchQueue(label: "RotatingFileLogHandler.\(fileURL.lastPathComponent)")
        self._logLevel = logLevel
        self._metadata = [:]
        // Ensure directory exists
        let dir = fileURL.deletingLastPathComponent()
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        // Ensure base file exists and open handle
        if FileManager.default.fileExists(atPath: fileURL.path) == false {
            FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        }
        self.handle = try? FileHandle(forWritingTo: fileURL)
        try? self.handle?.seekToEnd()
    }

    var logLevel: Logger.Level {
        get { _logLevel }
        set { _logLevel = newValue }
    }

    var metadata: Logger.Metadata {
        get { _metadata }
        set { _metadata = newValue }
    }

    subscript(metadataKey metadataKey: String) -> Logger.Metadata.Value? {
        get { _metadata[metadataKey] }
        set { _metadata[metadataKey] = newValue }
    }

    // Implement the primary log requirement with `source`.
    // Do NOT implement the legacy convenience overload to avoid ping-pong recursion.
    func log(level: Logger.Level,
             message: Logger.Message,
             metadata: Logger.Metadata?,
             source: String,
             file: String,
             function: String,
             line: UInt) {
        guard level >= self.logLevel else { return }
        let ts = Self.tsFormatter.string(from: Date())
        let mergedMD = mergeMetadata(self.metadata, metadata)
        let mdString = mergedMD.isEmpty ? "" : " " + formatMetadata(mergedMD)
        let line = "[\(level.rawValue.uppercased())] \(ts) [\(source)][\(label)]\(mdString) - \(message)\n"
        let data = Data(line.utf8)

        queue.sync {
            rotateIfNeeded(adding: data.count)
            append(data: data)
        }
    }

    private func rotateIfNeeded(adding bytes: Int) {
        let currentSize = (try? FileManager.default.attributesOfItem(atPath: fileURL.path)[.size] as? NSNumber)?.intValue ?? 0
        if currentSize + bytes <= maxFileSizeBytes { return }

        // Close current handle before rotation
        try? handle?.close()
        handle = nil

        // Perform rotation: shift .(n-1) -> .n, base -> .1, delete .(max-1)
        for idx in stride(from: maxFileCount - 1, through: 1, by: -1) {
            let src = fileURL.appendingPathExtension("\(idx)")
            let dst = fileURL.appendingPathExtension("\(idx + 1)")
            if FileManager.default.fileExists(atPath: dst.path) {
                try? FileManager.default.removeItem(at: dst)
            }
            if FileManager.default.fileExists(atPath: src.path) {
                try? FileManager.default.moveItem(at: src, to: dst)
            }
        }
        // Move base to .1
        let first = fileURL.appendingPathExtension("1")
        if FileManager.default.fileExists(atPath: first.path) {
            try? FileManager.default.removeItem(at: first)
        }
        if FileManager.default.fileExists(atPath: fileURL.path) {
            try? FileManager.default.moveItem(at: fileURL, to: first)
        }
        // Truncate/create new base file and reopen handle
        FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        self.handle = try? FileHandle(forWritingTo: fileURL)
        try? self.handle?.seekToEnd()
        // Reset failure flag after rotation
        self.loggedWriteFailure = false
    }

    private func append(data: Data) {
        // Ensure file exists and handle is open
        if handle == nil {
            if FileManager.default.fileExists(atPath: fileURL.path) == false {
                FileManager.default.createFile(atPath: fileURL.path, contents: nil)
            }
            handle = try? FileHandle(forWritingTo: fileURL)
            try? handle?.seekToEnd()
        }
        do {
            guard let handle = handle else { throw NSError(domain: "RotatingFileLogHandler", code: 1, userInfo: [NSLocalizedDescriptionKey: "No file handle"]) }
            try handle.write(contentsOf: data)
        } catch {
            // Avoid spamming logs; only emit once until next rotation/reopen
            if !loggedWriteFailure {
                fputs("RotatingFileLogHandler write failed: \(error)\n", stderr)
                self.loggedWriteFailure = true
            }
        }
    }

    private func mergeMetadata(_ base: Logger.Metadata, _ extra: Logger.Metadata?) -> Logger.Metadata {
        guard let extra else { return base }
        var merged = base
        for (k, v) in extra { merged[k] = v }
        return merged
    }

    private func formatMetadata(_ md: Logger.Metadata) -> String {
        let parts = md.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }
        return parts.joined(separator: " ")
    }
}

// Public bootstrap helper to install rotating file logging for the whole process.
public enum LoggingSetup {
    public static func bootstrapRotatingFileLogger(
        filePath: String,
        maxFileSizeBytes: Int = 5 * 1024 * 1024, // 5 MB
        maxFileCount: Int = 5,
        logLevel: Logger.Level = .info
    ) {
        let url = URL(fileURLWithPath: filePath)
        LoggingSystem.bootstrap { label in
            RotatingFileLogHandler(
                label: label,
                fileURL: url,
                maxFileSizeBytes: maxFileSizeBytes,
                maxFileCount: maxFileCount,
                logLevel: logLevel
            )
        }
    }
}
