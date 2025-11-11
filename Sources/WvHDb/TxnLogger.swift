//
//  TxnLogger.swift
//  WvHDb
//
//  Created by Gardner von Holt on 11/11/25.
//
// A dedicated transaction logger that writes JSON Lines with daily rotation
// at local midnight. Supports mid-day rollover with numeric suffixes (.1, .2, ...).
// Includes CSV truncation (8 KB) and SHA-256 hashing for integrity.
//

import Foundation
import CryptoKit

final class TxnLogger: TransactionLogging {
    private let activeLogURL: URL

    // Configuration
    private static let csvLogMaxBytes: Int = 8 * 1024 // 8 KB upper bound for logged CSV

    init(activeLogURL: URL) {
        self.activeLogURL = activeLogURL
        // Ensure directory and file exist
        let dir = activeLogURL.deletingLastPathComponent()
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        if !FileManager.default.fileExists(atPath: activeLogURL.path) {
            FileManager.default.createFile(atPath: activeLogURL.path, contents: nil)
        }
    }

    // MARK: - Public API

    // Common fields:
    // - version: 1
    // - ts / updated_at: seconds since 1970 (Double)
    // - txid: UUID string
    // - type, key
    // - op: specific operation string
    // - bytes: blob size
    // - csv: (possibly truncated to 8 KB)
    // - truncated: Bool (optional)
    // - sha256: hex string of blob

    func logUpdateBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        let (csv, truncated) = Self.csvLogPayload(from: value)
        let sha = Self.sha256Hex(value)
        var entry: [String: Any] = [
            "version": 1,
            "ts": ts,
            "updated_at": updatedAt,
            "txid": txid,
            "op": "update-before",
            "type": type,
            "key": key,
            "bytes": value.count,
            "csv": csv,
            "sha256": sha
        ]
        if truncated { entry["truncated"] = true }
        Self.appendTxnLog(to: activeLogURL, now: ts, entry: entry)
    }

    func logUpdateAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        let (csv, truncated) = Self.csvLogPayload(from: value)
        let sha = Self.sha256Hex(value)
        var entry: [String: Any] = [
            "version": 1,
            "ts": ts,
            "updated_at": updatedAt,
            "txid": txid,
            "op": "update-after",
            "type": type,
            "key": key,
            "bytes": value.count,
            "csv": csv,
            "sha256": sha
        ]
        if truncated { entry["truncated"] = true }
        Self.appendTxnLog(to: activeLogURL, now: ts, entry: entry)
    }

    func logInsertAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        let (csv, truncated) = Self.csvLogPayload(from: value)
        let sha = Self.sha256Hex(value)
        var entry: [String: Any] = [
            "version": 1,
            "ts": ts,
            "updated_at": updatedAt,
            "txid": txid,
            "op": "insert-after",
            "type": type,
            "key": key,
            "bytes": value.count,
            "csv": csv,
            "sha256": sha
        ]
        if truncated { entry["truncated"] = true }
        Self.appendTxnLog(to: activeLogURL, now: ts, entry: entry)
    }

    func logDeleteBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        let (csv, truncated) = Self.csvLogPayload(from: value)
        let sha = Self.sha256Hex(value)
        var entry: [String: Any] = [
            "version": 1,
            "ts": ts,
            "updated_at": updatedAt,
            "txid": txid,
            "op": "delete-before",
            "type": type,
            "key": key,
            "bytes": value.count,
            "csv": csv,
            "sha256": sha
        ]
        if truncated { entry["truncated"] = true }
        Self.appendTxnLog(to: activeLogURL, now: ts, entry: entry)
    }

    func logDeleteBeforeMissing(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String) {
        let entry: [String: Any] = [
            "version": 1,
            "ts": ts,
            "updated_at": updatedAt,
            "txid": txid,
            "op": "delete-before-missing",
            "type": type,
            "key": key
        ]
        Self.appendTxnLog(to: activeLogURL, now: ts, entry: entry)
    }

    // MARK: - Internals (nonisolated)

    private nonisolated static let dailyFormatter: DateFormatter = {
        let df = DateFormatter()
        df.calendar = Calendar(identifier: .gregorian)
        df.locale = Locale(identifier: "en_US_POSIX")
        df.timeZone = TimeZone.current // Local time for filenames and midnight boundaries
        df.dateFormat = "yyyy-MM-dd"
        return df
    }()

    private nonisolated static func dateString(for timeInterval: TimeInterval) -> String {
        let date = Date(timeIntervalSince1970: timeInterval)
        return dailyFormatter.string(from: date)
    }

    private nonisolated static func dailyLogURL(for activeURL: URL, dateString: String) -> URL {
        // activeURL = .../MyDB.txn.log
        // dailyURL = .../MyDB.yyyy-MM-dd.txn.log
        let base = activeURL.deletingPathExtension() // .../MyDB.txn
        let baseWithoutTxn = base.deletingPathExtension() // .../MyDB
        return baseWithoutTxn
            .appendingPathExtension(dateString)
            .appendingPathExtension("txn")
            .appendingPathExtension("log")
    }

    private nonisolated static func nextSuffixURL(base: URL) -> URL {
        // base is like .../MyDB.2025-11-11.txn.log
        // We look for .1, .2, ... and return the next available.
        let fm = FileManager.default
        var index = 1
        while true {
            let candidate = base.appendingPathExtension("\(index)")
            if !fm.fileExists(atPath: candidate.path) {
                return candidate
            }
            index += 1
        }
    }

    // Ensure we are writing into the correct day's file. If the active log's
    // current day does not match 'now' (LOCAL time), rotate by renaming active to dated name.
    // If a dated file already exists for that day, we suffix with .1, .2, ...
    private nonisolated static func ensureDailyRotation(activeURL: URL, now: TimeInterval) {
        let fm = FileManager.default
        let day = dateString(for: now)

        // If active file is empty or today's date file already matches, nothing to do.
        if let attrs = try? fm.attributesOfItem(atPath: activeURL.path),
           let modDate = attrs[.modificationDate] as? Date {
            let lastDay = dailyFormatter.string(from: modDate)
            if lastDay == day {
                return // still today's log
            }
        }

        // If the active file exists and has content, rotate it to the previous day's name.
        if fm.fileExists(atPath: activeURL.path),
           let size = (try? fm.attributesOfItem(atPath: activeURL.path))?[.size] as? NSNumber,
           size.intValue > 0 {
            // Determine the day of the active file by its mod date; if we can't, assume today.
            let prevDay: String
            if let modDate = ((try? fm.attributesOfItem(atPath: activeURL.path))?[.modificationDate] as? Date) {
                prevDay = dailyFormatter.string(from: modDate)
            } else {
                prevDay = day
            }
            let prevURL = dailyLogURL(for: activeURL, dateString: prevDay)

            var targetURL = prevURL
            if fm.fileExists(atPath: prevURL.path) {
                targetURL = nextSuffixURL(base: prevURL)
            }

            _ = try? fm.moveItem(at: activeURL, to: targetURL)
        }

        // Ensure active exists (fresh file for today)
        if !fm.fileExists(atPath: activeURL.path) {
            fm.createFile(atPath: activeURL.path, contents: nil)
        }
    }

    private nonisolated static func appendTxnLog(to activeURL: URL, now: TimeInterval, entry: [String: Any]) {
        do {
            // Ensure directory exists (best-effort)
            let dir = activeURL.deletingLastPathComponent()
            try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

            // Rotate if local day changed
            ensureDailyRotation(activeURL: activeURL, now: now)

            // Append JSON line
            let jsonData = try JSONSerialization.data(withJSONObject: entry, options: [])
            var dataToWrite = jsonData
            dataToWrite.append(0x0A) // newline for JSON Lines
            if let handle = try? FileHandle(forWritingTo: activeURL) {
                defer { try? handle.close() }
                try handle.seekToEnd()
                try handle.write(contentsOf: dataToWrite)
                try? handle.synchronize()
            } else {
                try dataToWrite.write(to: activeURL, options: .atomic)
            }
        } catch {
            fputs("TxnLogger append failed: \(error)\n", stderr)
        }
    }

    private nonisolated static func sha256Hex(_ data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    private nonisolated static func csvLogPayload(from data: Data) -> (csv: String, truncated: Bool) {
        // Try UTF-8 decode; if it fails, mark as non-utf8.
        guard let s = String(data: data, encoding: .utf8) else {
            return ("<non-utf8>", false)
        }
        if s.utf8.count <= csvLogMaxBytes {
            return (s, false)
        } else {
            // Truncate by bytes, not scalar count, to keep JSON size bounded.
            var bytes = Array(s.utf8.prefix(csvLogMaxBytes))
            // Ensure we don't cut in the middle of a multi-byte sequence: attempt to rebuild string safely.
            var truncatedString = String(bytes: bytes, encoding: .utf8)
            // If decoding fails because we cut a multibyte char, pop bytes until valid.
            while truncatedString == nil && !bytes.isEmpty {
                bytes.removeLast()
                truncatedString = String(bytes: bytes, encoding: .utf8)
            }
            return ((truncatedString ?? ""), true)
        }
    }
}
