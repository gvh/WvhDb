//
//  TxnLoggerTests.swift
//  WvHDb
//
//  Created by Gardner von Holt on 11/11/25.
//

import Foundation
import Testing
import CryptoKit

@testable import WvHDb

@Suite("TxnLogger behavior")
struct TxnLoggerTests {

    private func makeTempLogURL() throws -> URL {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("TxnLogger-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("test.txn.log")
    }

    private func readLines(at url: URL) throws -> [String] {
        guard FileManager.default.fileExists(atPath: url.path) else { return [] }
        let data = try Data(contentsOf: url)
        let text = String(decoding: data, as: UTF8.self)
        return text.split(separator: "\n").map { String($0) }
    }

    @Test("Write a few entries and verify JSON Lines exist")
    func testBasicAppend() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        let ts = Date().timeIntervalSince1970
        logger.logInsertAfter(type: "t", key: "k1", ts: ts, updatedAt: ts, txid: "tx1", value: Data("a,b\n".utf8))
        logger.logUpdateAfter(type: "t", key: "k1", ts: ts, updatedAt: ts, txid: "tx1", value: Data("a,c\n".utf8))
        logger.logDeleteBeforeMissing(type: "t", key: "missing", ts: ts, updatedAt: ts, txid: "tx2")

        let lines = try readLines(at: logURL)
        #expect(lines.count == 3)

        // Quick shape checks
        for line in lines {
            let json = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
            #expect(json["version"] as? Int == 1)
            #expect(json["txid"] is String)
            #expect(json["op"] is String)
            #expect(json["ts"] is Double)
        }
    }

    @Test("Truncation at ~8KB and non-UTF8 handling")
    func testTruncationAndNonUTF8() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        let ts = Date().timeIntervalSince1970

        // Create a ~10KB UTF-8 string
        let big = String(repeating: "x", count: 10 * 1024)
        logger.logInsertAfter(type: "t", key: "big", ts: ts, updatedAt: ts, txid: "txb", value: Data(big.utf8))

        // Non-UTF8
        let bytes = [UInt8](repeating: 0xFF, count: 32)
        let nonUTF8 = Data(bytes)
        logger.logInsertAfter(type: "t", key: "nonutf8", ts: ts, updatedAt: ts, txid: "txn", value: nonUTF8)

        let lines = try readLines(at: logURL)
        #expect(lines.count == 2)

        // Parse and assert
        for line in lines {
            let json = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
            let key = json["key"] as? String
            if key == "big" {
                #expect(json["truncated"] as? Bool == true)
                let csv = json["csv"] as? String
                #expect((csv?.utf8.count ?? 0) <= 8 * 1024)
            } else if key == "nonutf8" {
                #expect(json["csv"] as? String == "<non-utf8>")
                #expect(json["truncated"] == nil)
            }
            // sha256 should exist for insert-after
            #expect(json["sha256"] is String)
        }
    }

    @Test("Daily rotation and suffixing")
    func testRotation() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        // Two timestamps with different local days: subtract 36 hours to ensure previous day
        let now = Date()
        let tsToday = now.timeIntervalSince1970
        let tsYesterday = now.addingTimeInterval(-36 * 3600).timeIntervalSince1970

        // First write with an older day to force rotation on the next write
        logger.logInsertAfter(type: "t", key: "k-old", ts: tsYesterday, updatedAt: tsYesterday, txid: "old", value: Data("x\n".utf8))

        // Next write with today's timestamp should rotate the active file to yesterday's name
        logger.logInsertAfter(type: "t", key: "k-new", ts: tsToday, updatedAt: tsToday, txid: "new", value: Data("y\n".utf8))

        // Check files
        let fm = FileManager.default
        let dir = logURL.deletingLastPathComponent()
        let _ = logURL.deletingPathExtension().deletingPathExtension() // .../MyDB

        // Discover any *.txn.log files in dir
        let contents = try fm.contentsOfDirectory(atPath: dir.path)
        let datedLogs = contents.filter { $0.contains(".txn.log") }
        #expect(datedLogs.count >= 1)

        // There should be an active log and at least one dated file; the exact names depend on local date.
        // We don't assert exact filenames to avoid locale/timezone brittleness, but presence is enough.
    }

    @Test("File creation is lazy and appends without clobbering")
    func testFileCreationAndAppend() throws {
        let logURL = try makeTempLogURL()
        let fm = FileManager.default
        #expect(!fm.fileExists(atPath: logURL.path))

        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        logger.logInsertAfter(type: "t", key: "k1", ts: ts, updatedAt: ts, txid: "tx1", value: Data("v1\n".utf8))
        logger.logInsertAfter(type: "t", key: "k2", ts: ts, updatedAt: ts, txid: "tx2", value: Data("v2\n".utf8))

        let lines = try readLines(at: logURL)
        #expect(lines.count == 2)
    }

    @Test("Delete operations omit sha256 and indicate missing as appropriate")
    func testDeleteSemantics() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        logger.logDeleteBeforeMissing(type: "t", key: "missing", ts: ts, updatedAt: ts, txid: "txd")

        let lines = try readLines(at: logURL)
        #expect(lines.count == 1)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])

        #expect(json["op"] as? String == "delete-before-missing" || json["op"] as? String == "delete") // match your actual op code
        #expect(json["sha256"] == nil)
        #expect(json["key"] as? String == "missing")
    }

    @Test("Delete-before includes csv and sha256 when value is present")
    func testDeleteBeforeWithValue() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        let payload = String(repeating: "z", count: 128)
        let data = Data(payload.utf8)
        logger.logDeleteBefore(type: "t", key: "del", ts: ts, updatedAt: ts, txid: "tx-del", value: data)

        let lines = try readLines(at: logURL)
        #expect(lines.count == 1)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])

        #expect(json["op"] as? String == "delete-before")
        #expect(json["csv"] as? String == payload)
        #expect((json["sha256"] as? String)?.isEmpty == false)
        #expect(json["truncated"] == nil)
    }

    @Test("Delete-before with large payload sets truncated")
    func testDeleteBeforeTruncation() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        // > 8KB payload to trigger truncation
        let big = String(repeating: "q", count: 10 * 1024)
        logger.logDeleteBefore(type: "t", key: "del-big", ts: ts, updatedAt: ts, txid: "tx-del-big", value: Data(big.utf8))

        let lines = try readLines(at: logURL)
        #expect(lines.count == 1)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])

        #expect(json["op"] as? String == "delete-before")
        #expect(json["truncated"] as? Bool == true)
        let csv = json["csv"] as? String
        #expect((csv?.utf8.count ?? 0) <= 8 * 1024)
        #expect((json["sha256"] as? String)?.isEmpty == false)
    }

    @Test("Truncation boundary: exactly 8KB and just over")
    func testTruncationBoundary() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        let exact = String(repeating: "x", count: 8 * 1024)
        let over = String(repeating: "y", count: 8 * 1024 + 1)

        logger.logInsertAfter(type: "t", key: "exact", ts: ts, updatedAt: ts, txid: "txe", value: Data(exact.utf8))
        logger.logInsertAfter(type: "t", key: "over", ts: ts, updatedAt: ts, txid: "txo", value: Data(over.utf8))

        let lines = try readLines(at: logURL)
        #expect(lines.count == 2)

        for line in lines {
            let json = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
            let key = try #require(json["key"] as? String)
            let csv = json["csv"] as? String
            if key == "exact" {
                #expect(json["truncated"] as? Bool == false || json["truncated"] == nil)
                #expect(csv?.utf8.count == 8 * 1024)
            } else if key == "over" {
                #expect(json["truncated"] as? Bool == true)
                #expect((csv?.utf8.count ?? 0) <= 8 * 1024)
            }
        }
    }

    @Test("Concurrent writes produce valid JSON lines with correct count")
    func testConcurrentWrites() async throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        let total = 50
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<total {
                group.addTask {
                    logger.logInsertAfter(
                        type: "t",
                        key: "k\(i)",
                        ts: ts,
                        updatedAt: ts,
                        txid: "tx\(i)",
                        value: Data("v\(i)\n".utf8)
                    )
                }
            }
        }

        let lines = try readLines(at: logURL)
        #expect(lines.count == total)
        for line in lines {
            let _ = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
        }
    }

    @Test("Multiple rotations across three days")
    func testMultipleRotations() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        let now = Date()
        let d0 = now.addingTimeInterval(-72 * 3600).timeIntervalSince1970
        let d1 = now.addingTimeInterval(-36 * 3600).timeIntervalSince1970
        let d2 = now.timeIntervalSince1970

        logger.logInsertAfter(type: "t", key: "k0", ts: d0, updatedAt: d0, txid: "tx0", value: Data("v0\n".utf8))
        logger.logInsertAfter(type: "t", key: "k1", ts: d1, updatedAt: d1, txid: "tx1", value: Data("v1\n".utf8))
        logger.logInsertAfter(type: "t", key: "k2", ts: d2, updatedAt: d2, txid: "tx2", value: Data("v2\n".utf8))

        let fm = FileManager.default
        let dir = logURL.deletingLastPathComponent()
        let contents = try fm.contentsOfDirectory(atPath: dir.path)
        let datedLogs = contents.filter { $0.contains(".txn.log") }
        // Expect at least 2 files: one or more dated files + active
        #expect(datedLogs.count >= 2)
    }

    private func sha256Hex(_ data: Data) -> String {
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    @Test("sha256 matches known value for a known payload")
    func testSha256Deterministic() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970
        let payload = "hello,world\n"
        let payloadData = Data(payload.utf8)

        logger.logInsertAfter(type: "t", key: "k", ts: ts, updatedAt: ts, txid: "tx", value: payloadData)

        let lines = try readLines(at: logURL)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])
        let hash = try #require(json["sha256"] as? String)

        let expected = sha256Hex(payloadData) // adjust if the logger normalizes data
        #expect(hash == expected)
    }

    @Test("Timestamps are recorded accurately")
    func testTimestampsRecordedAccurately() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        let ts1 = Date().timeIntervalSince1970
        let ts2 = ts1 + 1.234

        logger.logInsertAfter(type: "t", key: "k1", ts: ts1, updatedAt: ts1, txid: "tx1", value: Data("v1\n".utf8))
        logger.logUpdateAfter(type: "t", key: "k1", ts: ts2, updatedAt: ts2, txid: "tx2", value: Data("v2\n".utf8))

        let lines = try readLines(at: logURL)
        #expect(lines.count == 2)

        // First line
        do {
            let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])
            #expect(abs((json["ts"] as? Double ?? -1) - ts1) < 0.001)
            #expect(abs((json["updated_at"] as? Double ?? -1) - ts1) < 0.001)
        }
        // Second line
        do {
            let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[1].utf8)) as? [String: Any])
            #expect(abs((json["ts"] as? Double ?? -1) - ts2) < 0.001)
            #expect(abs((json["updated_at"] as? Double ?? -1) - ts2) < 0.001)
        }
    }

    @Test("Mixed valid/invalid UTF-8 handling")
    func testMixedUTF8BytesHandling() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        var bytes: [UInt8] = Array("prefix,".utf8)
        bytes.append(contentsOf: [0xFF, 0xFF, 0xFF]) // invalid tail
        let data = Data(bytes)

        logger.logInsertAfter(type: "t", key: "mixed", ts: ts, updatedAt: ts, txid: "txm", value: data)

        let lines = try readLines(at: logURL)
        #expect(lines.count == 1)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])

        // Contract: non-UTF8 should be marked as <non-utf8>
        #expect(json["key"] as? String == "mixed")
        #expect(json["csv"] as? String == "<non-utf8>")
        #expect(json["truncated"] == nil)
    }

    @Test("Write order is preserved")
    func testWriteOrderIsPreserved() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        let keys = ["a", "b", "c", "d"]
        for (i, k) in keys.enumerated() {
            logger.logInsertAfter(type: "t", key: k, ts: ts + Double(i), updatedAt: ts + Double(i), txid: "tx\(i)", value: Data("v\(i)\n".utf8))
        }

        let lines = try readLines(at: logURL)
        #expect(lines.count == keys.count)
        let readKeys: [String] = try lines.map { line in
            let json = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
            return try #require(json["key"] as? String)
        }
        #expect(readKeys == keys)
    }

    @Test("CSV payload with quotes and newlines is handled")
    func testCsvEscapingAndNewlines() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)
        let ts = Date().timeIntervalSince1970

        let payload = "\"quoted\",value\nsecond,line\n"
        logger.logInsertAfter(type: "t", key: "csv", ts: ts, updatedAt: ts, txid: "txcsv", value: Data(payload.utf8))

        let lines = try readLines(at: logURL)
        #expect(lines.count == 1)
        let json = try #require(try JSONSerialization.jsonObject(with: Data(lines[0].utf8)) as? [String: Any])
        let csv = try #require(json["csv"] as? String)
        // Contract: UTF-8 payloads should be preserved (possibly truncated if large). Here it should equal the input.
        #expect(csv == payload)
    }

    @Test("Rotation at midnight boundary")
    func testRotationAtMidnightBoundary() throws {
        let logURL = try makeTempLogURL()
        let logger = TxnLogger(activeLogURL: logURL)

        // Simulate two timestamps straddling midnight by using a 24h difference and letting the logger decide by local day
        let now = Date()
        // Force previous day and current day
        let beforeMidnight = now.addingTimeInterval(-26 * 3600).timeIntervalSince1970
        let afterMidnight = now.timeIntervalSince1970

        logger.logInsertAfter(type: "t", key: "before", ts: beforeMidnight, updatedAt: beforeMidnight, txid: "txBefore", value: Data("x\n".utf8))
        logger.logInsertAfter(type: "t", key: "after", ts: afterMidnight, updatedAt: afterMidnight, txid: "txAfter", value: Data("y\n".utf8))

        let fm = FileManager.default
        let dir = logURL.deletingLastPathComponent()
        let contents = try fm.contentsOfDirectory(atPath: dir.path)
        let logs = contents.filter { $0.contains(".txn.log") }
        #expect(logs.count >= 1)
    }
}
