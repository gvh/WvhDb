//
//  KVStoreLoggerCallsTests.swift
//  WvHDb
//
//  Created by Gardner von Holt on 11/11/25.
//

import Foundation
import Testing
@testable import WvHDb

final class SpyTxnLogger: TransactionLogging {
    private(set) var calls: [String] = []

    func logUpdateBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        calls.append("update-before:\(type):\(key)")
    }
    func logUpdateAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        calls.append("update-after:\(type):\(key)")
    }
    func logInsertAfter(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        calls.append("insert-after:\(type):\(key)")
    }
    func logDeleteBefore(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String, value: Data) {
        calls.append("delete-before:\(type):\(key)")
    }
    func logDeleteBeforeMissing(type: String, key: String, ts: TimeInterval, updatedAt: TimeInterval, txid: String) {
        calls.append("delete-before-missing:\(type):\(key)")
    }
}

@Suite("KVStore logger call semantics")
struct KVStoreLoggerCallsTests {

    private func makeTempDBURL() throws -> URL {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent("KVStoreLogger-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("test.sqlite")
    }

    @Test("Insert emits insert-after; update emits before+after; delete emits delete-before")
    func testLoggerCalls() async throws {
        let dbURL = try makeTempDBURL()
        let spy = SpyTxnLogger()
        let store = try KVStore(path: dbURL.path, logger: spy)

        let type = "t"
        let key = "k"

        // Insert
        try await store.put(type: type, key: key, value: Data("a,b\n".utf8))
        #expect(spy.calls.contains("insert-after:t:k"))

        // Update
        try await store.put(type: type, key: key, value: Data("a,c\n".utf8))
        #expect(spy.calls.contains("update-before:t:k"))
        #expect(spy.calls.contains("update-after:t:k"))

        // Delete
        try await store.delete(type: type, key: key)
        #expect(spy.calls.contains("delete-before:t:k"))

        // Delete missing
        let spy2 = SpyTxnLogger()
        let store2 = try KVStore(path: dbURL.path + "-2", logger: spy2)
        try await store2.delete(type: type, key: "missing")
        #expect(spy2.calls.contains("delete-before-missing:t:missing"))
    }
}
